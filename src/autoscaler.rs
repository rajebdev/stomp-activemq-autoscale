use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

use crate::config::{Config, WorkerRange};
use crate::consumer_pool::ConsumerPool;
use crate::monitor::{ActiveMQMonitor, QueueMetrics};
use crate::scaling::{ScalingDecision, ScalingEngine};

/// Auto-scaling service that manages consumer pools and scaling decisions
pub struct AutoScaler {
    /// Configuration
    config: Config,
    /// ActiveMQ monitoring client
    monitor: ActiveMQMonitor,
    /// Scaling decision engine
    scaling_engine: ScalingEngine,
    /// Consumer pools mapped by queue name
    consumer_pools: Arc<Mutex<HashMap<String, ConsumerPool>>>,
    /// Global shutdown receiver
    shutdown_rx: Arc<Mutex<broadcast::Receiver<()>>>,
}

impl AutoScaler {
    /// Create a new auto-scaler service
    pub fn new(
        config: Config,
        consumer_pools: HashMap<String, ConsumerPool>,
        shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<Self> {
        // Create ActiveMQ monitor using unified config
        let monitor = ActiveMQMonitor::new(config.activemq.clone())?;

        // Create scaling engine
        let scaling_engine = ScalingEngine::new(0); // Parameter not used anymore

        info!(
            "🎯 Auto-scaler initialized with {} queues",
            consumer_pools.len()
        );

        Ok(Self {
            config,
            monitor,
            scaling_engine,
            consumer_pools: Arc::new(Mutex::new(consumer_pools)),
            shutdown_rx: Arc::new(Mutex::new(shutdown_rx)),
        })
    }

    /// Start the auto-scaling monitor loop
    pub async fn run(&mut self) -> Result<()> {
        let interval_secs = self
            .config
            .get_monitoring_interval_secs()
            .unwrap_or(5);

        info!(
            "🔄 Starting auto-scaling monitor (interval: {} seconds)",
            interval_secs
        );
        info!("⏰ Monitoring will check queue metrics every {} seconds", interval_secs);

        // Perform initial health check
        info!("🩺 Performing initial ActiveMQ health check...");
        match self.monitor.health_check().await {
            Ok(true) => {
                info!("✅ ActiveMQ health check PASSED - Ready for monitoring");
            }
            Ok(false) => {
                warn!("⚠️ ActiveMQ health check FAILED - Will continue but metrics may not work");
            }
            Err(e) => {
                error!("❌ ActiveMQ health check ERROR: {} - Will continue but metrics may not work", e);
            }
        }

        // Register all consumer pools with the scaling engine
        self.register_queues_for_scaling().await?;

        // Create interval timer
        let mut ticker = interval(Duration::from_secs(interval_secs));

        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if let Err(e) = self.monitoring_cycle().await {
                        error!("Monitoring cycle failed: {}", e);
                        // Continue running even if one cycle fails
                    }
                }
                _ = self.wait_for_shutdown() => {
                    info!("🛑 Auto-scaler received shutdown signal");
                    break;
                }
            }
        }

        info!("✅ Auto-scaler stopped");
        Ok(())
    }

    /// Wait for shutdown signal
    async fn wait_for_shutdown(&self) {
        let mut shutdown_rx = self.shutdown_rx.lock().await;
        let _ = shutdown_rx.recv().await;
    }

    /// Register all consumer pools with the scaling engine
    async fn register_queues_for_scaling(&mut self) -> Result<()> {
        let consumer_pools = self.consumer_pools.lock().await;

        for (queue_name, pool) in consumer_pools.iter() {
            let worker_range = pool.get_worker_range().clone();
            let current_workers = pool.get_worker_count().await as u32;

            self.scaling_engine.register_queue(
                queue_name.clone(),
                worker_range,
                current_workers,
            );
        }

        info!(
            "📊 Registered {} queues for auto-scaling",
            consumer_pools.len()
        );
        Ok(())
    }

    /// Perform one monitoring and scaling cycle
    async fn monitoring_cycle(&mut self) -> Result<()> {
        debug!("🔍 Starting monitoring cycle");

        // Get list of queue config keys to monitor
        let queue_config_keys = {
            let consumer_pools = self.consumer_pools.lock().await;
            consumer_pools.keys().cloned().collect::<Vec<_>>()
        };

        if queue_config_keys.is_empty() {
            warn!("No queues to monitor - consumer pools empty");
            return Ok(());
        }

        debug!("📋 Monitoring {} queues: {:?}", queue_config_keys.len(), queue_config_keys);

        // Get mapping from config keys to actual ActiveMQ queue names
        let queue_name_mapping = self.config.get_queue_key_to_activemq_name_mapping();
        
        // Build list of actual ActiveMQ queue names for API calls
        let mut activemq_queue_names = Vec::new();
        let mut config_key_to_activemq_name = std::collections::HashMap::new();
        
        for config_key in &queue_config_keys {
            if let Some(activemq_name) = queue_name_mapping.get(config_key) {
                activemq_queue_names.push(activemq_name.clone());
                config_key_to_activemq_name.insert(activemq_name.clone(), config_key.clone());
                debug!("🗺 Mapping: '{}' config → '{}' ActiveMQ queue", config_key, activemq_name);
            } else {
                error!("❌ No ActiveMQ queue name found for config key '{}'", config_key);
            }
        }

        debug!("🚀 Fetching metrics from ActiveMQ API for {} queues", activemq_queue_names.len());
        
        // Collect metrics for all queues using actual ActiveMQ names
        let metrics_results = self.monitor.get_multiple_queue_metrics(&activemq_queue_names).await;

        let mut successful_metrics = 0;
        let mut failed_metrics = 0;
        
        // Process each queue's metrics and apply scaling decisions
        for (activemq_queue_name, metrics_result) in metrics_results {
            // Map back from ActiveMQ name to config key for processing
            if let Some(config_key) = config_key_to_activemq_name.get(&activemq_queue_name) {
                match metrics_result {
                    Ok(mut metrics) => {
                        debug!(
                            "📈 Queue '{}' (ActiveMQ: '{}'): {} messages, {} consumers",
                            config_key, activemq_queue_name, metrics.queue_size, metrics.consumer_count
                        );
                        
                        successful_metrics += 1;
                        
                        // Update metrics to use config key for internal processing
                        metrics.queue_name = config_key.clone();
                        
                        if let Err(e) = self.process_queue_metrics(config_key, metrics).await {
                            error!("Failed to process metrics for queue '{}' (ActiveMQ: '{}'): {}", 
                                   config_key, activemq_queue_name, e);
                        }
                    }
                    Err(e) => {
                        error!("🚫 Failed to get metrics for queue '{}' (ActiveMQ: '{}'): {}", 
                              config_key, activemq_queue_name, e);
                        failed_metrics += 1;
                    }
                }
            } else {
                warn!("⚠️ No config key found for ActiveMQ queue '{}'", activemq_queue_name);
            }
        }

        debug!(
            "✅ Monitoring cycle completed - Success: {}/{}, Failed: {}", 
            successful_metrics, 
            successful_metrics + failed_metrics,
            failed_metrics
        );
        Ok(())
    }

    /// Process metrics for a single queue and apply scaling decision
    async fn process_queue_metrics(&mut self, queue_name: &str, metrics: QueueMetrics) -> Result<()> {
        // Get current worker count from the consumer pool
        let current_workers = {
            let consumer_pools = self.consumer_pools.lock().await;
            match consumer_pools.get(queue_name) {
                Some(pool) => pool.get_worker_count().await as u32,
                None => {
                    warn!("Consumer pool for queue '{}' not found", queue_name);
                    return Ok(());
                }
            }
        };

        debug!(
            "🔎 Processing metrics for '{}': queue_size={}, current_workers={}",
            queue_name, metrics.queue_size, current_workers
        );

        // Get scaling decision from the engine
        let decision = self
            .scaling_engine
            .evaluate_scaling(queue_name, metrics.clone(), current_workers);

        // Apply the scaling decision
        match decision {
            ScalingDecision::NoChange => {
                debug!("🔴 Queue '{}': No scaling needed ({}msg/{}workers)", 
                      queue_name, metrics.queue_size, current_workers);
            }
            ScalingDecision::ScaleUp(count) => {
                info!(
                    "🔴 ➡️ 🟢 Queue '{}': SCALING UP +{} workers ({}msg, {}->{}workers)",
                    queue_name, count, metrics.queue_size, current_workers, current_workers + count
                );
                self.apply_scale_up(queue_name, count).await?;
            }
            ScalingDecision::ScaleDown(count) => {
                info!(
                    "🟢 ➡️ 🔴 Queue '{}': SCALING DOWN -{} workers ({}msg, {}->{} workers)",
                    queue_name, count, metrics.queue_size, current_workers, current_workers - count
                );
                self.apply_scale_down(queue_name, count).await?;
            }
        }

        Ok(())
    }

    /// Apply scale up decision to a consumer pool
    async fn apply_scale_up(&self, queue_name: &str, count: u32) -> Result<()> {
        info!("🚀 Executing scale up for queue '{}': requesting +{} workers", queue_name, count);
        
        let consumer_pools = self.consumer_pools.lock().await;
        
        match consumer_pools.get(queue_name) {
            Some(pool) => {
                let current_count = pool.get_worker_count().await as u32;
                let target_count = current_count + count;
                
                info!(
                    "🔧 Queue '{}': Attempting to scale from {} to {} workers",
                    queue_name, current_count, target_count
                );
                
                match pool.scale_up(target_count).await {
                    Ok(actual_increase) => {
                        let final_count = current_count + actual_increase;
                        info!(
                            "🎉 ✅ Scale up SUCCESS for queue '{}': +{} workers ({} -> {} total)",
                            queue_name, actual_increase, current_count, final_count
                        );
                    }
                    Err(e) => {
                        error!("😱 ❌ Scale up FAILED for queue '{}': {}", queue_name, e);
                    }
                }
            }
            None => {
                error!("❌ Consumer pool for queue '{}' not found during scale up", queue_name);
            }
        }
        
        Ok(())
    }

    /// Apply scale down decision to a consumer pool
    async fn apply_scale_down(&self, queue_name: &str, count: u32) -> Result<()> {
        info!("🐌 Executing scale down for queue '{}': requesting -{} workers", queue_name, count);
        
        let consumer_pools = self.consumer_pools.lock().await;
        
        match consumer_pools.get(queue_name) {
            Some(pool) => {
                let current_count = pool.get_worker_count().await as u32;
                let target_count = current_count.saturating_sub(count);
                
                info!(
                    "🔧 Queue '{}': Attempting to scale from {} to {} workers",
                    queue_name, current_count, target_count
                );
                
                match pool.scale_down(target_count).await {
                    Ok(actual_decrease) => {
                        let final_count = current_count - actual_decrease;
                        info!(
                            "🎉 ✅ Scale down SUCCESS for queue '{}': -{} workers ({} -> {} total)",
                            queue_name, actual_decrease, current_count, final_count
                        );
                    }
                    Err(e) => {
                        error!("😱 ❌ Scale down FAILED for queue '{}': {}", queue_name, e);
                    }
                }
            }
            None => {
                error!("❌ Consumer pool for queue '{}' not found during scale down", queue_name);
            }
        }
        
        Ok(())
    }

    /// Get current status of all queues and workers
    pub async fn get_status(&self) -> HashMap<String, QueueStatus> {
        let mut status = HashMap::new();
        let consumer_pools = self.consumer_pools.lock().await;

        for (queue_name, pool) in consumer_pools.iter() {
            let worker_count = pool.get_worker_count().await;
            let worker_range = pool.get_worker_range().clone();
            
            // Get last metrics from scaling engine
            let last_metrics = self
                .scaling_engine
                .get_queue_history(queue_name)
                .and_then(|h| h.last_metrics.as_ref());

            status.insert(
                queue_name.clone(),
                QueueStatus {
                    queue_name: queue_name.clone(),
                    current_workers: worker_count as u32,
                    worker_range,
                    last_queue_size: last_metrics.map(|m| m.queue_size).unwrap_or(0),
                    last_consumer_count: last_metrics.map(|m| m.consumer_count).unwrap_or(0),
                },
            );
        }

        status
    }

    /// Stop all consumer pools
    pub async fn stop_all_pools(&self) -> Result<()> {
        info!("🛑 Stopping all consumer pools");
        
        let consumer_pools = self.consumer_pools.lock().await;
        
        for (queue_name, pool) in consumer_pools.iter() {
            info!("Stopping consumer pool for queue '{}'", queue_name);
            if let Err(e) = pool.stop_all().await {
                error!("Failed to stop consumer pool for '{}': {}", queue_name, e);
            }
        }
        
        info!("✅ All consumer pools stopped");
        Ok(())
    }
}

/// Status information for a queue
#[derive(Debug, Clone)]
pub struct QueueStatus {
    pub queue_name: String,
    pub current_workers: u32,
    pub worker_range: WorkerRange,
    pub last_queue_size: u32,
    pub last_consumer_count: u32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use crate::consumer_pool::MessageHandler;
    use std::collections::HashMap;
    use std::pin::Pin;
    use tokio::sync::broadcast;

    fn create_test_config() -> Config {
        let mut scaling_config = ScalingConfig {
            enabled: true,
            interval_secs: 5,
            workers: HashMap::new(),
        };

        scaling_config
            .workers
            .insert("test".to_string(), "1-3".to_string());

        Config {
            service: ServiceConfig {
                name: "test-service".to_string(),
                version: "1.0.0".to_string(),
                description: "Test service".to_string(),
            },
            activemq: ActiveMQConfig {
                host: "localhost".to_string(),
                stomp_port: 61613,
                web_port: 8161,
                username: "admin".to_string(),
                password: "admin".to_string(),
                heartbeat_secs: 30,
                broker_name: "localhost".to_string(),
            },
            destinations: DestinationsConfig {
                queues: HashMap::new(),
                topics: HashMap::new(),
            },
            scaling: scaling_config,
            consumers: ConsumersConfig {
                ack_mode: "client_individual".to_string(),
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                output: "stdout".to_string(),
            },
            shutdown: ShutdownConfig {
                timeout_secs: 30,
                grace_period_secs: 5,
            },
            retry: RetryConfig::default(),
        }
    }

    fn create_test_handler() -> Box<MessageHandler> {
        Box::new(|_msg: String| {
            Box::pin(async move { Ok(()) })
                as Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        })
    }

    #[tokio::test]
    async fn test_autoscaler_creation() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let consumer_pools = HashMap::new();

        let result = AutoScaler::new(config, consumer_pools, shutdown_rx);
        assert!(result.is_ok());
    }

    #[test]
    fn test_queue_status() {
        let status = QueueStatus {
            queue_name: "test".to_string(),
            current_workers: 2,
            worker_range: WorkerRange { min: 1, max: 4, is_fixed: false },
            last_queue_size: 5,
            last_consumer_count: 2,
        };

        assert_eq!(status.queue_name, "test");
        assert_eq!(status.current_workers, 2);
        assert_eq!(status.last_queue_size, 5);
    }
}