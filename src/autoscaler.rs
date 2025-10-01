use anyhow::Result;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tokio::time::{interval, Duration};
use tracing::{debug, error, info, warn};

use crate::broker_monitor::BrokerMonitor;
use crate::config::Config;
use crate::consumer_pool::ConsumerPool;
use crate::monitor::{create_broker_monitor, QueueMetrics};
use crate::scaling::{ScalingDecision, ScalingEngine};

/// Auto-scaling service that manages consumer pools and scaling decisions
pub struct AutoScaler {
    /// Configuration
    config: Config,
    /// Broker monitoring client (trait object)
    monitor: Box<dyn BrokerMonitor>,
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
        // Create broker monitor using factory
        let monitor = create_broker_monitor(config.broker.clone())?;

        // Create scaling engine
        let scaling_engine = ScalingEngine::new(0); // Parameter not used anymore

        info!(
            "🎯 Auto-scaler initialized with {} queues using {} broker",
            consumer_pools.len(),
            monitor.broker_type()
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
        info!("🩺 Performing initial {} health check...", self.monitor.broker_type());
        match self.monitor.health_check().await {
            Ok(true) => {
                info!("✅ {} health check PASSED - Ready for monitoring", self.monitor.broker_type());
            }
            Ok(false) => {
                warn!("⚠️ {} health check FAILED - Will continue but metrics may not work", self.monitor.broker_type());
            }
            Err(e) => {
                error!("❌ {} health check ERROR: {} - Will continue but metrics may not work", self.monitor.broker_type(), e);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use crate::consumer_pool::ConsumerPool;
    use crate::monitor::QueueMetrics;
    use std::collections::HashMap;
    use tokio::sync::broadcast;
    use tokio::time::{timeout, Duration};

    fn create_test_config() -> Config {
        let mut scaling_config = ScalingConfig {
            enabled: true,
            interval_secs: 5,
            workers: HashMap::new(),
        };

        scaling_config
            .workers
            .insert("test_queue".to_string(), "1-3".to_string());
        scaling_config
            .workers
            .insert("order_queue".to_string(), "2-5".to_string());

        let mut queues = HashMap::new();
        queues.insert("test_queue".to_string(), "actual.test.queue".to_string());
        queues.insert("order_queue".to_string(), "actual.order.queue".to_string());

        Config {
            service: ServiceConfig {
                name: "test-service".to_string(),
                version: "1.0.0".to_string(),
                description: "Test service".to_string(),
            },
            broker: BrokerConfig {
                broker_type: BrokerType::ActiveMQ,
                host: "localhost".to_string(),
                stomp_port: 61613,
                web_port: 8161,
                username: "admin".to_string(),
                password: "admin".to_string(),
                heartbeat_secs: 30,
                broker_name: "localhost".to_string(),
            },
            destinations: DestinationsConfig {
                queues,
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

    fn create_mock_consumer_pool(
        queue_name: &str,
        worker_range: WorkerRange,
        _initial_workers: usize,
    ) -> ConsumerPool {
        let config = create_test_config();
        
        // Create a mock handler that does nothing
        let handler = Box::new(|_msg: String| {
            Box::pin(async move { Ok(()) })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = anyhow::Result<()>> + Send>,
                >
        });

        ConsumerPool::new(
            queue_name.to_string(),
            config,
            worker_range,
            handler,
        )
    }

    #[tokio::test]
    async fn test_autoscaler_creation_success() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let consumer_pools = HashMap::new();

        let result = AutoScaler::new(config, consumer_pools, shutdown_rx);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_autoscaler_creation_with_consumer_pools() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        
        let mut consumer_pools = HashMap::new();
        let pool = create_mock_consumer_pool(
            "test_queue",
            WorkerRange { min: 1, max: 3, is_fixed: false },
            1,
        );
        consumer_pools.insert("test_queue".to_string(), pool);

        let result = AutoScaler::new(config, consumer_pools, shutdown_rx);
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_register_queues_for_scaling() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        
        let mut consumer_pools = HashMap::new();
        let pool1 = create_mock_consumer_pool(
            "test_queue",
            WorkerRange { min: 1, max: 3, is_fixed: false },
            2,
        );
        let pool2 = create_mock_consumer_pool(
            "order_queue",
            WorkerRange { min: 2, max: 5, is_fixed: false },
            3,
        );
        consumer_pools.insert("test_queue".to_string(), pool1);
        consumer_pools.insert("order_queue".to_string(), pool2);

        let mut autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let result = autoscaler.register_queues_for_scaling().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_register_queues_for_scaling_empty_pools() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let consumer_pools = HashMap::new();

        let mut autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let result = autoscaler.register_queues_for_scaling().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_status_empty_pools() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let consumer_pools = HashMap::new();

        let autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let status = autoscaler.get_status().await;
        assert!(status.is_empty());
    }

    #[tokio::test]
    async fn test_get_status_with_pools() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        
        let mut consumer_pools = HashMap::new();
        let pool = create_mock_consumer_pool(
            "test_queue",
            WorkerRange { min: 1, max: 3, is_fixed: false },
            2,
        );
        consumer_pools.insert("test_queue".to_string(), pool);

        let autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let status = autoscaler.get_status().await;
        
        assert_eq!(status.len(), 1);
        assert!(status.contains_key("test_queue"));
        
        let queue_status = &status["test_queue"];
        assert_eq!(queue_status.queue_name, "test_queue");
        assert_eq!(queue_status.current_workers, 0); // Initially no workers running
        assert_eq!(queue_status.worker_range.min, 1);
        assert_eq!(queue_status.worker_range.max, 3);
    }

    #[tokio::test]
    async fn test_stop_all_pools_empty() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let consumer_pools = HashMap::new();

        let autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let result = autoscaler.stop_all_pools().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_stop_all_pools_with_pools() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        
        let mut consumer_pools = HashMap::new();
        let pool = create_mock_consumer_pool(
            "test_queue",
            WorkerRange { min: 1, max: 3, is_fixed: false },
            1,
        );
        consumer_pools.insert("test_queue".to_string(), pool);

        let autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let result = autoscaler.stop_all_pools().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_wait_for_shutdown() {
        let config = create_test_config();
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let consumer_pools = HashMap::new();

        let autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        
        // Send shutdown signal in background
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = shutdown_tx.send(());
        });

        // Test that wait_for_shutdown returns when signal is sent
        let result = timeout(Duration::from_secs(1), autoscaler.wait_for_shutdown()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_monitoring_cycle_empty_pools() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let consumer_pools = HashMap::new();

        let mut autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let result = autoscaler.monitoring_cycle().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_queue_metrics_no_pool_found() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let consumer_pools = HashMap::new();

        let mut autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        
        let metrics = QueueMetrics {
            queue_name: "nonexistent_queue".to_string(),
            queue_size: 10,
            consumer_count: 1,
            enqueue_count: 100,
            dequeue_count: 90,
            memory_percent_usage: 50.0,
        };

        let result = autoscaler.process_queue_metrics("nonexistent_queue", metrics).await;
        assert!(result.is_ok()); // Should handle gracefully
    }

    #[tokio::test]
    async fn test_apply_scale_up_no_pool_found() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let consumer_pools = HashMap::new();

        let autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let result = autoscaler.apply_scale_up("nonexistent_queue", 2).await;
        assert!(result.is_ok()); // Should handle gracefully
    }

    #[tokio::test]
    async fn test_apply_scale_down_no_pool_found() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let consumer_pools = HashMap::new();

        let autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let result = autoscaler.apply_scale_down("nonexistent_queue", 1).await;
        assert!(result.is_ok()); // Should handle gracefully
    }

    #[tokio::test]
    async fn test_apply_scale_up_with_pool() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        
        let mut consumer_pools = HashMap::new();
        let pool = create_mock_consumer_pool(
            "test_queue",
            WorkerRange { min: 1, max: 5, is_fixed: false },
            2,
        );
        consumer_pools.insert("test_queue".to_string(), pool);

        let autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let result = autoscaler.apply_scale_up("test_queue", 2).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_apply_scale_down_with_pool() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        
        let mut consumer_pools = HashMap::new();
        let pool = create_mock_consumer_pool(
            "test_queue",
            WorkerRange { min: 1, max: 5, is_fixed: false },
            3,
        );
        consumer_pools.insert("test_queue".to_string(), pool);

        let autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let result = autoscaler.apply_scale_down("test_queue", 1).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_queue_status_creation() {
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
        assert_eq!(status.last_consumer_count, 2);
        assert_eq!(status.worker_range.min, 1);
        assert_eq!(status.worker_range.max, 4);
        assert!(!status.worker_range.is_fixed);
    }

    #[test]
    fn test_queue_status_clone() {
        let status = QueueStatus {
            queue_name: "test".to_string(),
            current_workers: 3,
            worker_range: WorkerRange { min: 2, max: 6, is_fixed: true },
            last_queue_size: 10,
            last_consumer_count: 3,
        };

        let cloned_status = status.clone();
        assert_eq!(status.queue_name, cloned_status.queue_name);
        assert_eq!(status.current_workers, cloned_status.current_workers);
        assert_eq!(status.last_queue_size, cloned_status.last_queue_size);
        assert_eq!(status.last_consumer_count, cloned_status.last_consumer_count);
        assert_eq!(status.worker_range.min, cloned_status.worker_range.min);
        assert_eq!(status.worker_range.max, cloned_status.worker_range.max);
        assert_eq!(status.worker_range.is_fixed, cloned_status.worker_range.is_fixed);
    }

    #[test]
    fn test_queue_status_debug() {
        let status = QueueStatus {
            queue_name: "debug_test".to_string(),
            current_workers: 1,
            worker_range: WorkerRange { min: 1, max: 3, is_fixed: false },
            last_queue_size: 0,
            last_consumer_count: 1,
        };

        let debug_str = format!("{:?}", status);
        assert!(debug_str.contains("debug_test"));
        assert!(debug_str.contains("current_workers: 1"));
    }

    #[tokio::test]
    async fn test_autoscaler_with_multiple_pools() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        
        let mut consumer_pools = HashMap::new();
        let pool1 = create_mock_consumer_pool(
            "test_queue",
            WorkerRange { min: 1, max: 3, is_fixed: false },
            1,
        );
        let pool2 = create_mock_consumer_pool(
            "order_queue",
            WorkerRange { min: 2, max: 5, is_fixed: false },
            2,
        );
        consumer_pools.insert("test_queue".to_string(), pool1);
        consumer_pools.insert("order_queue".to_string(), pool2);

        let autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let status = autoscaler.get_status().await;
        
        assert_eq!(status.len(), 2);
        assert!(status.contains_key("test_queue"));
        assert!(status.contains_key("order_queue"));
    }

    #[tokio::test]
    async fn test_autoscaler_register_and_get_status() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        
        let mut consumer_pools = HashMap::new();
        let pool = create_mock_consumer_pool(
            "test_queue",
            WorkerRange { min: 1, max: 3, is_fixed: false },
            2,
        );
        consumer_pools.insert("test_queue".to_string(), pool);

        let mut autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        
        // Register queues first
        let register_result = autoscaler.register_queues_for_scaling().await;
        assert!(register_result.is_ok());
        
        // Then get status
        let status = autoscaler.get_status().await;
        assert_eq!(status.len(), 1);
        assert!(status.contains_key("test_queue"));
    }

    #[tokio::test]
    async fn test_autoscaler_full_workflow() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        
        let mut consumer_pools = HashMap::new();
        let pool = create_mock_consumer_pool(
            "test_queue",
            WorkerRange { min: 1, max: 3, is_fixed: false },
            1,
        );
        consumer_pools.insert("test_queue".to_string(), pool);

        let mut autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        
        // Register queues
        let register_result = autoscaler.register_queues_for_scaling().await;
        assert!(register_result.is_ok());
        
        // Get initial status
        let initial_status = autoscaler.get_status().await;
        assert_eq!(initial_status.len(), 1);
        
        // Test scaling operations
        let scale_up_result = autoscaler.apply_scale_up("test_queue", 1).await;
        assert!(scale_up_result.is_ok());
        
        let scale_down_result = autoscaler.apply_scale_down("test_queue", 1).await;
        assert!(scale_down_result.is_ok());
        
        // Stop all pools
        let stop_result = autoscaler.stop_all_pools().await;
        assert!(stop_result.is_ok());
    }

    #[tokio::test]
    async fn test_edge_case_scale_down_zero_workers() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        
        let mut consumer_pools = HashMap::new();
        let pool = create_mock_consumer_pool(
            "test_queue",
            WorkerRange { min: 0, max: 3, is_fixed: false },
            0,
        );
        consumer_pools.insert("test_queue".to_string(), pool);

        let autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let result = autoscaler.apply_scale_down("test_queue", 1).await;
        assert!(result.is_ok()); // Should handle gracefully even with 0 workers
    }

    #[tokio::test]
    async fn test_edge_case_scale_up_max_workers() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        
        let mut consumer_pools = HashMap::new();
        let pool = create_mock_consumer_pool(
            "test_queue",
            WorkerRange { min: 1, max: 3, is_fixed: false },
            3,
        );
        consumer_pools.insert("test_queue".to_string(), pool);

        let autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let result = autoscaler.apply_scale_up("test_queue", 1).await;
        assert!(result.is_ok()); // Should handle gracefully even at max workers
    }

    #[tokio::test]
    async fn test_process_queue_metrics_with_pool() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        
        let mut consumer_pools = HashMap::new();
        let pool = create_mock_consumer_pool(
            "test_queue",
            WorkerRange { min: 1, max: 3, is_fixed: false },
            1,
        );
        consumer_pools.insert("test_queue".to_string(), pool);

        let mut autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        
        // Register the queue first
        let _ = autoscaler.register_queues_for_scaling().await;
        
        let metrics = QueueMetrics {
            queue_name: "test_queue".to_string(),
            queue_size: 10,
            consumer_count: 1,
            enqueue_count: 100,
            dequeue_count: 90,
            memory_percent_usage: 50.0,
        };

        let result = autoscaler.process_queue_metrics("test_queue", metrics).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_autoscaler_configuration_validation() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let consumer_pools = HashMap::new();

        let autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        
        // Verify config was set correctly
        assert_eq!(autoscaler.config.broker.host, "localhost");
        assert_eq!(autoscaler.config.broker.web_port, 8161);
        assert_eq!(autoscaler.config.scaling.enabled, true);
        assert_eq!(autoscaler.config.scaling.interval_secs, 5);
    }

    #[tokio::test]
    async fn test_queue_status_with_different_worker_ranges() {
        let config = create_test_config();
        let (_shutdown_tx, shutdown_rx) = broadcast::channel(1);
        
        let mut consumer_pools = HashMap::new();
        
        // Pool with fixed workers
        let pool1 = create_mock_consumer_pool(
            "fixed_queue",
            WorkerRange { min: 3, max: 3, is_fixed: true },
            3,
        );
        
        // Pool with variable workers
        let pool2 = create_mock_consumer_pool(
            "variable_queue",
            WorkerRange { min: 1, max: 10, is_fixed: false },
            5,
        );
        
        consumer_pools.insert("fixed_queue".to_string(), pool1);
        consumer_pools.insert("variable_queue".to_string(), pool2);

        let autoscaler = AutoScaler::new(config, consumer_pools, shutdown_rx).unwrap();
        let status = autoscaler.get_status().await;
        
        assert_eq!(status.len(), 2);
        
        let fixed_status = &status["fixed_queue"];
        assert!(fixed_status.worker_range.is_fixed);
        assert_eq!(fixed_status.worker_range.min, 3);
        assert_eq!(fixed_status.worker_range.max, 3);
        
        let variable_status = &status["variable_queue"];
        assert!(!variable_status.worker_range.is_fixed);
        assert_eq!(variable_status.worker_range.min, 1);
        assert_eq!(variable_status.worker_range.max, 10);
    }
}