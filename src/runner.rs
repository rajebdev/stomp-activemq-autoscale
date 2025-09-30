use anyhow::Result;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::future::Future;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
use tokio::time::Duration;
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::service::StompService;
use crate::consumer_pool::{ConsumerPool, MessageHandler};
use crate::autoscaler::AutoScaler;


/// Type alias for message handler function
pub type MessageHandlerFn = Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static>;

/// Type alias for auto-scaling message handler function  
pub type AutoScaleHandlerFn = Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static>;

/// Configuration for a queue with custom handler
pub struct QueueConfig {
    pub name: String,
    pub handler: Option<MessageHandlerFn>,
    pub auto_scaling: bool,
}

/// Configuration for a topic with custom handler
pub struct TopicConfig {
    pub name: String,
    pub handler: Option<MessageHandlerFn>,
}

/// Builder for configuring and running the STOMP application
pub struct StompRunner {
    config: Option<Config>,
    queue_configs: Vec<QueueConfig>,
    topic_configs: Vec<TopicConfig>,
    auto_scale_handlers: HashMap<String, AutoScaleHandlerFn>,
}

impl Default for StompRunner {
    fn default() -> Self {
        Self::new()
    }
}

impl StompRunner {
    /// Create a new STOMP runner instance
    pub fn new() -> Self {
        Self {
            config: None,
            queue_configs: Vec::new(),
            topic_configs: Vec::new(),
            auto_scale_handlers: HashMap::new(),
        }
    }

    /// Set configuration directly
    pub fn with_config(mut self, config: Config) -> Self {
        self.config = Some(config);
        self
    }


    /// Add a queue with a custom handler (supports both static and auto-scaling)
    pub fn add_queue<F, Fut>(mut self, queue_name: &str, handler: F) -> Self 
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        // Create shared handler that can be used for both static and auto-scaling
        let shared_handler = Arc::new(handler);
        
        // Create static handler wrapper
        let static_handler = shared_handler.clone();
        let handler_fn: MessageHandlerFn = Arc::new(move |msg| {
            let handler = static_handler.clone();
            Box::pin(async move { handler(msg).await })
        });
        
        // Create auto-scaling handler wrapper  
        let auto_handler = shared_handler.clone();
        let auto_scale_handler_fn: AutoScaleHandlerFn = Arc::new(move |msg| {
            let handler = auto_handler.clone();
            Box::pin(async move { handler(msg).await })
        });
        
        // Store both static and auto-scaling handlers
        self.auto_scale_handlers.insert(queue_name.to_string(), auto_scale_handler_fn);
        
        self.queue_configs.push(QueueConfig {
            name: queue_name.to_string(),
            handler: Some(handler_fn),
            auto_scaling: true, // This will be determined by config at runtime
        });
        self
    }

    /// Add a topic with custom handler
    pub fn add_topic<F, Fut>(mut self, topic_name: &str, handler: F) -> Self 
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let handler_fn: MessageHandlerFn = Arc::new(move |msg| {
            Box::pin(handler(msg))
        });
        
        self.topic_configs.push(TopicConfig {
            name: topic_name.to_string(),
            handler: Some(handler_fn),
        });
        self
    }

    /// Run the STOMP application with the configured settings
    pub async fn run(mut self) -> Result<()> {
        // Get configuration - return error if none provided and default config fails to load
        let config = match self.config.take() {
            Some(config) => config,
            None => {
                return Err(anyhow::anyhow!(
                    "No configuration provided. Use .with_config(config) to set configuration before calling .run()"
                ));
            }
        };

        // Check if monitoring is configured and enabled
        let monitoring_enabled = config.is_auto_scaling_enabled();
        let has_auto_scaling_queues = !config.get_auto_scaling_queues().is_empty();
        let has_custom_handlers_for_auto_scaling = self.queue_configs.iter()
            .any(|q| config.get_auto_scaling_queues().contains(&q.name));
        
        if monitoring_enabled && (has_auto_scaling_queues || has_custom_handlers_for_auto_scaling) {
            info!("Starting with auto-scaling mode enabled");
            self.run_with_auto_scaling(config).await
        } else {
            if config.is_monitoring_configured() && !monitoring_enabled {
                info!("Monitoring disabled, using minimum worker counts");
            } else {
                info!("Starting with static worker mode");
            }
            self.run_static_workers(config).await
        }
    }

    /// Run with auto-scaling enabled
    async fn run_with_auto_scaling(self, config: Config) -> Result<()> {
        debug!("Initializing auto-scaling system");

        // Create shutdown broadcast channel
        let (shutdown_tx, _shutdown_rx) = broadcast::channel::<()>(1);

        // Setup auto-scaling consumer pools
        let consumer_pools = self.setup_custom_consumer_pools(&config, &shutdown_tx).await?;

        // Create task collection for managing fixed worker subscribers
        let mut subscriber_tasks = JoinSet::new();

        // Setup fixed worker queues alongside auto-scaling
        self.setup_fixed_worker_subscribers(&config, &shutdown_tx, &mut subscriber_tasks).await;

        if consumer_pools.is_empty() && subscriber_tasks.is_empty() {
            warn!("No queues configured for auto-scaling or fixed workers");
            return Ok(());
        }

        if !consumer_pools.is_empty() {
            debug!("Consumer pools initialized for {} queues", consumer_pools.len());
        }

        // Handle topics with static workers (if any)
        let topic_tasks = self.setup_custom_topic_subscribers(&config, &shutdown_tx).await;

        // Create and start auto-scaler
        let autoscaler = AutoScaler::new(
            config.clone(),
            consumer_pools,
            shutdown_tx.subscribe(),
        )?;

        // Setup graceful shutdown signal handler
        let shutdown_handle = self.setup_shutdown_handler(shutdown_tx.clone());

        // Start auto-scaler in background
        let autoscaler_handle = self.start_autoscaler(autoscaler).await;

        info!("STOMP service started successfully");

        // Wait for shutdown and cleanup
        self.shutdown_hybrid_system(shutdown_handle, autoscaler_handle, topic_tasks, subscriber_tasks).await
    }

    /// Run with static workers
    async fn run_static_workers(self, config: Config) -> Result<()> {
        debug!("Initializing static worker system");
        
        // Create shutdown broadcast channel
        let (shutdown_tx, _shutdown_rx) = broadcast::channel::<()>(1);

        // Create task collection for managing subscribers
        let mut subscriber_tasks = JoinSet::new();

        // Setup static subscribers for queues and topics
        self.setup_custom_static_subscribers(&config, &shutdown_tx, &mut subscriber_tasks).await;

        // Setup graceful shutdown signal handler
        let shutdown_handle = self.setup_shutdown_handler(shutdown_tx.clone());


        info!("STOMP service started successfully");

        // Wait for shutdown signal
        let _ = shutdown_handle.await;

        info!("Shutdown signal received, stopping all subscribers...");

        // Shutdown static workers gracefully
        self.shutdown_static_workers(subscriber_tasks, &config).await;

        info!("STOMP service shutdown complete");
        Ok(())
    }

    /// Setup consumer pools with custom handlers for auto-scaling queues
    async fn setup_custom_consumer_pools(
        &self,
        config: &Config,
        shutdown_tx: &broadcast::Sender<()>,
    ) -> Result<HashMap<String, ConsumerPool>> {
        let mut consumer_pools = HashMap::new();
        
        // Get auto-scaling queues from config
        let config_auto_queues = config.get_auto_scaling_queues();
        
        // Combine custom queues and config queues
        let mut all_auto_queues = Vec::new();
        
        // Add custom configured queues that are defined as auto-scaling in config
        for queue_config in &self.queue_configs {
            if config.get_auto_scaling_queues().contains(&queue_config.name) {
                all_auto_queues.push(queue_config.name.clone());
            }
        }
        
        // Add queues from config that aren't already in custom configs
        for queue_name in &config_auto_queues {
            if !all_auto_queues.contains(queue_name) {
                all_auto_queues.push(queue_name.clone());
            }
        }
        
        if all_auto_queues.is_empty() {
            return Ok(HashMap::new());
        }

        info!(
            "📊 Setting up {} auto-scaling queues",
            all_auto_queues.len()
        );

        for queue_name in &all_auto_queues {
            if let Some(worker_range) = config.get_queue_worker_range(queue_name) {
            debug!(
                "🏊 Creating consumer pool for '{}' (workers: {}-{})",
                queue_name, worker_range.min, worker_range.max
            );

                // Use custom handler if provided, otherwise skip this queue
                let handler = if let Some(custom_handler) = self.auto_scale_handlers.get(queue_name) {
                    self.create_custom_queue_message_handler(queue_name.clone(), custom_handler.clone())
                } else {
                    debug!("No custom handler configured for auto-scaling queue '{}', skipping", queue_name);
                    continue;
                };
                
                // Create consumer pool
                let mut pool = ConsumerPool::new(
                    queue_name.clone(),
                    config.clone(),
                    worker_range.clone(),
                    handler,
                    shutdown_tx.subscribe(),
                );

                // Initialize pool with minimum workers
                pool.initialize().await?;
                
                consumer_pools.insert(queue_name.clone(), pool);
            }
        }

        Ok(consumer_pools)
    }

    /// Setup topic subscribers with custom handlers for auto-scaling mode
    async fn setup_custom_topic_subscribers(
        &self,
        config: &Config,
        shutdown_tx: &broadcast::Sender<()>,
    ) -> JoinSet<Result<()>> {
        let mut topic_tasks = JoinSet::new();
        
        // Get topic names from config
        let config_topics = config.get_all_topic_names();
        
        // Combine custom topics and config topics
        let mut all_topics = Vec::new();
        
        // Add custom configured topics
        for topic_config in &self.topic_configs {
            all_topics.push(topic_config.name.clone());
        }
        
        // Add topics from config that aren't already in custom configs
        for topic_name in &config_topics {
            if !all_topics.contains(topic_name) {
                all_topics.push(topic_name.clone());
            }
        }
        
        for topic_name in all_topics {
            debug!(
                "📊 Topic '{}' configured with 1 static worker",
                topic_name
            );

            let config_clone = config.clone();
            let _shutdown_rx = shutdown_tx.subscribe();
            let topic_name_clone = topic_name.clone();
            
            // Find custom handler for this topic
            let custom_handler = self.topic_configs.iter()
                .find(|tc| tc.name == topic_name)
                .and_then(|tc| tc.handler.as_ref())
                .cloned();

            topic_tasks.spawn(async move {
                let mut service = StompService::new(config_clone).await?;
                
                if let Some(handler) = custom_handler {
                    service.receive_topic(&topic_name_clone, move |msg| handler(msg)).await
                } else {
                    // No handler configured - skip this topic
                    debug!("No handler configured for topic '{}', skipping", topic_name_clone);
                    Ok(())
                }
            });
        }

        topic_tasks
    }

    /// Setup fixed worker subscribers (for hybrid auto-scaling mode)
    async fn setup_fixed_worker_subscribers(
        &self,
        config: &Config,
        shutdown_tx: &broadcast::Sender<()>,
        subscriber_tasks: &mut JoinSet<Result<()>>,
    ) {
        // Get fixed worker queues from config
        let fixed_worker_queues = config.get_fixed_worker_queues();
        
        if fixed_worker_queues.is_empty() {
            return;
        }
        
        info!(
            "🔧 Setting up {} fixed worker queues alongside auto-scaling",
            fixed_worker_queues.len()
        );
        
        for queue_name in &fixed_worker_queues {
            // Get worker count for this queue
            let worker_count = config.get_queue_worker_range(queue_name)
                .map(|range| range.min)
                .unwrap_or(1);
            
            debug!(
                "📊 Fixed worker queue '{}' configured with {} worker(s)",
                queue_name, worker_count
            );
            
            // Find custom handler for this queue
            let custom_handler = self.queue_configs.iter()
                .find(|qc| qc.name == *queue_name)
                .and_then(|qc| qc.handler.as_ref())
                .cloned();

            // Start multiple workers for this queue if needed
            for worker_id in 0..worker_count {
                let config_clone = config.clone();
                let _shutdown_rx = shutdown_tx.subscribe();
                let queue_name_clone = queue_name.clone();
                let custom_handler_clone = custom_handler.clone();

                subscriber_tasks.spawn(async move {
                    // Add small delay to stagger worker startup
                    if worker_id > 0 {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100 * worker_id as u64)).await;
                    }
                    
                    let mut service = StompService::new(config_clone).await?;
                    
                    if let Some(handler) = custom_handler_clone {
                        info!("👥 Starting fixed worker {} for queue '{}' (hybrid mode)", worker_id + 1, queue_name_clone);
                        
                        // Add a small delay to allow connection to fully establish before subscription
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                        
                        info!("🔗 Fixed worker {} connecting to queue '{}'", worker_id + 1, queue_name_clone);
                        service.receive_queue(&queue_name_clone, move |msg| handler(msg)).await
                    } else {
                        // No handler configured - skip this queue
                        info!("No handler configured for fixed worker queue '{}', skipping", queue_name_clone);
                        Ok(())
                    }
                });
            }
        }
    }

    /// Setup static subscribers with custom handlers for queues and topics
    async fn setup_custom_static_subscribers(
        &self,
        config: &Config,
        shutdown_tx: &broadcast::Sender<()>,
        subscriber_tasks: &mut JoinSet<Result<()>>,
    ) {
        // Get queue and topic names from config
        let config_queues = config.get_all_queue_names();
        let config_topics = config.get_all_topic_names();
        
        // Combine custom queues and config queues (excluding auto-scaling ones)
        let mut all_queues = Vec::new();
        
        // Add custom configured queues that are NOT auto-scaling
        for queue_config in &self.queue_configs {
            if !config.get_auto_scaling_queues().contains(&queue_config.name) {
                all_queues.push(queue_config.name.clone());
            }
        }
        
        // Add queues from config that aren't already in custom configs
        // This includes both regular queues and fixed worker queues from monitoring config
        for queue_name in &config_queues {
            if !all_queues.contains(queue_name) && !config.get_auto_scaling_queues().contains(queue_name) {
                all_queues.push(queue_name.clone());
            }
        }
        
        // Add fixed worker queues from monitoring config
        for queue_name in &config.get_fixed_worker_queues() {
            if !all_queues.contains(queue_name) {
                all_queues.push(queue_name.clone());
            }
        }
        
        // If monitoring is disabled, add all configured queues using their minimum worker counts
        if config.is_monitoring_configured() && !config.is_auto_scaling_enabled() {
            for queue_name in &config.get_all_configured_queues() {
                if !all_queues.contains(queue_name) {
                    all_queues.push(queue_name.clone());
                }
            }
        }
        
        // Combine custom topics and config topics
        let mut all_topics = Vec::new();
        
        // Add custom configured topics
        for topic_config in &self.topic_configs {
            all_topics.push(topic_config.name.clone());
        }
        
        // Add topics from config that aren't already in custom configs
        for topic_name in &config_topics {
            if !all_topics.contains(topic_name) {
                all_topics.push(topic_name.clone());
            }
        }

        let total_static_workers: u32 = all_queues.iter()
            .map(|queue_name| {
                config.get_queue_worker_range(queue_name)
                    .map(|range| range.min)
                    .unwrap_or(1)
            })
            .sum();
            
        info!(
            "🔧 Setting up {} queues ({} workers) and {} topics with static workers...",
            all_queues.len(),
            total_static_workers,
            all_topics.len()
        );

        // Start subscribers for each queue
        for queue_name in all_queues {
            // Get worker count for this queue
            let worker_count = config.get_queue_worker_range(&queue_name)
                .map(|range| range.min)
                .unwrap_or(1);
            
            debug!(
                "📊 Queue '{}' configured with {} worker(s)",
                queue_name, worker_count
            );
            
            // Find custom handler for this queue
            let custom_handler = self.queue_configs.iter()
                .find(|qc| qc.name == queue_name)
                .and_then(|qc| qc.handler.as_ref())
                .cloned();

            // Start multiple workers for this queue if needed
            for worker_id in 0..worker_count {
                let config_clone = config.clone();
                let _shutdown_rx = shutdown_tx.subscribe();
                let queue_name_clone = queue_name.clone();
                let custom_handler_clone = custom_handler.clone();

                subscriber_tasks.spawn(async move {
                    // Add small delay to stagger worker startup
                    if worker_id > 0 {
                        tokio::time::sleep(tokio::time::Duration::from_millis(100 * worker_id as u64)).await;
                    }
                    
                    let mut service = StompService::new(config_clone).await?;
                    
                    if let Some(handler) = custom_handler_clone {
                        debug!("👥 Starting static worker {} for queue '{}' (fixed worker mode)", worker_id + 1, queue_name_clone);
                        
                        // Add a small delay to allow connection to fully establish before subscription
                        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
                        
                        debug!("🔗 Worker {} connecting to queue '{}'", worker_id + 1, queue_name_clone);
                        service.receive_queue(&queue_name_clone, move |msg| handler(msg)).await
                    } else {
                        // No handler configured - skip this queue
                        debug!("No handler configured for queue '{}', skipping", queue_name_clone);
                        Ok(())
                    }
                });
            }
        }

        // Start subscribers for each topic
        for topic_name in all_topics {
            debug!(
                "📊 Topic '{}' configured with 1 static worker",
                topic_name
            );

            let config_clone = config.clone();
            let _shutdown_rx = shutdown_tx.subscribe();
            let topic_name_clone = topic_name.clone();
            
            // Find custom handler for this topic
            let custom_handler = self.topic_configs.iter()
                .find(|tc| tc.name == topic_name)
                .and_then(|tc| tc.handler.as_ref())
                .cloned();

            subscriber_tasks.spawn(async move {
                let mut service = StompService::new(config_clone).await?;
                
                if let Some(handler) = custom_handler {
                    service.receive_topic(&topic_name_clone, move |msg| handler(msg)).await
                } else {
                    // No handler configured - skip this topic
                    debug!("No handler configured for topic '{}', skipping", topic_name_clone);
                    Ok(())
                }
            });
        }
    }

    /// Setup shutdown signal handler
    fn setup_shutdown_handler(&self, shutdown_tx: broadcast::Sender<()>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            crate::utils::setup_signal_handlers().await;
            let _ = shutdown_tx.send(()); // Notify all services to shutdown
        })
    }

    /// Start autoscaler in background
    async fn start_autoscaler(&self, mut autoscaler: AutoScaler) -> tokio::task::JoinHandle<AutoScaler> {
        tokio::spawn(async move {
            if let Err(e) = autoscaler.run().await {
                error!("Auto-scaler failed: {}", e);
            }
            autoscaler
        })
    }

    /// Handle shutdown for hybrid system (auto-scaling + fixed workers)
    async fn shutdown_hybrid_system(
        &self,
        shutdown_handle: tokio::task::JoinHandle<()>,
        autoscaler_handle: tokio::task::JoinHandle<AutoScaler>,
        mut topic_tasks: JoinSet<Result<()>>,
        mut subscriber_tasks: JoinSet<Result<()>>,
    ) -> Result<()> {
        // Wait for shutdown signal
        let _ = shutdown_handle.await;

        info!("🛑 Shutdown signal received, stopping hybrid system...");

        // Stop the auto-scaler and get it back
        let autoscaler = match autoscaler_handle.await {
            Ok(autoscaler) => autoscaler,
            Err(e) => {
                error!("Failed to join auto-scaler task: {}", e);
                return self.shutdown_fixed_workers_only(subscriber_tasks, topic_tasks).await;
            }
        };

        // Stop all consumer pools
        autoscaler.stop_all_pools().await?;

        // Stop fixed worker tasks
        subscriber_tasks.abort_all();
        while let Some(result) = subscriber_tasks.join_next().await {
            if let Err(e) = result {
                if !e.is_cancelled() {
                    warn!("Fixed worker task error during shutdown: {}", e);
                }
            }
        }

        // Stop topic tasks
        topic_tasks.abort_all();
        while let Some(result) = topic_tasks.join_next().await {
            if let Err(e) = result {
                if !e.is_cancelled() {
                    warn!("Topic task error during shutdown: {}", e);
                }
            }
        }

        info!("✅ Hybrid system shutdown complete");
        Ok(())
    }

    /// Handle shutdown for fixed workers only (fallback)
    async fn shutdown_fixed_workers_only(
        &self,
        mut subscriber_tasks: JoinSet<Result<()>>,
        mut topic_tasks: JoinSet<Result<()>>,
    ) -> Result<()> {
        // Stop fixed worker tasks
        subscriber_tasks.abort_all();
        while let Some(result) = subscriber_tasks.join_next().await {
            if let Err(e) = result {
                if !e.is_cancelled() {
                    warn!("Fixed worker task error during shutdown: {}", e);
                }
            }
        }

        // Stop topic tasks
        topic_tasks.abort_all();
        while let Some(result) = topic_tasks.join_next().await {
            if let Err(e) = result {
                if !e.is_cancelled() {
                    warn!("Topic task error during shutdown: {}", e);
                }
            }
        }

        info!("✅ Fixed workers shutdown complete");
        Ok(())
    }

    /// Handle shutdown for auto-scaling system
    async fn shutdown_autoscaling_system(
        &self,
        shutdown_handle: tokio::task::JoinHandle<()>,
        autoscaler_handle: tokio::task::JoinHandle<AutoScaler>,
        mut topic_tasks: JoinSet<Result<()>>,
    ) -> Result<()> {
        // Wait for shutdown signal
        let _ = shutdown_handle.await;

        info!("🛑 Shutdown signal received, stopping auto-scaling system...");

        // Stop the auto-scaler and get it back
        let autoscaler = match autoscaler_handle.await {
            Ok(autoscaler) => autoscaler,
            Err(e) => {
                error!("Failed to join auto-scaler task: {}", e);
                return Ok(());
            }
        };

        // Stop all consumer pools
        autoscaler.stop_all_pools().await?;

        // Stop topic tasks
        topic_tasks.abort_all();
        while let Some(result) = topic_tasks.join_next().await {
            if let Err(e) = result {
                if !e.is_cancelled() {
                    warn!("Topic task error during shutdown: {}", e);
                }
            }
        }

        info!("✅ Auto-scaling application shutdown complete");
        Ok(())
    }

    /// Handle shutdown for static workers
    async fn shutdown_static_workers(
        &self,
        mut subscriber_tasks: JoinSet<Result<()>>,
        config: &Config,
    ) {
        let shutdown_timeout = Duration::from_secs(config.shutdown.timeout_secs as u64);
        let mut shutdown_count = 0;
        let total_subscribers = subscriber_tasks.len();

        let shutdown_start = tokio::time::Instant::now();
        while !subscriber_tasks.is_empty() && shutdown_start.elapsed() < shutdown_timeout {
            match tokio::time::timeout(Duration::from_secs(1), subscriber_tasks.join_next()).await {
                Ok(Some(result)) => {
                    shutdown_count += 1;
                    match result {
                        Ok(Ok(())) => {
                            info!(
                                "✅ Worker {}/{} shut down gracefully",
                                shutdown_count, total_subscribers
                            );
                        }
                        Ok(Err(e)) => {
                            warn!(
                                "⚠️ Worker {}/{} shut down with error: {}",
                                shutdown_count, total_subscribers, e
                            );
                        }
                        Err(e) => {
                            warn!(
                                "⚠️ Worker {}/{} join error: {}",
                                shutdown_count, total_subscribers, e
                            );
                        }
                    }
                }
                Ok(None) => break,
                Err(_) => continue,
            }
        }

        // Force stop remaining tasks
        if !subscriber_tasks.is_empty() {
            warn!(
                "⏰ Shutdown timeout reached, force stopping {} remaining workers",
                subscriber_tasks.len()
            );
            subscriber_tasks.abort_all();
            while let Some(result) = subscriber_tasks.join_next().await {
                if let Err(e) = result {
                    if !e.is_cancelled() {
                        warn!("Force-stopped worker error: {}", e);
                    }
                }
            }
        }

        info!(
            "✅ Static worker application shutdown complete ({} workers stopped)",
            total_subscribers
        );
    }

    /// Create a custom queue message handler for auto-scaling pools
    fn create_custom_queue_message_handler(
        &self,
        queue_name: String,
        custom_handler: AutoScaleHandlerFn,
    ) -> Box<MessageHandler> {
        let queue_name_clone = queue_name.clone();
        // Create a wrapper that calls the custom handler
        Box::new(move |msg: String| {
            let queue_name = queue_name_clone.clone();
            let handler = custom_handler.clone();
            Box::pin(async move {
                let start_time = std::time::Instant::now();
                debug!(
                    "[{}] Processing message with custom handler: {}",
                    queue_name,
                    msg.chars().take(50).collect::<String>()
                );

                // Call the custom handler function
                let result = handler(msg).await;

                let processing_time = start_time.elapsed();
                match &result {
                    Ok(()) => {
                        debug!(
                            "[{}] ✅ Message processed successfully in {}ms",
                            queue_name,
                            processing_time.as_millis()
                        );
                    }
                    Err(e) => {
                        error!("[{}] ❌ Message processing failed: {}", queue_name, e);
                    }
                }
                result
            }) as Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_run_without_config_returns_error() {
        let runner = StompRunner::new();
        
        let result = runner.run().await;
        
        assert!(result.is_err());
        let error_message = result.unwrap_err().to_string();
        assert!(error_message.contains("No configuration provided"));
        assert!(error_message.contains(".with_config(config)"));
    }

    #[test]
    fn test_runner_builder_pattern() {
        let runner = StompRunner::new()
            .add_queue("test_queue", |_msg| async { Ok(()) })
            .add_topic("test_topic", |_msg| async { Ok(()) });
        
        // Verify the builder pattern works
        assert_eq!(runner.queue_configs.len(), 1);
        assert_eq!(runner.topic_configs.len(), 1);
        assert_eq!(runner.queue_configs[0].name, "test_queue");
        assert_eq!(runner.topic_configs[0].name, "test_topic");
    }

    #[test]
    fn test_with_config_sets_configuration() {
        use crate::config::*;
        use std::collections::HashMap;
        
        let config = Config {
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
                queues: HashMap::new(),
                topics: HashMap::new(),
            },
            scaling: ScalingConfig::default(),
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
        };
        
        let runner = StompRunner::new().with_config(config);
        
        // Verify config is set (we can't directly access it due to Option<Config>, 
        // but we can verify it doesn't return the error)
        assert!(runner.config.is_some());
    }
}
