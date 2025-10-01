use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::broadcast;
use tracing::debug;
use crate::config::Config;
use crate::runner::StompRunner;
use crate::stomp_listener_handle::StompListenerHandle;

/// Type alias for message handler function
pub type MessageHandler = Box<dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

/// StompListener for listening to messages from queues and topics
pub struct StompListener {
    runner: StompRunner,
}

impl StompListener {
    /// Create a new StompListener with the given configuration
    pub fn new(config: Config) -> Self {
        let runner = StompRunner::new().with_config(config);
        
        Self {
            runner,
        }
    }

    /// Add a queue with message handler
    pub fn add_queue<F, Fut>(mut self, queue_name: &str, handler: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        debug!("📥 Adding queue listener for: {}", queue_name);
        self.runner = self.runner.add_queue(queue_name, handler);
        self
    }

    /// Add a topic with message handler
    pub fn add_topic<F, Fut>(mut self, topic_name: &str, handler: F) -> Self
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        debug!("📥 Adding topic listener for: {}", topic_name);
        self.runner = self.runner.add_topic(topic_name, handler);
        self
    }

    /// Run the listener (blocking)
    pub async fn run(self) -> Result<()> {
        debug!("🚀 Starting STOMP Listener...");
        
        // Use the existing runner to handle the listening
        self.runner.run().await
    }

    /// Run the listener in background (non-blocking)
    pub fn run_background(self) -> StompListenerHandle {
        debug!("🚀 Starting STOMP Listener with shutdown handle...");
        
        // Create shutdown broadcast channel
        let (shutdown_tx, _shutdown_rx) = broadcast::channel::<()>(1);
        
        // Clone shutdown sender for the handle
        let handle_shutdown_tx = shutdown_tx.clone();
        
        // Spawn the listener task
        let task_handle = tokio::spawn(async move {
            // Use the runner with shutdown signal
            self.runner.run_with_shutdown_signal(shutdown_tx).await
        });
        
        StompListenerHandle::new(handle_shutdown_tx, task_handle)
    }
}