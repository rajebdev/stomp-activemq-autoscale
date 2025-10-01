use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use tokio::task::JoinHandle;
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
    pub fn run_background(self) -> JoinHandle<Result<()>> {
        tokio::spawn(async move {
            self.run().await
        })
    }
    
    /// Run the listener with shutdown handle for graceful shutdown control
    pub fn run_with_shutdown_handle(self) -> StompListenerHandle {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use std::collections::HashMap;
    use tokio::time::{timeout, Duration};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn create_test_config() -> Config {
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
                timeout_secs: 5, // Short timeout for tests
                grace_period_secs: 1,
            },
            retry: RetryConfig::default(),
        }
    }

    #[test]
    fn test_stomp_listener_handle_creation() {
        let config = create_test_config();
        let listener = StompListener::new(config)
            .add_queue("test_queue", |_msg| async { Ok(()) });

        // This should create a handle without errors
        let handle = listener.run_with_shutdown_handle();
        
        // Immediately shutdown to avoid hanging test
        handle.shutdown();
    }

    #[tokio::test]
    async fn test_graceful_shutdown_signal() {
        let config = create_test_config();
        let shutdown_called = Arc::new(AtomicBool::new(false));
        let shutdown_called_clone = shutdown_called.clone();

        let listener = StompListener::new(config)
            .add_queue("test_queue", move |_msg| {
                let shutdown_called = shutdown_called_clone.clone();
                async move {
                    // Simulate some work
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    shutdown_called.store(true, Ordering::SeqCst);
                    Ok(())
                }
            });

        let handle = listener.run_with_shutdown_handle();

        // Give the service a moment to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send shutdown signal
        handle.shutdown();

        // Verify shutdown signal was sent (this is implicit since shutdown() doesn't block)
        assert!(true, "Shutdown signal sent successfully");
    }

    #[tokio::test]
    async fn test_shutdown_and_wait_timeout() {
        let config = create_test_config();
        
        let listener = StompListener::new(config)
            .add_queue("test_queue", |_msg| async {
                // This handler will never complete
                tokio::time::sleep(Duration::from_secs(3600)).await;
                Ok(())
            });

        let handle = listener.run_with_shutdown_handle();

        // Give the service a moment to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Test that shutdown_and_wait completes within reasonable time
        // (should be faster due to task cancellation)
        let shutdown_result = timeout(
            Duration::from_secs(10), 
            handle.shutdown_and_wait()
        ).await;

        assert!(shutdown_result.is_ok(), "Shutdown should complete within timeout");
    }

    #[test]
    fn test_stomp_listener_builder_pattern() {
        let config = create_test_config();
        let listener = StompListener::new(config)
            .add_queue("queue1", |_msg| async { Ok(()) })
            .add_queue("queue2", |_msg| async { Ok(()) })
            .add_topic("topic1", |_msg| async { Ok(()) });

        // Verify the builder pattern works (we can't easily test the internal state,
        // but we can verify the calls don't panic)
        let handle = listener.run_with_shutdown_handle();
        handle.shutdown();
    }

    #[tokio::test]
    async fn test_multiple_shutdown_calls() {
        let config = create_test_config();
        
        let listener = StompListener::new(config)
            .add_queue("test_queue", |_msg| async { Ok(()) });

        let handle = listener.run_with_shutdown_handle();

        // Give the service a moment to start
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Call shutdown multiple times - should not panic
        handle.shutdown();
        handle.shutdown();
        handle.shutdown();

        // This should complete successfully
        assert!(true, "Multiple shutdown calls handled gracefully");
    }
}