use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::broadcast;
use tracing::debug;
use crate::config::Config;
use crate::runner::{Runner, StompRunner};
use crate::listener_handle::StompListenerHandle;

/// Type alias for message handler function
pub type MessageHandler = Box<dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync>;

/// StompListener for listening to messages from queues and topics
pub struct StompListener<R: Runner> {
    runner: R,
}

impl<R: Runner + 'static> StompListener<R> {
    /// Create a new StompListener with a custom runner (for testing)
    #[cfg(test)]
    pub fn with_runner(runner: R) -> Self {
        Self { runner }
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

/// Implement for StompRunner specifically
impl StompListener<StompRunner> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use std::time::Duration;
    use crate::config::*;
    use crate::runner::MockRunner;
    use std::collections::HashMap;
    
    // Helper to create a test config
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
                timeout_secs: 30,
                grace_period_secs: 5,
            },
            retry: RetryConfig::default(),
        }
    }
    
    #[test]
    fn test_stomp_listener_new_creates_listener_with_config() {
        let config = create_test_config();
        let _listener = StompListener::new(config.clone());
        
        // We can't directly access the private runner field, but we can 
        // verify that the listener was created successfully
        // (if config was invalid, this would panic)
        assert!(true); // If we get here, listener was created successfully
    }
    
    #[test] 
    fn test_stomp_listener_add_queue_returns_self() {
        let config = create_test_config();
        let listener = StompListener::new(config);
        
        let handler = |_msg: String| async { Ok(()) };
        let _updated_listener = listener.add_queue("test_queue", handler);
        
        // Should return a listener (this is mainly a compile-time check)
        assert!(true);
    }
    
    #[test]
    fn test_stomp_listener_add_topic_returns_self() {
        let config = create_test_config();
        let listener = StompListener::new(config);
        
        let handler = |_msg: String| async { Ok(()) };
        let _updated_listener = listener.add_topic("test_topic", handler);
        
        // Should return a listener (this is mainly a compile-time check)
        assert!(true);
    }
    
    #[test]
    fn test_stomp_listener_chaining_queues_and_topics() {
        let config = create_test_config();
        let _listener = StompListener::new(config)
            .add_queue("queue1", |_msg: String| async { Ok(()) })
            .add_queue("queue2", |_msg: String| async { Ok(()) })
            .add_topic("topic1", |_msg: String| async { Ok(()) })
            .add_topic("topic2", |_msg: String| async { Ok(()) });
        
        // Should be able to chain multiple queue and topic additions
        assert!(true);
    }
    
    #[tokio::test]
    async fn test_stomp_listener_with_mock_runner_run_success() {
        let mut mock_runner = MockRunner::new();
        
        // Set expectation that run() will be called once and succeed
        mock_runner
            .expect_run()
            .times(1)
            .returning(|| Ok(()));
        
        let listener = StompListener::with_runner(mock_runner);
        let result = listener.run().await;
        
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_stomp_listener_with_mock_runner_run_failure() {
        let mut mock_runner = MockRunner::new();
        
        // Set expectation that run() will be called once and fail
        mock_runner
            .expect_run()
            .times(1)
            .returning(|| Err(anyhow!("Test error")));
        
        let listener = StompListener::with_runner(mock_runner);
        let result = listener.run().await;
        
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Test error");
    }
    
    #[tokio::test]
    async fn test_stomp_listener_run_background_creates_handle() {
        let mut mock_runner = MockRunner::new();
        
        // Set expectation for run_with_shutdown_signal
        mock_runner
            .expect_run_with_shutdown_signal()
            .times(1)
            .returning(|_shutdown_tx| Ok(()));
        
        let listener = StompListener::with_runner(mock_runner);
        let handle = listener.run_background();
        
        // Verify we got a handle
        assert!(true);
        
        // Test shutdown
        let result = handle.shutdown_and_wait().await;
        assert!(result.is_ok());
    }
    
    #[tokio::test] 
    async fn test_stomp_listener_run_background_with_shutdown_failure() {
        let mut mock_runner = MockRunner::new();
        
        // Set expectation that run_with_shutdown_signal will fail
        mock_runner
            .expect_run_with_shutdown_signal()
            .times(1)
            .returning(|_shutdown_tx| Err(anyhow!("Connection failed")));
        
        let listener = StompListener::with_runner(mock_runner);
        let handle = listener.run_background();
        
        // The error should propagate through the handle
        let result = handle.shutdown_and_wait().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Connection failed"));
    }
    
    #[tokio::test] 
    async fn test_stomp_listener_run_background_shutdown_signal_propagation() {
        let mut mock_runner = MockRunner::new();
        
        // Set expectation for run_with_shutdown_signal that simulates proper shutdown handling
        mock_runner
            .expect_run_with_shutdown_signal()
            .times(1)
            .returning(|_shutdown_tx| Ok(()));
        
        let listener = StompListener::with_runner(mock_runner);
        let handle = listener.run_background();
        
        // Give the task a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;
        
        // Send shutdown and wait
        let result = handle.shutdown_and_wait().await;
        assert!(result.is_ok());
    }
    
    #[test] 
    fn test_multiple_queue_handlers_with_different_signatures() {
        let config = create_test_config();
        
        // Test with different handler signatures to ensure they all compile
        let _listener = StompListener::new(config)
            .add_queue("queue1", |_msg: String| async move { 
                // Simple success handler
                Ok(()) 
            })
            .add_queue("queue2", |msg: String| async move { 
                // Handler that uses the message
                if msg.is_empty() {
                    Err(anyhow!("Empty message"))
                } else {
                    Ok(())
                }
            })
            .add_topic("topic1", |_msg: String| async move { 
                // Simple success handler for topic
                Ok(()) 
            });
        
        assert!(true); // Compilation success test
    }
    
    #[test]
    fn test_stomp_listener_with_runner_constructor() {
        let mock_runner = MockRunner::new();
        // Don't set any expectations since we're just testing construction
        
        let _listener = StompListener::with_runner(mock_runner);
        
        // Should be able to create a listener with a mock runner
        assert!(true);
    }
    
    #[tokio::test] 
    async fn test_stomp_runner_implements_runner_trait() {
        // Test that StompRunner correctly implements Runner trait
        let config = create_test_config();
        let runner = StompRunner::new().with_config(config);
        
        // This should compile and work through the Runner trait
        let _listener = StompListener::with_runner(runner);
        
        // We can't actually run it without a real STOMP server, but we can verify
        // that the trait implementation compiles and the listener can be created
        assert!(true);
    }
    
    #[tokio::test] 
    async fn test_runner_trait_bounds_compile() {
        // Test that our Runner trait has the correct bounds
        fn require_send_sync<T: Send + Sync>(_: &T) {}
        
        let mock_runner = MockRunner::new();
        require_send_sync(&mock_runner);
        
        // Should compile without issues
        assert!(true);
    }
    
    #[tokio::test] 
    async fn test_concurrent_shutdown_calls() {
        let mut mock_runner = MockRunner::new();
        
        mock_runner
            .expect_run_with_shutdown_signal()
            .times(1)
            .returning(|_shutdown_tx| Ok(()));
        
        let listener = StompListener::with_runner(mock_runner);
        let handle = listener.run_background();
        
        // Test that multiple concurrent shutdown calls don't cause issues
        handle.shutdown();
        handle.shutdown();
        
        let result = handle.shutdown_and_wait().await;
        assert!(result.is_ok());
    }
}
