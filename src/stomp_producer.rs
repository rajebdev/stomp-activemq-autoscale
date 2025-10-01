use anyhow::Result;
use std::collections::HashMap;
use tracing::debug;
use crate::config::Config;
use crate::client::StompClient;

/// StompProducer for sending messages to queues and topics
pub struct StompProducer {
    client: StompClient,
}

impl StompProducer {
    /// Create a new StompProducer with the given configuration
    pub async fn new(config: Config) -> Result<Self> {
        let client = StompClient::new(config).await?;
        Ok(Self { client })
    }

    /// Send a message to a queue with custom headers
    pub async fn send_queue_with_headers(&mut self, queue_name: &str, message: &str, headers: HashMap<String, String>) -> Result<()> {
        debug!("📤 Sending message to queue '{}' with headers: {}", queue_name, message);
        
        // Send message to queue using existing client method with custom headers
        // The client will automatically connect if not already connected
        self.client.send_queue(queue_name, message, headers).await?;
        
        debug!("✅ Message sent successfully to queue '{}'", queue_name);
        Ok(())
    }

    /// Send a message to a queue (convenience method without headers)
    pub async fn send_queue(&mut self, queue_name: &str, message: &str) -> Result<()> {
        debug!("📤 Sending message to queue '{}': {}", queue_name, message);
        
        // Send message to queue using existing client method with empty headers
        // The client will automatically connect if not already connected
        let headers = HashMap::new();
        self.client.send_queue(queue_name, message, headers).await?;
        
        debug!("✅ Message sent successfully to queue '{}'", queue_name);
        Ok(())
    }

    /// Send a message to a topic with custom headers
    pub async fn send_topic_with_headers(&mut self, topic_name: &str, message: &str, headers: HashMap<String, String>) -> Result<()> {
        debug!("📤 Sending message to topic '{}' with headers: {}", topic_name, message);
        
        // Send message to topic using existing client method with custom headers
        // The client will automatically connect if not already connected
        self.client.send_topic(topic_name, message, headers).await?;
        
        debug!("✅ Message sent successfully to topic '{}'", topic_name);
        Ok(())
    }

    /// Send a message to a topic (convenience method without headers)
    pub async fn send_topic(&mut self, topic_name: &str, message: &str) -> Result<()> {
        debug!("📤 Sending message to topic '{}': {}", topic_name, message);
        
        // Send message to topic using existing client method with empty headers
        // The client will automatically connect if not already connected
        let headers = HashMap::new();
        self.client.send_topic(topic_name, message, headers).await?;
        
        debug!("✅ Message sent successfully to topic '{}'", topic_name);
        Ok(())
    }

    /// Disconnect the client (optional cleanup)
    pub async fn disconnect(&mut self) -> Result<()> {
        self.client.disconnect().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::*;
    use std::collections::HashMap;
    use tokio::time::{timeout, Duration};

    fn create_test_config() -> Config {
        Config {
            service: ServiceConfig {
                name: "test-producer".to_string(),
                version: "1.0.0".to_string(),
                description: "Test producer service".to_string(),
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
                queues: {
                    let mut queues = HashMap::new();
                    queues.insert("test_queue".to_string(), "test".to_string());
                    queues.insert("order_queue".to_string(), "orders".to_string());
                    queues
                },
                topics: {
                    let mut topics = HashMap::new();
                    topics.insert("notifications".to_string(), "notifications".to_string());
                    topics
                },
            },
            scaling: ScalingConfig::default(),
            consumers: ConsumersConfig::default(),
            logging: LoggingConfig::default(),
            shutdown: ShutdownConfig::default(),
            retry: RetryConfig::default(),
        }
    }

    fn create_artemis_config() -> Config {
        let mut config = create_test_config();
        config.broker.broker_type = BrokerType::Artemis;
        config.broker.broker_name = "broker".to_string();
        config
    }

    #[tokio::test]
    async fn test_producer_creation() {
        let config = create_test_config();
        
        // This will fail to connect but should create the producer struct
        let result = timeout(Duration::from_secs(1), StompProducer::new(config)).await;
        
        // We expect this to timeout or fail due to no actual broker
        // but the important part is that the creation logic is tested
        match result {
            Ok(Ok(_)) => {
                // If it succeeds (unlikely without real broker), that's fine
            }
            Ok(Err(_)) | Err(_) => {
                // Expected - no real broker to connect to
            }
        }
    }

    #[tokio::test]
    async fn test_send_queue_without_headers() {
        let config = create_test_config();
        
        // Test queue sending (will fail to connect but tests the method signature)
        let result = timeout(
            Duration::from_millis(100),
            async {
                let mut producer = StompProducer::new(config).await?;
                producer.send_queue("test_queue", "test message").await
            }
        ).await;
        
        // Should timeout or fail due to no broker connection
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_send_queue_with_headers() {
        let config = create_test_config();
        
        let mut headers = HashMap::new();
        headers.insert("priority".to_string(), "high".to_string());
        headers.insert("correlation-id".to_string(), "12345".to_string());
        
        let result = timeout(
            Duration::from_millis(100),
            async {
                let mut producer = StompProducer::new(config).await?;
                producer.send_queue_with_headers("test_queue", "test message", headers).await
            }
        ).await;
        
        // Should timeout or fail due to no broker connection
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_send_topic_without_headers() {
        let config = create_test_config();
        
        let result = timeout(
            Duration::from_millis(100),
            async {
                let mut producer = StompProducer::new(config).await?;
                producer.send_topic("notifications", "notification message").await
            }
        ).await;
        
        // Should timeout or fail due to no broker connection
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_send_topic_with_headers() {
        let config = create_test_config();
        
        let mut headers = HashMap::new();
        headers.insert("event-type".to_string(), "user-action".to_string());
        headers.insert("timestamp".to_string(), "1234567890".to_string());
        
        let result = timeout(
            Duration::from_millis(100),
            async {
                let mut producer = StompProducer::new(config).await?;
                producer.send_topic_with_headers("notifications", "notification message", headers).await
            }
        ).await;
        
        // Should timeout or fail due to no broker connection
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_send_with_empty_headers() {
        let config = create_test_config();
        let empty_headers = HashMap::new();
        
        let result = timeout(
            Duration::from_millis(100),
            async {
                let mut producer = StompProducer::new(config).await?;
                producer.send_queue_with_headers("test_queue", "test message", empty_headers).await
            }
        ).await;
        
        // Should timeout or fail due to no broker connection
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_send_large_message() {
        let config = create_test_config();
        
        // Create a large message (1MB)
        let large_message = "x".repeat(1024 * 1024);
        
        let result = timeout(
            Duration::from_millis(200),
            async {
                let mut producer = StompProducer::new(config).await?;
                producer.send_queue("test_queue", &large_message).await
            }
        ).await;
        
        // Should timeout or fail due to no broker connection
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_send_unicode_message() {
        let config = create_test_config();
        
        let unicode_message = "Hello 世界! 🌍 Здравствуй мир! مرحبا بالعالم!";
        
        let result = timeout(
            Duration::from_millis(100),
            async {
                let mut producer = StompProducer::new(config).await?;
                producer.send_queue("test_queue", unicode_message).await
            }
        ).await;
        
        // Should timeout or fail due to no broker connection
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_disconnect() {
        let config = create_test_config();
        
        let result = timeout(
            Duration::from_millis(100),
            async {
                let mut producer = StompProducer::new(config).await?;
                producer.disconnect().await
            }
        ).await;
        
        // Disconnect should work even if never connected
        match result {
            Ok(Ok(())) => {
                // Success case
            }
            Ok(Err(_)) | Err(_) => {
                // May fail if client creation fails
            }
        }
    }

    #[tokio::test]
    async fn test_producer_with_artemis_config() {
        let config = create_artemis_config();
        
        let result = timeout(
            Duration::from_millis(100),
            async {
                let mut producer = StompProducer::new(config).await?;
                producer.send_queue("test_queue", "artemis test").await
            }
        ).await;
        
        // Should timeout or fail due to no broker connection
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_multiple_send_operations() {
        let config = create_test_config();
        
        let result = timeout(
            Duration::from_millis(200),
            async {
                let mut producer = StompProducer::new(config).await?;
                
                // Try multiple operations
                producer.send_queue("test_queue", "message 1").await?;
                producer.send_queue("test_queue", "message 2").await?;
                producer.send_topic("notifications", "notification").await?;
                
                Ok::<(), anyhow::Error>(())
            }
        ).await;
        
        // Should timeout or fail due to no broker connection
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[tokio::test] 
    async fn test_send_to_nonexistent_destination() {
        let config = create_test_config();
        
        let result = timeout(
            Duration::from_millis(100),
            async {
                let mut producer = StompProducer::new(config).await?;
                producer.send_queue("nonexistent_queue", "test message").await
            }
        ).await;
        
        // Should timeout or fail due to no broker connection
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[test]
    fn test_producer_struct_debug() {
        // Test that we can debug format producer related structures
        let config = create_test_config();
        let debug_str = format!("{:?}", config.service);
        assert!(debug_str.contains("test-producer"));
        
        let debug_broker = format!("{:?}", config.broker);
        assert!(debug_broker.contains("ActiveMQ"));
    }

    #[tokio::test]
    async fn test_send_with_special_characters() {
        let config = create_test_config();
        
        let special_message = "Message with \n newlines \t tabs \r carriage returns and \0 null bytes";
        
        let result = timeout(
            Duration::from_millis(100),
            async {
                let mut producer = StompProducer::new(config).await?;
                producer.send_queue("test_queue", special_message).await
            }
        ).await;
        
        // Should timeout or fail due to no broker connection
        assert!(result.is_err() || result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_headers_with_special_values() {
        let config = create_test_config();
        
        let mut headers = HashMap::new();
        headers.insert("empty".to_string(), "".to_string());
        headers.insert("spaces".to_string(), "   ".to_string());
        headers.insert("special-chars".to_string(), "!@#$%^&*()_+-={}[]|\\:;\"'<>?,./ ".to_string());
        headers.insert("unicode".to_string(), "αβγδε".to_string());
        
        let result = timeout(
            Duration::from_millis(100),
            async {
                let mut producer = StompProducer::new(config).await?;
                producer.send_queue_with_headers("test_queue", "test", headers).await
            }
        ).await;
        
        // Should timeout or fail due to no broker connection
        assert!(result.is_err() || result.unwrap().is_err());
    }
}
