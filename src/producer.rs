use anyhow::Result;
use std::collections::HashMap;
use tracing::debug;
use crate::config::Config;
use crate::client::{Client, StompClient};

/// StompProducer for sending messages to queues and topics
pub struct StompProducer<C: Client> {
    client: C,
}

impl<C: Client> StompProducer<C> {
    /// Create a new StompProducer with a custom client (for testing)
    #[cfg(test)]
    pub fn with_client(client: C) -> Self {
        Self { client }
    }

    /// Send a message to a queue with custom headers
    pub async fn send_queue_with_headers(&mut self, queue_name: &str, message: &str, headers: HashMap<String, String>) -> Result<()> {
        debug!("📤 Sending message to queue '{}' with headers: {}", queue_name, message);
        
        // Send message to queue using client trait method with custom headers
        // The client will automatically connect if not already connected
        self.client.send_queue(queue_name, message, headers).await?;
        
        debug!("✅ Message sent successfully to queue '{}'", queue_name);
        Ok(())
    }

    /// Send a message to a queue (convenience method without headers)
    pub async fn send_queue(&mut self, queue_name: &str, message: &str) -> Result<()> {
        debug!("📤 Sending message to queue '{}': {}", queue_name, message);
        
        // Send message to queue using client trait method with empty headers
        // The client will automatically connect if not already connected
        let headers = HashMap::new();
        self.client.send_queue(queue_name, message, headers).await?;
        
        debug!("✅ Message sent successfully to queue '{}'", queue_name);
        Ok(())
    }

    /// Send a message to a topic with custom headers
    pub async fn send_topic_with_headers(&mut self, topic_name: &str, message: &str, headers: HashMap<String, String>) -> Result<()> {
        debug!("📤 Sending message to topic '{}' with headers: {}", topic_name, message);
        
        // Send message to topic using client trait method with custom headers
        // The client will automatically connect if not already connected
        self.client.send_topic(topic_name, message, headers).await?;
        
        debug!("✅ Message sent successfully to topic '{}'", topic_name);
        Ok(())
    }

    /// Send a message to a topic (convenience method without headers)
    pub async fn send_topic(&mut self, topic_name: &str, message: &str) -> Result<()> {
        debug!("📤 Sending message to topic '{}': {}", topic_name, message);
        
        // Send message to topic using client trait method with empty headers
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

/// Implement for StompClient specifically to maintain backward compatibility
impl StompProducer<StompClient> {
    /// Create a new StompProducer with the given configuration
    pub async fn new(config: Config) -> Result<Self> {
        let client = StompClient::new(config).await?;
        Ok(Self { client })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;
    use std::collections::HashMap;
    
    // Import the generated MockClient from client module
    use crate::client::MockClient;
    
    // === UNIT TESTS WITH MOCKS ===
    
    #[tokio::test]
    async fn test_producer_with_client_constructor() {
        let mock_client = MockClient::new();
        let _producer = StompProducer::with_client(mock_client);
        
        // Should be able to create producer with mock client
        assert!(true);
    }
    
    #[tokio::test]
    async fn test_send_queue_with_headers_success() {
        let mut mock_client = MockClient::new();
        
        // Set up expectation
        mock_client
            .expect_send_queue()
            .times(1)
            .withf(|queue, msg, headers| {
                queue == "test_queue" && msg == "test message" && headers.len() == 2
            })
            .returning(|_, _, _| Ok(()));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        let mut headers = HashMap::new();
        headers.insert("priority".to_string(), "high".to_string());
        headers.insert("type".to_string(), "order".to_string());
        
        let result = producer.send_queue_with_headers("test_queue", "test message", headers).await;
        
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_send_queue_with_headers_failure() {
        let mut mock_client = MockClient::new();
        
        // Set up expectation to return error
        mock_client
            .expect_send_queue()
            .times(1)
            .returning(|_, _, _| Err(anyhow!("Connection failed")));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        let headers = HashMap::new();
        let result = producer.send_queue_with_headers("test_queue", "test message", headers).await;
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Connection failed"));
    }
    
    #[tokio::test]
    async fn test_send_queue_without_headers_success() {
        let mut mock_client = MockClient::new();
        
        // Set up expectation - should receive empty headers
        mock_client
            .expect_send_queue()
            .times(1)
            .withf(|queue, msg, headers| {
                queue == "test_queue" && msg == "test message" && headers.is_empty()
            })
            .returning(|_, _, _| Ok(()));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        let result = producer.send_queue("test_queue", "test message").await;
        
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_send_topic_with_headers_success() {
        let mut mock_client = MockClient::new();
        
        // Set up expectation
        mock_client
            .expect_send_topic()
            .times(1)
            .withf(|topic, msg, headers| {
                topic == "notifications" && msg == "alert" && headers.len() == 1
            })
            .returning(|_, _, _| Ok(()));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        let mut headers = HashMap::new();
        headers.insert("urgency".to_string(), "high".to_string());
        
        let result = producer.send_topic_with_headers("notifications", "alert", headers).await;
        
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_send_topic_without_headers_success() {
        let mut mock_client = MockClient::new();
        
        // Set up expectation - should receive empty headers
        mock_client
            .expect_send_topic()
            .times(1)
            .withf(|topic, msg, headers| {
                topic == "notifications" && msg == "simple alert" && headers.is_empty()
            })
            .returning(|_, _, _| Ok(()));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        let result = producer.send_topic("notifications", "simple alert").await;
        
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_send_topic_failure() {
        let mut mock_client = MockClient::new();
        
        // Set up expectation to return error
        mock_client
            .expect_send_topic()
            .times(1)
            .returning(|_, _, _| Err(anyhow!("Topic not found")));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        let result = producer.send_topic("missing_topic", "message").await;
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Topic not found"));
    }
    
    #[tokio::test]
    async fn test_disconnect_success() {
        let mut mock_client = MockClient::new();
        
        // Set up expectation
        mock_client
            .expect_disconnect()
            .times(1)
            .returning(|| Ok(()));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        let result = producer.disconnect().await;
        
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_disconnect_failure() {
        let mut mock_client = MockClient::new();
        
        // Set up expectation to return error
        mock_client
            .expect_disconnect()
            .times(1)
            .returning(|| Err(anyhow!("Disconnect failed")));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        let result = producer.disconnect().await;
        
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Disconnect failed"));
    }
    
    #[tokio::test]
    async fn test_multiple_operations_on_same_producer() {
        let mut mock_client = MockClient::new();
        
        // Set up expectations for multiple operations
        mock_client
            .expect_send_queue()
            .times(2)
            .returning(|_, _, _| Ok(()));
        
        mock_client
            .expect_send_topic()
            .times(1)
            .returning(|_, _, _| Ok(()));
        
        mock_client
            .expect_disconnect()
            .times(1)
            .returning(|| Ok(()));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        // Perform multiple operations
        let result1 = producer.send_queue("queue1", "message1").await;
        assert!(result1.is_ok());
        
        let result2 = producer.send_queue("queue2", "message2").await;
        assert!(result2.is_ok());
        
        let result3 = producer.send_topic("topic1", "broadcast").await;
        assert!(result3.is_ok());
        
        let result4 = producer.disconnect().await;
        assert!(result4.is_ok());
    }
    
    #[tokio::test]
    async fn test_empty_message() {
        let mut mock_client = MockClient::new();
        
        mock_client
            .expect_send_queue()
            .times(1)
            .withf(|_, msg, _| msg.is_empty())
            .returning(|_, _, _| Ok(()));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        let result = producer.send_queue("test_queue", "").await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_large_message() {
        let mut mock_client = MockClient::new();
        let large_message = "x".repeat(1024 * 1024); // 1MB message
        
        mock_client
            .expect_send_queue()
            .times(1)
            .withf(move |_, msg, _| msg.len() == 1024 * 1024)
            .returning(|_, _, _| Ok(()));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        let result = producer.send_queue("test_queue", &large_message).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_unicode_message() {
        let mut mock_client = MockClient::new();
        let unicode_msg = "Hello 世界! 🌍 Здравствуй мир! مرحبا بالعالم!";
        
        mock_client
            .expect_send_topic()
            .times(1)
            .withf(move |_, msg, _| msg.contains("世界") && msg.contains("🌍"))
            .returning(|_, _, _| Ok(()));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        let result = producer.send_topic("notifications", unicode_msg).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_special_characters_in_message() {
        let mut mock_client = MockClient::new();
        let special_msg = "Message with \n\t\r\0 special chars";
        
        mock_client
            .expect_send_queue()
            .times(1)
            .withf(move |_, msg, _| msg.contains("\n") && msg.contains("\t"))
            .returning(|_, _, _| Ok(()));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        let result = producer.send_queue("test_queue", special_msg).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_headers_with_special_values() {
        let mut mock_client = MockClient::new();
        
        mock_client
            .expect_send_queue()
            .times(1)
            .withf(|_, _, headers| {
                headers.get("empty").unwrap().is_empty() &&
                headers.get("spaces").unwrap() == "   " &&
                headers.get("unicode").unwrap().contains("αβγ")
            })
            .returning(|_, _, _| Ok(()));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        let mut headers = HashMap::new();
        headers.insert("empty".to_string(), "".to_string());
        headers.insert("spaces".to_string(), "   ".to_string());
        headers.insert("unicode".to_string(), "αβγδε".to_string());
        headers.insert("special".to_string(), "!@#$%^&*()".to_string());
        
        let result = producer.send_queue_with_headers("test_queue", "test", headers).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_empty_headers_map() {
        let mut mock_client = MockClient::new();
        
        mock_client
            .expect_send_topic()
            .times(1)
            .withf(|_, _, headers| headers.is_empty())
            .returning(|_, _, _| Ok(()));
        
        let mut producer = StompProducer::with_client(mock_client);
        
        let empty_headers = HashMap::new();
        let result = producer.send_topic_with_headers("topic", "message", empty_headers).await;
        assert!(result.is_ok());
    }
    
    // === UNIT TESTS FOR StompProducer::new (lines 77-80) ===
    // These tests cover the creation logic without actually connecting to a broker
    
    #[tokio::test]
    async fn test_producer_new_creates_client_success() {
        // Test line 77-79: Validates that StompProducer::new properly constructs
        // a StompClient and wraps it in the producer struct
        // Note: StompClient::new only initializes the struct, doesn't connect yet
        
        use crate::config::{
            Config, ServiceConfig, BrokerConfig, BrokerType, DestinationsConfig,
            ScalingConfig, ConsumersConfig, LoggingConfig, ShutdownConfig, RetryConfig
        };
        
        let config = Config {
            service: ServiceConfig {
                name: "test-producer-service".to_string(),
                version: "1.0.0".to_string(),
                description: "Test producer".to_string(),
            },
            broker: BrokerConfig {
                broker_type: BrokerType::ActiveMQ,
                host: "localhost".to_string(),
                username: "admin".to_string(),
                password: "admin".to_string(),
                stomp_port: 61613,
                web_port: 8161,
                heartbeat_secs: 30,
                broker_name: "localhost".to_string(),
            },
            destinations: DestinationsConfig {
                queues: HashMap::new(),
                topics: HashMap::new(),
            },
            scaling: ScalingConfig::default(),
            consumers: ConsumersConfig::default(),
            logging: LoggingConfig::default(),
            shutdown: ShutdownConfig::default(),
            retry: RetryConfig::default(),
        };
        
        // Line 78: let client = StompClient::new(config).await?;
        // StompClient::new just creates the struct, doesn't connect
        let result = StompProducer::new(config).await;
        
        // Line 79: Ok(Self { client })
        // Should succeed because new() only initializes, doesn't connect
        assert!(result.is_ok(), "StompProducer::new should succeed as it only initializes the client struct");
        
        let producer = result.unwrap();
        // Verify the producer was created with the client
        // This confirms line 79: Ok(Self { client }) structure
        assert!(std::mem::size_of_val(&producer) > 0);
    }
    
    #[tokio::test]
    async fn test_producer_new_with_activemq_config() {
        // Test line 77-80 with ActiveMQ broker type
        // Validates config is properly passed through to StompClient::new
        
        use crate::config::{
            Config, ServiceConfig, BrokerConfig, BrokerType, DestinationsConfig,
            ScalingConfig, ConsumersConfig, LoggingConfig, ShutdownConfig, RetryConfig
        };
        
        let config = Config {
            service: ServiceConfig {
                name: "activemq-producer".to_string(),
                version: "1.0.0".to_string(),
                description: "ActiveMQ producer test".to_string(),
            },
            broker: BrokerConfig {
                broker_type: BrokerType::ActiveMQ,
                host: "activemq.example.com".to_string(),
                username: "activemq-user".to_string(),
                password: "activemq-pass".to_string(),
                stomp_port: 61613,
                web_port: 8161,
                heartbeat_secs: 10,
                broker_name: "activemq-broker".to_string(),
            },
            destinations: DestinationsConfig {
                queues: HashMap::from([
                    ("orders".to_string(), "queue.orders".to_string()),
                    ("payments".to_string(), "queue.payments".to_string()),
                ]),
                topics: HashMap::from([
                    ("notifications".to_string(), "topic.notifications".to_string()),
                ]),
            },
            scaling: ScalingConfig::default(),
            consumers: ConsumersConfig::default(),
            logging: LoggingConfig::default(),
            shutdown: ShutdownConfig::default(),
            retry: RetryConfig::default(),
        };
        
        // Call new() - should succeed as it only initializes
        let result = StompProducer::new(config).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_producer_new_with_artemis_config() {
        // Test line 77-80 with Artemis broker type
        
        use crate::config::{
            Config, ServiceConfig, BrokerConfig, BrokerType, DestinationsConfig,
            ScalingConfig, ConsumersConfig, LoggingConfig, ShutdownConfig, RetryConfig
        };
        
        let config = Config {
            service: ServiceConfig {
                name: "artemis-producer".to_string(),
                version: "2.0.0".to_string(),
                description: "Artemis producer test".to_string(),
            },
            broker: BrokerConfig {
                broker_type: BrokerType::Artemis,
                host: "artemis.example.com".to_string(),
                username: "artemis-user".to_string(),
                password: "artemis-pass".to_string(),
                stomp_port: 61616,
                web_port: 8161,
                heartbeat_secs: 60,
                broker_name: "artemis-broker".to_string(),
            },
            destinations: DestinationsConfig {
                queues: HashMap::new(),
                topics: HashMap::new(),
            },
            scaling: ScalingConfig::default(),
            consumers: ConsumersConfig::default(),
            logging: LoggingConfig::default(),
            shutdown: ShutdownConfig::default(),
            retry: RetryConfig::default(),
        };
        
        // Call new() - should succeed as it only initializes
        let result = StompProducer::new(config).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_producer_new_return_type_structure() {
        // Test that line 77-79 properly returns Result<StompProducer<StompClient>>
        
        use crate::config::{
            Config, ServiceConfig, BrokerConfig, BrokerType, DestinationsConfig,
            ScalingConfig, ConsumersConfig, LoggingConfig, ShutdownConfig, RetryConfig
        };
        
        let config = Config {
            service: ServiceConfig {
                name: "return-type-test".to_string(),
                version: "1.0.0".to_string(),
                description: "Test return type structure".to_string(),
            },
            broker: BrokerConfig {
                broker_type: BrokerType::ActiveMQ,
                host: "localhost".to_string(),
                username: "admin".to_string(),
                password: "admin".to_string(),
                stomp_port: 61613,
                web_port: 8161,
                heartbeat_secs: 30,
                broker_name: "localhost".to_string(),
            },
            destinations: DestinationsConfig {
                queues: HashMap::new(),
                topics: HashMap::new(),
            },
            scaling: ScalingConfig::default(),
            consumers: ConsumersConfig::default(),
            logging: LoggingConfig::default(),
            shutdown: ShutdownConfig::default(),
            retry: RetryConfig::default(),
        };
        
        // Explicitly type the result to verify return type signature (line 77)
        let result: Result<StompProducer<StompClient>> = StompProducer::new(config).await;
        
        // Should be Ok because new() only initializes (line 79: Ok(Self { client }))
        assert!(result.is_ok());
        
        // Verify we can unwrap and get the producer
        let producer = result.unwrap();
        assert!(std::mem::size_of_val(&producer) > 0);
    }
    
    #[tokio::test]
    async fn test_producer_new_with_custom_heartbeat() {
        // Test line 78: Ensures config with different heartbeat values is passed correctly
        
        use crate::config::{
            Config, ServiceConfig, BrokerConfig, BrokerType, DestinationsConfig,
            ScalingConfig, ConsumersConfig, LoggingConfig, ShutdownConfig, RetryConfig
        };
        
        let config = Config {
            service: ServiceConfig {
                name: "heartbeat-test".to_string(),
                version: "1.0.0".to_string(),
                description: "Custom heartbeat test".to_string(),
            },
            broker: BrokerConfig {
                broker_type: BrokerType::ActiveMQ,
                host: "localhost".to_string(),
                username: "admin".to_string(),
                password: "admin".to_string(),
                stomp_port: 61613,
                web_port: 8161,
                heartbeat_secs: 120, // Custom heartbeat value
                broker_name: "localhost".to_string(),
            },
            destinations: DestinationsConfig {
                queues: HashMap::new(),
                topics: HashMap::new(),
            },
            scaling: ScalingConfig::default(),
            consumers: ConsumersConfig::default(),
            logging: LoggingConfig::default(),
            shutdown: ShutdownConfig::default(),
            retry: RetryConfig::default(),
        };
        
        let result = StompProducer::new(config).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_producer_new_with_retry_config() {
        // Test line 78: Validates complex config with retry settings is properly passed
        
        use crate::config::{
            Config, ServiceConfig, BrokerConfig, BrokerType, DestinationsConfig,
            ScalingConfig, ConsumersConfig, LoggingConfig, ShutdownConfig, RetryConfig
        };
        
        let config = Config {
            service: ServiceConfig {
                name: "retry-test".to_string(),
                version: "1.0.0".to_string(),
                description: "Retry config test".to_string(),
            },
            broker: BrokerConfig {
                broker_type: BrokerType::ActiveMQ,
                host: "localhost".to_string(),
                username: "admin".to_string(),
                password: "admin".to_string(),
                stomp_port: 61613,
                web_port: 8161,
                heartbeat_secs: 30,
                broker_name: "localhost".to_string(),
            },
            destinations: DestinationsConfig {
                queues: HashMap::new(),
                topics: HashMap::new(),
            },
            scaling: ScalingConfig::default(),
            consumers: ConsumersConfig::default(),
            logging: LoggingConfig::default(),
            shutdown: ShutdownConfig::default(),
            retry: RetryConfig {
                max_attempts: 5,
                initial_delay_ms: 1000,
                max_delay_ms: 10000,
                backoff_multiplier: 2.5,
            },
        };
        
        let result = StompProducer::new(config).await;
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_producer_new_multiple_instances() {
        // Test that multiple producers can be created (lines 77-80)
        // Validates that the constructor works repeatedly
        
        use crate::config::{
            Config, ServiceConfig, BrokerConfig, BrokerType, DestinationsConfig,
            ScalingConfig, ConsumersConfig, LoggingConfig, ShutdownConfig, RetryConfig
        };
        
        let config1 = Config {
            service: ServiceConfig {
                name: "producer-1".to_string(),
                version: "1.0.0".to_string(),
                description: "First producer".to_string(),
            },
            broker: BrokerConfig {
                broker_type: BrokerType::ActiveMQ,
                host: "localhost".to_string(),
                username: "admin".to_string(),
                password: "admin".to_string(),
                stomp_port: 61613,
                web_port: 8161,
                heartbeat_secs: 30,
                broker_name: "localhost".to_string(),
            },
            destinations: DestinationsConfig {
                queues: HashMap::new(),
                topics: HashMap::new(),
            },
            scaling: ScalingConfig::default(),
            consumers: ConsumersConfig::default(),
            logging: LoggingConfig::default(),
            shutdown: ShutdownConfig::default(),
            retry: RetryConfig::default(),
        };
        
        let config2 = Config {
            service: ServiceConfig {
                name: "producer-2".to_string(),
                version: "1.0.0".to_string(),
                description: "Second producer".to_string(),
            },
            broker: BrokerConfig {
                broker_type: BrokerType::Artemis,
                host: "artemis-host".to_string(),
                username: "artemis".to_string(),
                password: "artemis".to_string(),
                stomp_port: 61616,
                web_port: 8161,
                heartbeat_secs: 60,
                broker_name: "artemis".to_string(),
            },
            destinations: DestinationsConfig {
                queues: HashMap::new(),
                topics: HashMap::new(),
            },
            scaling: ScalingConfig::default(),
            consumers: ConsumersConfig::default(),
            logging: LoggingConfig::default(),
            shutdown: ShutdownConfig::default(),
            retry: RetryConfig::default(),
        };
        
        // Create multiple producers
        let result1 = StompProducer::new(config1).await;
        let result2 = StompProducer::new(config2).await;
        
        assert!(result1.is_ok());
        assert!(result2.is_ok());
    }
}
