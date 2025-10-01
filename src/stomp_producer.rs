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