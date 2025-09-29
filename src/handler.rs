use anyhow::Result;
use std::future::Future;
use std::pin::Pin;
use std::time::Instant;
use tracing::{debug, error};

/// Type alias for handler functions
pub type HandlerFn = fn(String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;

/// Message handlers struct containing both topic and queue handlers
#[derive(Clone)]
pub struct MessageHandlers;

impl MessageHandlers {
    /// Create a new instance of message handlers
    pub fn new() -> Self {
        Self
    }

    /// Default topic message handler
    /// Processes messages received from topics
    pub async fn topic_handler(msg: String) -> Result<()> {
        let start_time = Instant::now();

        debug!("📢 Processing topic message: {}", msg);

        // Simulate some processing work
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        // Log successful processing
        let processing_time = start_time.elapsed();
        debug!(
            "✅ Topic message processed successfully in {}ms",
            processing_time.as_millis()
        );

        Ok(())
    }

    /// Default queue message handler  
    /// Processes messages received from queues
    pub async fn queue_handler(msg: String) -> Result<()> {
        let start_time = Instant::now();

        debug!("📬 Processing queue message: {}", msg);

        // Simulate some processing work
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        // Log successful processing
        let processing_time = start_time.elapsed();
        debug!(
            "✅ Queue message processed successfully in {}ms",
            processing_time.as_millis()
        );

        Ok(())
    }

    /// Get the topic handler function
    pub fn get_topic_handler() -> HandlerFn {
        |msg: String| Box::pin(Self::topic_handler(msg))
    }

    /// Get the queue handler function
    pub fn get_queue_handler() -> HandlerFn {
        |msg: String| Box::pin(Self::queue_handler(msg))
    }
}

impl Default for MessageHandlers {
    fn default() -> Self {
        Self::new()
    }
}

/// Example custom handler that could be used for specific use cases
pub struct CustomHandlers {
    pub name: String,
}

impl CustomHandlers {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
        }
    }

    /// Custom topic handler with specific business logic
    pub async fn custom_topic_handler(&self, msg: &str) -> Result<()> {
        debug!(
            "🎯 [{}] Custom topic handler processing: {}",
            self.name,
            msg.chars().take(50).collect::<String>()
        );

        // Custom business logic here
        match self.process_topic_message(msg).await {
            Ok(_) => {
                debug!("🎉 [{}] Custom topic processing completed", self.name);
                Ok(())
            }
            Err(e) => {
                error!("❌ [{}] Custom topic processing failed: {}", self.name, e);
                Err(e)
            }
        }
    }

    /// Custom queue handler with specific business logic
    pub async fn custom_queue_handler(&self, msg: &str) -> Result<()> {
        debug!(
            "🎯 [{}] Custom queue handler processing: {}",
            self.name,
            msg.chars().take(50).collect::<String>()
        );

        // Custom business logic here
        match self.process_queue_message(msg).await {
            Ok(_) => {
                debug!("🎉 [{}] Custom queue processing completed", self.name);
                Ok(())
            }
            Err(e) => {
                error!("❌ [{}] Custom queue processing failed: {}", self.name, e);
                Err(e)
            }
        }
    }

    /// Process topic message - implement your business logic here
    async fn process_topic_message(&self, msg: &str) -> Result<()> {
        // Example: Parse JSON, validate, transform, etc.
        debug!("[{}] Processing topic message content: {}", self.name, msg);

        // Simulate processing delay
        tokio::time::sleep(tokio::time::Duration::from_millis(25)).await;

        Ok(())
    }

    /// Process queue message - implement your business logic here
    async fn process_queue_message(&self, msg: &str) -> Result<()> {
        // Example: Database operations, API calls, file processing, etc.
        debug!("[{}] Processing queue message content: {}", self.name, msg);

        // Simulate processing delay
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        Ok(())
    }

    /// Get custom topic handler function
    pub fn get_custom_topic_handler(
        &self,
    ) -> impl Fn(&str) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + Clone + 'static
    {
        let name = self.name.clone();
        move |msg: &str| {
            let msg_owned = msg.to_string();
            let name_owned = name.clone();
            Box::pin(async move {
                let handler = CustomHandlers::new(&name_owned);
                handler.custom_topic_handler(&msg_owned).await
            })
        }
    }

    /// Get custom queue handler function
    pub fn get_custom_queue_handler(
        &self,
    ) -> impl Fn(&str) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + Clone + 'static
    {
        let name = self.name.clone();
        move |msg: &str| {
            let msg_owned = msg.to_string();
            let name_owned = name.clone();
            Box::pin(async move {
                let handler = CustomHandlers::new(&name_owned);
                handler.custom_queue_handler(&msg_owned).await
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_topic_handler() {
        let result = MessageHandlers::topic_handler("test topic message".to_string()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_queue_handler() {
        let result = MessageHandlers::queue_handler("test queue message".to_string()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_custom_handlers() {
        let custom_handler = CustomHandlers::new("test-handler");

        let topic_result = custom_handler.custom_topic_handler("test topic").await;
        assert!(topic_result.is_ok());

        let queue_result = custom_handler.custom_queue_handler("test queue").await;
        assert!(queue_result.is_ok());
    }
}
