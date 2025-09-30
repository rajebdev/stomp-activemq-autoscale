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
    async fn test_message_handlers_new() {
        let _handlers = MessageHandlers::new();
        // Since MessageHandlers is a unit struct, we just verify it can be created
        // The real test is that this compiles and runs without panic
    }

    #[tokio::test]
    async fn test_message_handlers_default() {
        let _handlers = MessageHandlers::default();
        // Verify default implementation works
    }

    #[tokio::test]
    async fn test_topic_handler() {
        let start_time = std::time::Instant::now();
        let result = MessageHandlers::topic_handler("test topic message".to_string()).await;
        let elapsed = start_time.elapsed();
        
        assert!(result.is_ok());
        // Should take at least 10ms due to sleep in handler
        assert!(elapsed.as_millis() >= 10);
    }

    #[tokio::test]
    async fn test_topic_handler_with_empty_message() {
        let result = MessageHandlers::topic_handler("".to_string()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_topic_handler_with_long_message() {
        let long_message = "a".repeat(1000);
        let result = MessageHandlers::topic_handler(long_message).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_topic_handler_with_unicode() {
        let unicode_message = "测试消息 🚀 émojis and ñońáscii";
        let result = MessageHandlers::topic_handler(unicode_message.to_string()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_queue_handler() {
        let start_time = std::time::Instant::now();
        let result = MessageHandlers::queue_handler("test queue message".to_string()).await;
        let elapsed = start_time.elapsed();
        
        assert!(result.is_ok());
        // Should take at least 50ms due to sleep in handler
        assert!(elapsed.as_millis() >= 50);
    }

    #[tokio::test]
    async fn test_queue_handler_with_empty_message() {
        let result = MessageHandlers::queue_handler("".to_string()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_queue_handler_with_long_message() {
        let long_message = "b".repeat(2000);
        let result = MessageHandlers::queue_handler(long_message).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_queue_handler_with_json_message() {
        let json_message = r#"{"id": 123, "action": "process", "data": {"value": "test"}}"#;
        let result = MessageHandlers::queue_handler(json_message.to_string()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_topic_handler_function() {
        let handler_fn = MessageHandlers::get_topic_handler();
        let result = handler_fn("test message".to_string()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_queue_handler_function() {
        let handler_fn = MessageHandlers::get_queue_handler();
        let result = handler_fn("test message".to_string()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_multiple_concurrent_topic_handlers() {
        let handlers = vec![
            tokio::spawn(MessageHandlers::topic_handler("message 1".to_string())),
            tokio::spawn(MessageHandlers::topic_handler("message 2".to_string())),
            tokio::spawn(MessageHandlers::topic_handler("message 3".to_string())),
        ];

        for handle in handlers {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_multiple_concurrent_queue_handlers() {
        let handlers = vec![
            tokio::spawn(MessageHandlers::queue_handler("message 1".to_string())),
            tokio::spawn(MessageHandlers::queue_handler("message 2".to_string())),
            tokio::spawn(MessageHandlers::queue_handler("message 3".to_string())),
        ];

        for handle in handlers {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_custom_handlers_new() {
        let custom_handler = CustomHandlers::new("test-handler");
        assert_eq!(custom_handler.name, "test-handler");
    }

    #[test]
    fn test_custom_handlers_new_with_empty_name() {
        let custom_handler = CustomHandlers::new("");
        assert_eq!(custom_handler.name, "");
    }

    #[test]
    fn test_custom_handlers_new_with_special_chars() {
        let custom_handler = CustomHandlers::new("test-handler-123_@#$");
        assert_eq!(custom_handler.name, "test-handler-123_@#$");
    }

    #[tokio::test]
    async fn test_custom_topic_handler() {
        let custom_handler = CustomHandlers::new("test-handler");
        let start_time = std::time::Instant::now();
        let result = custom_handler.custom_topic_handler("test topic").await;
        let elapsed = start_time.elapsed();
        
        assert!(result.is_ok());
        // Should take at least 25ms due to sleep in process_topic_message
        assert!(elapsed.as_millis() >= 25);
    }

    #[tokio::test]
    async fn test_custom_queue_handler() {
        let custom_handler = CustomHandlers::new("test-handler");
        let start_time = std::time::Instant::now();
        let result = custom_handler.custom_queue_handler("test queue").await;
        let elapsed = start_time.elapsed();
        
        assert!(result.is_ok());
        // Should take at least 100ms due to sleep in process_queue_message
        assert!(elapsed.as_millis() >= 100);
    }

    #[tokio::test]
    async fn test_custom_topic_handler_with_long_message() {
        let custom_handler = CustomHandlers::new("test-handler");
        let long_message = "c".repeat(1500);
        let result = custom_handler.custom_topic_handler(&long_message).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_custom_queue_handler_with_long_message() {
        let custom_handler = CustomHandlers::new("test-handler");
        let long_message = "d".repeat(3000);
        let result = custom_handler.custom_queue_handler(&long_message).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_process_topic_message() {
        let custom_handler = CustomHandlers::new("test-handler");
        let start_time = std::time::Instant::now();
        let result = custom_handler.process_topic_message("test message").await;
        let elapsed = start_time.elapsed();
        
        assert!(result.is_ok());
        assert!(elapsed.as_millis() >= 25);
    }

    #[tokio::test]
    async fn test_process_queue_message() {
        let custom_handler = CustomHandlers::new("test-handler");
        let start_time = std::time::Instant::now();
        let result = custom_handler.process_queue_message("test message").await;
        let elapsed = start_time.elapsed();
        
        assert!(result.is_ok());
        assert!(elapsed.as_millis() >= 100);
    }

    #[tokio::test]
    async fn test_get_custom_topic_handler_function() {
        let custom_handler = CustomHandlers::new("test-handler");
        let handler_fn = custom_handler.get_custom_topic_handler();
        let result = handler_fn("test message").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_get_custom_queue_handler_function() {
        let custom_handler = CustomHandlers::new("test-handler");
        let handler_fn = custom_handler.get_custom_queue_handler();
        let result = handler_fn("test message").await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_custom_handlers_with_different_names() {
        let handler1 = CustomHandlers::new("handler-1");
        let handler2 = CustomHandlers::new("handler-2");
        
        let result1 = handler1.custom_topic_handler("test message 1").await;
        let result2 = handler2.custom_topic_handler("test message 2").await;
        
        assert!(result1.is_ok());
        assert!(result2.is_ok());
        assert_eq!(handler1.name, "handler-1");
        assert_eq!(handler2.name, "handler-2");
    }

    #[tokio::test]
    async fn test_concurrent_custom_handlers() {
        let _custom_handler = CustomHandlers::new("concurrent-test");
        
        let topic_task = tokio::spawn({
            let handler = CustomHandlers::new("topic-handler");
            async move { handler.custom_topic_handler("topic message").await }
        });
        
        let queue_task = tokio::spawn({
            let handler = CustomHandlers::new("queue-handler");
            async move { handler.custom_queue_handler("queue message").await }
        });
        
        let topic_result = topic_task.await.unwrap();
        let queue_result = queue_task.await.unwrap();
        
        assert!(topic_result.is_ok());
        assert!(queue_result.is_ok());
    }

    #[tokio::test]
    async fn test_custom_handler_functions_with_closure() {
        let custom_handler = CustomHandlers::new("closure-test");
        
        let topic_handler = custom_handler.get_custom_topic_handler();
        let queue_handler = custom_handler.get_custom_queue_handler();
        
        // Test that the closures can be called multiple times
        let topic_result1 = topic_handler("message 1").await;
        let topic_result2 = topic_handler("message 2").await;
        let queue_result1 = queue_handler("message 3").await;
        let queue_result2 = queue_handler("message 4").await;
        
        assert!(topic_result1.is_ok());
        assert!(topic_result2.is_ok());
        assert!(queue_result1.is_ok());
        assert!(queue_result2.is_ok());
    }

    #[tokio::test]
    async fn test_handler_timing_consistency() {
        // Test that handlers are consistently taking the expected time
        let (time1, time2, time3) = tokio::join!(
            async {
                let start = std::time::Instant::now();
                MessageHandlers::topic_handler("timing test".to_string()).await.unwrap();
                start.elapsed()
            },
            async {
                let start = std::time::Instant::now();
                MessageHandlers::topic_handler("timing test".to_string()).await.unwrap();
                start.elapsed()
            },
            async {
                let start = std::time::Instant::now();
                MessageHandlers::topic_handler("timing test".to_string()).await.unwrap();
                start.elapsed()
            }
        );
        
        let topic_times = vec![time1, time2, time3];
        
        let (qtime1, qtime2, qtime3) = tokio::join!(
            async {
                let start = std::time::Instant::now();
                MessageHandlers::queue_handler("timing test".to_string()).await.unwrap();
                start.elapsed()
            },
            async {
                let start = std::time::Instant::now();
                MessageHandlers::queue_handler("timing test".to_string()).await.unwrap();
                start.elapsed()
            },
            async {
                let start = std::time::Instant::now();
                MessageHandlers::queue_handler("timing test".to_string()).await.unwrap();
                start.elapsed()
            }
        );
        
        let queue_times = vec![qtime1, qtime2, qtime3];
        
        // All topic handlers should take at least 10ms
        for time in topic_times {
            assert!(time.as_millis() >= 10);
        }
        
        // All queue handlers should take at least 50ms
        for time in queue_times {
            assert!(time.as_millis() >= 50);
        }
    }

    #[tokio::test]
    async fn test_custom_handlers() {
        let custom_handler = CustomHandlers::new("test-handler");

        let topic_result = custom_handler.custom_topic_handler("test topic").await;
        assert!(topic_result.is_ok());

        let queue_result = custom_handler.custom_queue_handler("test queue").await;
        assert!(queue_result.is_ok());
    }

    #[test]
    fn test_message_handlers_clone() {
        let handlers1 = MessageHandlers::new();
        let _handlers2 = handlers1.clone();
        // MessageHandlers is a unit struct, so clone should work without issues
    }

    #[tokio::test]
    async fn test_handler_with_special_characters() {
        let special_message = "Message with newlines\nand tabs\tand quotes\"";
        
        let topic_result = MessageHandlers::topic_handler(special_message.to_string()).await;
        assert!(topic_result.is_ok());
        
        let queue_result = MessageHandlers::queue_handler(special_message.to_string()).await;
        assert!(queue_result.is_ok());
        
        let custom_handler = CustomHandlers::new("special-chars-test");
        let custom_topic_result = custom_handler.custom_topic_handler(special_message).await;
        let custom_queue_result = custom_handler.custom_queue_handler(special_message).await;
        
        assert!(custom_topic_result.is_ok());
        assert!(custom_queue_result.is_ok());
    }

    #[tokio::test]
    async fn test_handler_performance_baseline() {
        // Measure baseline performance for regression testing
        let message = "Performance test message";
        
        // Test default handlers
        let start = std::time::Instant::now();
        MessageHandlers::topic_handler(message.to_string()).await.unwrap();
        let topic_time = start.elapsed();
        
        let start = std::time::Instant::now();
        MessageHandlers::queue_handler(message.to_string()).await.unwrap();
        let queue_time = start.elapsed();
        
        // Test custom handlers
        let custom_handler = CustomHandlers::new("perf-test");
        
        let start = std::time::Instant::now();
        custom_handler.custom_topic_handler(message).await.unwrap();
        let custom_topic_time = start.elapsed();
        
        let start = std::time::Instant::now();
        custom_handler.custom_queue_handler(message).await.unwrap();
        let custom_queue_time = start.elapsed();
        
        // Verify minimum expected times (with some tolerance)
        assert!(topic_time.as_millis() >= 10);
        assert!(queue_time.as_millis() >= 50);
        assert!(custom_topic_time.as_millis() >= 25);
        assert!(custom_queue_time.as_millis() >= 100);
        
        // Verify maximum reasonable times (should not take too long)
        assert!(topic_time.as_millis() < 100);  // Should be much faster than 100ms
        assert!(queue_time.as_millis() < 150);  // Should be much faster than 150ms
        assert!(custom_topic_time.as_millis() < 100);
        assert!(custom_queue_time.as_millis() < 200);
    }
}
