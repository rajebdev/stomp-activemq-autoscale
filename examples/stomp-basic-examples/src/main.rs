use anyhow::Result;
use std::collections::HashMap;
use stomp_activemq_autoscale::{
    config::Config,
    stomp_producer::StompProducer,
    stomp_listener::StompListener,
};
use tracing::info;
use tokio::time::Duration;

// Custom handler for processing notification messages
async fn handle_notification_message(message: String) -> Result<()> {
    info!("🔔 Processing NOTIFICATION: {}", message);
    // Simulate some processing time
    tokio::time::sleep(Duration::from_millis(50)).await;
    Ok(())
}

// Custom handler for processing API request messages
async fn handle_api_request_message(message: String) -> Result<()> {
    info!("🌐 Processing API REQUEST: {}", message);
    // Simulate API processing
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(())
}

// Custom handler for processing general messages
async fn handle_general_message(message: String) -> Result<()> {
    info!("🛒 Processing GENERAL: {}", message);
    // Simulate some processing time
    tokio::time::sleep(Duration::from_millis(1)).await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging first
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .with_thread_ids(false)
        .with_line_number(false)
        .init();
    
    info!("🚀 Starting STOMP Basic Example Application");
    
    // Load configuration from parent directory
    let config = Config::load("config.yaml")
        .or_else(|_| Config::load("../../config.yaml"))?;
    
    // Display startup information
    info!("📋 Service: {} v{}", config.service.name, config.service.version);
    info!("🔗 Broker: {}:{}", config.broker.host, config.broker.stomp_port);
    
    // 1. Create StompProducer (efficient connection reuse)
    let mut stomp_producer = StompProducer::new(config.clone()).await?;
    info!("✅ StompProducer initialized");
    
    // 2. Create StompListener with message handlers and graceful shutdown support
    let stomp_listener = StompListener::new(config.clone())
        .add_queue("default", handle_general_message)
        .add_queue("errorsx", handle_general_message)
        .add_queue("api_requests", handle_api_request_message)
        .add_topic("notifications", handle_notification_message);
    
    info!("✅ StompListener initialized with handlers");
    
    // Start listener with graceful shutdown handle
    let shutdown_handle = stomp_listener.run_background();
    info!("🚀 StompListener started with graceful shutdown support");
    
    // Wait a bit for listener to be ready
    tokio::time::sleep(Duration::from_secs(2)).await;
    
    // 3. Send messages to queues (reuses same connection)
    info!("📤 Sending messages to queues...");
    stomp_producer.send_queue("default", "Hello from basic example queue!").await?;
    stomp_producer.send_queue("api_requests", "API request from basic example").await?;
    stomp_producer.send_queue("errorsx", "Error handling test from basic").await?;
    
    // 3.1. Send messages to queues with custom headers
    info!("📤 Sending messages to queues with JMS standard headers...");
    let mut headers = HashMap::new();
    headers.insert("content-type".to_string(), "application/json".to_string());
    headers.insert("JMSPriority".to_string(), "5".to_string()); // 0-9 priority scale
    headers.insert("JMSDeliveryMode".to_string(), "PERSISTENT".to_string()); // PERSISTENT or NON_PERSISTENT
    headers.insert("JMSCorrelationID".to_string(), "basic-example-001".to_string());
    
    stomp_producer.send_queue_with_headers("default", 
        r#"{"type": "json", "message": "Hello with JMS headers!"}"#, 
        headers.clone()).await?;
    
    headers.insert("JMSPriority".to_string(), "3".to_string());
    headers.insert("JMSCorrelationID".to_string(), "api-request-12345".to_string());
    headers.insert("JMSType".to_string(), "api_request".to_string());
    stomp_producer.send_queue_with_headers("api_requests", 
        r#"{"api": "user_info", "id": 12345}"#, 
        headers.clone()).await?;
    
    // 4. Send messages to topics (reuses same connection)
    info!("📤 Sending messages to topics...");
    stomp_producer.send_topic("notifications", "Hello from basic example topic!").await?;
    stomp_producer.send_topic("notifications", "Another notification from basic").await?;
    
    // 4.1. Send messages to topics with custom headers
    info!("📤 Sending messages to topics with JMS standard headers...");
    let mut topic_headers = HashMap::new();
    topic_headers.insert("content-type".to_string(), "text/plain".to_string());
    topic_headers.insert("JMSPriority".to_string(), "9".to_string()); // High priority
    topic_headers.insert("JMSDeliveryMode".to_string(), "PERSISTENT".to_string());
    topic_headers.insert("JMSType".to_string(), "notification".to_string());
    topic_headers.insert("JMSTimestamp".to_string(), chrono::Utc::now().timestamp_millis().to_string());
    
    stomp_producer.send_topic_with_headers("notifications", 
        "Urgent notification with JMS headers and timestamp!", 
        topic_headers).await?;
    
    info!("📤 All messages sent successfully");
    
    // Wait for a while to see message processing
    info!("⏳ Waiting to see message processing...");
    tokio::time::sleep(Duration::from_secs(5)).await;
    
    // Demonstrate that we can also just send shutdown signal without waiting
    // (useful for fire-and-forget scenarios)
    info!("💡 Note: You can also use shutdown_handle.shutdown() for non-blocking shutdown");
    
    // Send a few more messages to demonstrate continuous operation
    info!("📤 Sending additional messages with mixed JMS header usage...");
    for i in 1..=3 {
        // Send some messages without headers
        stomp_producer.send_queue("default", &format!("Basic batch message {}", i)).await?;
        
        // Send some messages with JMS headers
        if i % 2 == 0 {
            let mut batch_headers = HashMap::new();
            batch_headers.insert("JMSCorrelationID".to_string(), format!("batch-{}", i));
            batch_headers.insert("JMSPriority".to_string(), "4".to_string()); // Normal priority
            batch_headers.insert("JMSDeliveryMode".to_string(), "NON_PERSISTENT".to_string());
            batch_headers.insert("sequence".to_string(), i.to_string()); // Custom header
            
            stomp_producer.send_queue_with_headers("default", 
                &format!("Batch message {} with JMS headers", i), 
                batch_headers).await?;
        }
        
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    
    // Optional: explicitly disconnect when done
    stomp_producer.disconnect().await?;
    info!("🔌 StompProducer disconnected");
    
    info!("🎉 Basic example completed successfully");
    
    // Setup graceful shutdown
    info!("⏳ Application running... Press Ctrl+C to shutdown gracefully");
    
    // Wait for shutdown signal and handle graceful shutdown
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("🛑 Ctrl+C received, initiating graceful shutdown...");
            
            // Use the new graceful shutdown functionality
            match shutdown_handle.shutdown_and_wait().await {
                Ok(()) => info!("✅ Service shut down gracefully"),
                Err(e) => info!("⚠️ Error during shutdown: {}", e),
            }
        }
    }
    
    info!("👋 Application shutdown complete");
    Ok(())
}
