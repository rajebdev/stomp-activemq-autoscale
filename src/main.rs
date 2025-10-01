use anyhow::Result;
use stomp_activemq_autoscale::service::StompService;
use stomp_activemq_autoscale::utils;
use tracing::info;
use std::collections::HashMap;

// Custom handler for processing notification messages
async fn handle_notification_message(message: String) -> Result<()> {
    info!("🔔 Processing NOTIFICATION: {}", message);
    // Simulate some processing time
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    Ok(())
}

// Custom handler for processing API request messages
async fn handle_api_request_message(message: String) -> Result<()> {
    info!("🌐 Processing API REQUEST: {}", message);
    // Simulate API processing
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    Ok(())
}

// Custom handler for processing general messages
async fn handle_general_message(message: String) -> Result<()> {
    info!("🛒 Processing GENERAL: {}", message);
    // Simulate some processing time
    tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging first
    utils::initialize_logging();
    
    // Load configuration
    let config = utils::load_configuration("config.yaml")?;
    
    // Display startup information
    utils::display_startup_info(&config);
    
    // Clone config for sending test messages
    let config_for_test = config.clone();
    
    // Start sending test messages in background
    tokio::spawn(async move {
        utils::send_test_messages(&config_for_test).await;
    });
    
    // Example 1: Use configuration with custom handlers
    // Note: Whether queues use auto-scaling or static workers is determined by config.yaml
    let mut stomp_service = StompService::new(config).await?;

    stomp_service.connect().await?;
    stomp_service.send_queue("test_queue", "Test message from main", HashMap::new()).await?;

    stomp_service
        .add_queue("default", handle_general_message)  // Handler for default queue
        .add_queue("errorsx", handle_general_message)  // Handler for errorsx queue
        .add_queue("api_requests", handle_api_request_message)  // Handler for api_requests queue
        .add_topic("notifications", handle_notification_message)  // Handler for notifications topic
        .run()
        .await
    
    // stomp_service.send_queue("test_queue", "Test message from main", HashMap::new()).await

    // Alternative examples (commented out):
    
    // Example 2: Simple setup using config file directly in main
    /*
    let config = utils::load_configuration("config.yaml")?;
    StompService::new(config).await?
        .run()
        .await
    */
    
    // Example 3: Custom configuration with multiple handlers
    /*
    // Send test messages in background if needed
    let config_for_test = utils::load_configuration("config.yaml")?;
    tokio::spawn(async move {
        utils::send_test_messages(&config_for_test).await;
    });
    
    let config = utils::load_configuration("config.yaml")?;
    StompService::new(config).await?
        .add_queue("user_events", |msg| async move {
            debug!("👤 User event: {}", msg);
            Ok(())
        })
        .add_queue("system_logs", |msg| async move {
            debug!("📋 System log: {}", msg);
            Ok(())
        })
        .add_queue("high_load_queue", |msg| async move {
            debug!("⚡ High load processing: {}", msg);
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            Ok(())
        })
        .run()
        .await
    */
}

