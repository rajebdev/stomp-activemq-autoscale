use anyhow::Result;
use crate::config::Config;
use crate::service::StompService;
use tracing::{debug, error, info, warn};
use tokio::signal;
use tokio::time::{sleep, Duration};
use std::collections::HashMap;

/// Initialize structured logging with environment filter support
pub fn initialize_logging() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .with_target(false)
        .with_thread_ids(false)
        .with_line_number(false)
        .init();
}

/// Load configuration from YAML file
pub fn load_configuration(config_path: &str) -> Result<Config> {
    Config::load(config_path)
}

/// Display startup information
pub fn display_startup_info(config: &Config) {
    info!(
        "🚀 Starting Auto-Scaling STOMP Application: {}",
        config.service.name
    );
    info!("📋 Version: {}", config.service.version);
    info!("📄 Description: {}", config.service.description);
    info!("🔗 Broker: {}:{}", config.broker.host, config.broker.stomp_port);
}

/// Setup signal handlers for graceful shutdown
pub async fn setup_signal_handlers() {
    #[cfg(unix)]
    {
        use signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
        let mut sigint =
            signal(SignalKind::interrupt()).expect("Failed to register SIGINT handler");

        tokio::select! {
            _ = sigterm.recv() => {
                info!("📡 Received SIGTERM - initiating graceful shutdown");
            }
            _ = sigint.recv() => {
                info!("📡 Received SIGINT (Ctrl+C) - initiating graceful shutdown");
            }
        }
    }

    #[cfg(windows)]
    {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("📡 Received Ctrl+C - initiating graceful shutdown");
            }
            Err(err) => {
                error!("Unable to listen for shutdown signal: {}", err);
            }
        }
    }
}

/// Send test messages to queues and topics for demonstration
pub async fn send_test_messages(config: &Config) {
    // Wait a bit before starting message sending
    sleep(Duration::from_secs(2)).await;

    debug!("📤 Sending test messages...");

    // Create STOMP service for sending messages
    if let Ok(mut stomp_service) = StompService::new(config.clone()).await {
        // Send to topics
        let mut topic_headers = HashMap::new();
        topic_headers.insert("content-type".to_string(), "text/plain".to_string());
        topic_headers.insert("priority".to_string(), "high".to_string());

        if let Err(e) = stomp_service
            .send_topic(
                "notifications",
                "Test topic message for auto-scaling system",
                topic_headers,
            )
            .await
        {
            warn!("Failed to send topic message: {}", e);
        }

        // Send to queues
        let mut queue_headers = HashMap::new();
        queue_headers.insert("content-type".to_string(), "text/plain".to_string());
        queue_headers.insert("persistent".to_string(), "true".to_string());

        // Send multiple messages to test auto-scaling
        for i in 1..=5 {
            let message = format!("Test queue message #{} for auto-scaling", i);
            
            // Send to 'default' queue (maps to /queue/demo)
            if let Err(e) = stomp_service
                .send_queue("default", &message, queue_headers.clone())
                .await
            {
                warn!("Failed to send default queue message {}: {}", i, e);
            }
            
            // Send to 'api_requests' queue
            if let Err(e) = stomp_service
                .send_queue("api_requests", &message, queue_headers.clone())
                .await
            {
                warn!("Failed to send api_requests queue message {}: {}", i, e);
            }
            
            // Small delay between messages
            sleep(Duration::from_millis(100)).await;
        }

        debug!("✅ Test messages sent successfully");
        
        // Clean disconnect
        if let Err(e) = stomp_service.disconnect().await {
            warn!("Test message sender disconnect error: {}", e);
        }
    } else {
        warn!("Failed to create STOMP service for sending test messages");
    }
}

/// Helper function to normalize destination names by stripping STOMP prefixes
/// This is used for Artemis compatibility where logical address names are preferred
pub fn normalize_destination_name(destination: &str) -> &str {
    destination
        .strip_prefix("/queue/")
        .or_else(|| destination.strip_prefix("/topic/"))
        .unwrap_or(destination)
}

/// Helper function to check if destination is a queue
pub fn is_queue_destination(destination: &str) -> bool {
    destination.starts_with("/queue/")
}

/// Helper function to determine routing types priority based on destination type
/// For Artemis: queues prefer anycast (point-to-point), topics prefer multicast (publish-subscribe)
pub fn determine_routing_types(destination: &str) -> [&'static str; 2] {
    if is_queue_destination(destination) {
        ["anycast", "multicast"]  // Try anycast first for queues (point-to-point)
    } else {
        ["multicast", "anycast"]  // Try multicast first for topics (publish-subscribe)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_destination_name() {
        // Test queue prefix stripping
        assert_eq!(normalize_destination_name("/queue/demo"), "demo");
        assert_eq!(normalize_destination_name("/queue/orders"), "orders");
        
        // Test topic prefix stripping
        assert_eq!(normalize_destination_name("/topic/events"), "events");
        assert_eq!(normalize_destination_name("/topic/alerts"), "alerts");
        
        // Test no prefix (should return as-is)
        assert_eq!(normalize_destination_name("demo"), "demo");
        assert_eq!(normalize_destination_name("events"), "events");
        
        // Test edge cases
        assert_eq!(normalize_destination_name("/queue/"), "");
        assert_eq!(normalize_destination_name("/topic/"), "");
        assert_eq!(normalize_destination_name(""), "");
    }

    #[test]
    fn test_is_queue_destination() {
        // Test queue destinations
        assert!(is_queue_destination("/queue/demo"));
        assert!(is_queue_destination("/queue/orders"));
        assert!(is_queue_destination("/queue/"));
        
        // Test non-queue destinations
        assert!(!is_queue_destination("/topic/events"));
        assert!(!is_queue_destination("demo"));
        assert!(!is_queue_destination(""));
        assert!(!is_queue_destination("/other/path"));
    }

    #[test]
    fn test_determine_routing_types() {
        // Test queue destinations (should prefer anycast)
        let queue_types = determine_routing_types("/queue/demo");
        assert_eq!(queue_types[0], "anycast");
        assert_eq!(queue_types[1], "multicast");
        
        // Test topic destinations (should prefer multicast)
        let topic_types = determine_routing_types("/topic/events");
        assert_eq!(topic_types[0], "multicast");
        assert_eq!(topic_types[1], "anycast");
        
        // Test non-standard destinations (should default to topic behavior)
        let default_types = determine_routing_types("demo");
        assert_eq!(default_types[0], "multicast");
        assert_eq!(default_types[1], "anycast");
    }

    #[test]
    fn test_destination_helper_integration() {
        // Test full workflow for queue
        let queue_dest = "/queue/orders";
        assert!(is_queue_destination(queue_dest));
        assert_eq!(normalize_destination_name(queue_dest), "orders");
        let routing = determine_routing_types(queue_dest);
        assert_eq!(routing[0], "anycast");
        
        // Test full workflow for topic
        let topic_dest = "/topic/notifications";
        assert!(!is_queue_destination(topic_dest));
        assert_eq!(normalize_destination_name(topic_dest), "notifications");
        let routing = determine_routing_types(topic_dest);
        assert_eq!(routing[0], "multicast");
    }

    #[test]
    fn test_artemis_constants() {
        // Test that basic destination helper functions work correctly
        assert!(is_queue_destination("/queue/test"));
        assert!(!is_queue_destination("/topic/test"));
        assert_eq!(normalize_destination_name("/queue/test"), "test");
        assert_eq!(normalize_destination_name("/topic/test"), "test");
    }
}