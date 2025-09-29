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
    info!("🔗 Broker: {}:{}", config.activemq.host, config.activemq.stomp_port);
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