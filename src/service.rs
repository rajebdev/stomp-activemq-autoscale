use anyhow::{Context, Result};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::time::{sleep, Duration};
use tracing::{debug, error, info, trace, warn};
use uuid::Uuid;

use stomp::connection::{Credentials, HeartBeat};
use stomp::header::Header;
use stomp::option_setter::OptionSetter;
use stomp::session::{Session, SessionEvent};
use stomp::session_builder::SessionBuilder;
use stomp::subscription::{AckMode, AckOrNack};

use crate::config::Config;
use crate::utils::{normalize_destination_name};

// Option setters for the session builder (following reference implementation)
struct WithHeader(Header);
struct WithHeartBeat(HeartBeat);
struct WithCredentials<'a>(Credentials<'a>);

impl OptionSetter<SessionBuilder> for WithHeader {
    fn set_option(self, mut builder: SessionBuilder) -> SessionBuilder {
        builder.config.headers.push(self.0);
        builder
    }
}

impl OptionSetter<SessionBuilder> for WithHeartBeat {
    fn set_option(self, mut builder: SessionBuilder) -> SessionBuilder {
        builder.config.heartbeat = self.0;
        builder
    }
}

impl<'a> OptionSetter<SessionBuilder> for WithCredentials<'a> {
    fn set_option(self, mut builder: SessionBuilder) -> SessionBuilder {
        builder.config.credentials = Some(stomp::connection::OwnedCredentials::from(self.0));
        builder
    }
}

/// Core STOMP client for handling message operations
pub struct StompClient {
    config: Config,
    session: Option<Session>,
    /// Tracks whether the connection is healthy
    is_connected: Arc<AtomicBool>,
    /// Shutdown signal for graceful cleanup
    shutdown_tx: Option<broadcast::Sender<()>>,
    shutdown_rx: Option<broadcast::Receiver<()>>,
    /// Track subscription destinations for reconnection
    active_subscriptions: Arc<std::sync::Mutex<Vec<String>>>,
}

impl StompClient {
    /// Create a new STOMP client instance
    pub async fn new(config: Config) -> Result<Self> {
        debug!("Initializing STOMP service: {}", config.service.name);

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        Ok(Self {
            config,
            session: None,
            is_connected: Arc::new(AtomicBool::new(false)),
            shutdown_tx: Some(shutdown_tx),
            shutdown_rx: Some(shutdown_rx),
            active_subscriptions: Arc::new(std::sync::Mutex::new(Vec::new())),
        })
    }

    /// Connect to the STOMP broker using the simplified pattern from reference
    pub async fn connect(&mut self) -> Result<()> {
        let (send_heartbeat, recv_heartbeat) = self.config.get_heartbeat_ms();
        debug!(
            "Connecting to STOMP broker at {}:{}",
            self.config.broker.host, self.config.broker.stomp_port
        );

        // Build session using the reference pattern with option setters
        let mut session_builder =
            SessionBuilder::new(&self.config.broker.host, self.config.broker.stomp_port)
                .with(WithHeartBeat(HeartBeat(send_heartbeat, recv_heartbeat)));

        // Add credentials if available
        if let Some((username, password)) = self.config.get_credentials() {
            session_builder =
                session_builder.with(WithCredentials(Credentials(&username, &password)));
            debug!("Using credentials for user: {}", username);
        }

        // Add client ID header
        let client_id = format!("{}-{}", self.config.service.name, Uuid::new_v4());
        session_builder =
            session_builder.with(WithHeader(Header::new("custom-client-id", &client_id)));

        let session = session_builder
            .start()
            .await
            .with_context(|| "Failed to establish STOMP connection")?;

        self.session = Some(session);
        self.is_connected.store(true, Ordering::Relaxed);
        info!("Connected to STOMP broker {}:{}", self.config.broker.host, self.config.broker.stomp_port);
        debug!("Connection established with client-id: {}", client_id);

        Ok(())
    }

    /// Send message to a topic with custom headers support
    pub async fn send_topic(
        &mut self,
        topic_name: &str,
        payload: &str,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        // Ensure we have an active session
        if self.session.is_none() {
            self.connect().await?;
        }

        let session = self
            .session
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("No active STOMP session"))?;

        // Build destination path - try config first, then fallback to direct path
        let destination = if let Some(topic_path) = self.config.get_topic_config(topic_name) {
            topic_path.clone()
        } else if topic_name.starts_with("/topic/") {
            topic_name.to_string()
        } else {
            format!("/topic/{}", topic_name)
        };

        debug!("Sending message to topic: {}", destination);

        // Normalize destination for Artemis to use logical address names
        let destination_for_stomp = match self.config.broker.broker_type {
            crate::config::BrokerType::ActiveMQ => destination.clone(),
            crate::config::BrokerType::Artemis => {
                // Strip STOMP prefix for Artemis to use logical address name
                normalize_destination_name(&destination).to_string()
            },
        };
        
        debug!("Using STOMP destination: {}", destination_for_stomp);

        // Send message using comprehensive header support API with fluent chaining
        let mut message_builder = session.message(&destination_for_stomp, payload)
            .with_content_type("text/plain"); // Set default content type

        // Add Artemis-specific routing type header for topics (multicast)
        if let crate::config::BrokerType::Artemis = self.config.broker.broker_type {
            message_builder = message_builder.add_header("destination-type", "MULTICAST");
            debug!("Added Artemis routing header: {}={} for topic", "destination-type", "MULTICAST");
        }

        // Add custom headers
        if !headers.is_empty() {
            for (key, value) in &headers {
                message_builder = message_builder.add_header(key, value);
                trace!("Adding custom topic header: {} = {}", key, value);
            }
        }

        // Send message and handle result with match
        match message_builder.send().await {
            Ok(_) => {
                debug!("Message sent successfully to topic: {}", destination);
                Ok(())
            }
            Err(e) => {
                error!("Failed to send message to topic {}: {}", destination, e);
                Err(anyhow::anyhow!("Failed to send message to topic: {}", e))
            }
        }
    }

    /// Send message to a queue with priority and persistence support
    pub async fn send_queue(
        &mut self,
        queue_name: &str,
        payload: &str,
        headers: HashMap<String, String>,
    ) -> Result<()> {
        // Ensure we have an active session
        if self.session.is_none() {
            self.connect().await?;
        }

        let session = self
            .session
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("No active STOMP session"))?;

        // Build destination path - try config first, then fallback to direct path
        let destination = if let Some(queue_path) = self.config.get_queue_config(queue_name) {
            queue_path.clone()
        } else if queue_name.starts_with("/queue/") {
            queue_name.to_string()
        } else {
            format!("/queue/{}", queue_name)
        };

        debug!("Sending message to queue: {}", destination);

        // Normalize destination for Artemis to use logical address names
        let destination_for_stomp = match self.config.broker.broker_type {
            crate::config::BrokerType::ActiveMQ => destination.clone(),
            crate::config::BrokerType::Artemis => {
                // Strip STOMP prefix for Artemis to use logical address name
                normalize_destination_name(&destination).to_string()
            },
        };
        
        debug!("Using STOMP destination: {}", destination_for_stomp);

        // Send message using comprehensive header support API with advanced features
        let mut message_builder = session.message(&destination_for_stomp, payload)
            .with_content_type("text/plain"); // Set default content type

        // Add Artemis-specific routing type header for queues (anycast)
        if let crate::config::BrokerType::Artemis = self.config.broker.broker_type {
            message_builder = message_builder.add_header("destination-type", "ANYCAST");
            debug!("Added Artemis routing header: {}={} for queue", "destination-type", "ANYCAST");
        }

        // Add custom headers with enhanced functionality
        if !headers.is_empty() {
            for (key, value) in &headers {
                message_builder = message_builder.add_header(key, value);
                trace!("Adding custom header: {} = {}", key, value);
            }
        }

        // Ready to send message with proper broker-compatible headers

        // Send message and handle result with match
        match message_builder.send().await {
            Ok(_) => {
                debug!("Message sent successfully to queue: {}", destination);
                Ok(())
            }
            Err(e) => {
                error!("Failed to send message to queue {}: {}", destination, e);
                Err(anyhow::anyhow!("Failed to send message to queue: {}", e))
            }
        }
    }

    /// Receive messages from a topic. Simple single-subscription implementation.
    /// Topics typically don't need scaling like queues, so we keep it simple.
    pub async fn receive_topic<F>(&mut self, topic_name: &str, handler: F) -> Result<()>
    where
        F: Fn(String) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        // Try to get topic configuration, fallback to direct path
        let topic_path = if let Some(topic_path_config) = self.config.get_topic_config(topic_name) {
            topic_path_config.clone()
        } else if topic_name.starts_with("/topic/") {
            topic_name.to_string()
        } else {
            format!("/topic/{}", topic_name)
        };

        debug!("Starting topic worker for: {}", topic_path);

        // Main reconnection loop
        loop {
            // Check if we need to connect/reconnect
            if !self.is_healthy() {
                match self.reconnect().await {
                    Ok(()) => {
                        debug!("Successfully connected/reconnected to topic: {}", topic_path);
                    }
                    Err(e) => {
                        error!("Failed to connect/reconnect to topic {}: {}", topic_path, e);
                        return Err(e);
                    }
                }
            }

            // Ensure we have a session
            if self.session.is_none() {
                self.connect().await?;
            }

            let mut session = self
                .session
                .take()
                .ok_or_else(|| anyhow::anyhow!("No active STOMP session"))?;

            // Add this destination to active subscriptions for reconnection
            {
                let mut subs = self.active_subscriptions.lock().unwrap();
                if !subs.contains(&topic_path) {
                    subs.push(topic_path.clone());
                }
            }

            let mut connected = false;
            let mut should_reconnect = false;

            while let Some(event) = session.next_event().await {
                match event {
                    SessionEvent::Connected => {
                        debug!("Connected to STOMP broker for topic subscription");

                        // Normalize destination for Artemis to use logical address names
                        let destination_for_stomp = match self.config.broker.broker_type {
                            crate::config::BrokerType::ActiveMQ => topic_path.clone(),
                            crate::config::BrokerType::Artemis => {
                                // Strip STOMP prefix for Artemis to use logical address name
                                normalize_destination_name(&topic_path).to_string()
                            },
                        };

                        let mut session_builder = session
                            .subscription(&destination_for_stomp)
                            .with(AckMode::ClientIndividual);

                        if self.config.broker.broker_type == crate::config::BrokerType::Artemis {
                            session_builder = session_builder.add_header("subscription-type", "MULTICAST");
                            debug!("Added Artemis subscription header: {}={}", "subscription-type", "MULTICAST");
                        }

                        let subscription_id = session_builder
                            .start()
                            .await?;

                        info!("Subscribed to topic: {} (ID: {})", topic_path, subscription_id);
                        connected = true;
                    }

                    SessionEvent::Message {
                        destination, frame, ..
                    } => {
                        if !connected {
                            continue;
                        }

                        let body_str = std::str::from_utf8(&frame.body)
                            .unwrap_or("<non-UTF8>")
                            .to_string();

                        let message_id = frame
                            .headers
                            .headers
                            .iter()
                            .find(|h| h.0 == "message-id")
                            .map(|h| h.1.as_str())
                            .unwrap_or("unknown");

                        debug!(
                            "Received topic message from {}: {} (ID: {})",
                            destination,
                            body_str.chars().take(50).collect::<String>(),
                            message_id
                        );

                        // Process message with handler
                        match handler(body_str).await {
                            Ok(()) => {
                                trace!("✅ Topic message processed successfully");

                                // Acknowledge the message
                                if let Err(e) =
                                    session.acknowledge_frame(&frame, AckOrNack::Ack).await
                                {
                                    error!("❌ Failed to acknowledge topic message: {}", e);
                                } else {
                                    trace!("✓ Topic message acknowledged");
                                }
                            }
                            Err(e) => {
                                error!("Topic message processing failed: {}", e);

                                // Negative acknowledge (NACK) the message
                                if let Err(nack_err) =
                                    session.acknowledge_frame(&frame, AckOrNack::Nack).await
                                {
                                    error!("Failed to NACK topic message: {}", nack_err);
                                }
                            }
                        }
                    }

                    SessionEvent::Receipt { id, .. } => {
                        debug!("📄 Received receipt: {}", id);
                    }

                    SessionEvent::ErrorFrame(frame) => {
                        error!("[{}] Error frame received: {:?}", topic_path, frame);
                        self.mark_unhealthy();
                        should_reconnect = true;
                        break;
                    }

                    SessionEvent::Disconnected(reason) => {
                        warn!("[{}] Session disconnected: {:?}", topic_path, reason);
                        self.mark_unhealthy();
                        should_reconnect = true;
                        break;
                    }

                    _ => {
                        // Ignore other events
                    }
                }
            }

            // Store the session back if we're not reconnecting
            if !should_reconnect {
                self.session = Some(session);
                break;
            } else {
                debug!("Connection lost for topic {}, will attempt reconnection...", topic_path);
                // Session will be recreated on next iteration
                sleep(Duration::from_millis(1000)).await; // Brief pause before reconnecting
            }
        }

        Ok(())
    }

    /// Receive messages from a queue. Simple single-subscription implementation.
    /// Scaling is managed by ConsumerPool, so this always creates exactly one subscription.
    pub async fn receive_queue<F>(&mut self, queue_name: &str, handler: F) -> Result<()>
    where
        F: Fn(String) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        // Try to get queue configuration, fallback to direct path
        let queue_path = if let Some(queue_path_config) = self.config.get_queue_config(queue_name) {
            queue_path_config.clone()
        } else if queue_name.starts_with("/queue/") {
            queue_name.to_string()
        } else {
            format!("/queue/{}", queue_name)
        };

        debug!("Starting queue worker for: {}", queue_path);

        // Main reconnection loop
        loop {
            // Check if we need to connect/reconnect
            if !self.is_healthy() {
                match self.reconnect().await {
                    Ok(()) => {
                        debug!("Successfully connected/reconnected to queue: {}", queue_path);
                    }
                    Err(e) => {
                        error!("Failed to connect/reconnect to queue {}: {}", queue_path, e);
                        return Err(e);
                    }
                }
            }

            // Ensure we have a session
            if self.session.is_none() {
                self.connect().await?;
            }

            let mut session = self
                .session
                .take()
                .ok_or_else(|| anyhow::anyhow!("No active STOMP session"))?;

            // Add this destination to active subscriptions for reconnection
            {
                let mut subs = self.active_subscriptions.lock().unwrap();
                if !subs.contains(&queue_path) {
                    subs.push(queue_path.clone());
                }
            }

            let mut connected = false;
            let mut should_reconnect = false;

            while let Some(event) = session.next_event().await {
                match event {
                    SessionEvent::Connected => {
                        debug!("Connected to STOMP broker for queue subscription");

                        // Normalize destination for Artemis to use logical address names
                        let destination_for_stomp = match self.config.broker.broker_type {
                            crate::config::BrokerType::ActiveMQ => queue_path.clone(),
                            crate::config::BrokerType::Artemis => {
                                // Strip STOMP prefix for Artemis to use logical address name
                                normalize_destination_name(&queue_path).to_string()
                            },
                        };

                        let mut session_builder = session
                            .subscription(&destination_for_stomp)
                            .with(AckMode::ClientIndividual);
                        
                        if self.config.broker.broker_type == crate::config::BrokerType::Artemis {
                            session_builder = session_builder.add_header("subscription-type", "ANYCAST");
                            debug!("Added Artemis subscription header: {}={}", "subscription-type", "ANYCAST");
                        }

                        let subscription_id = session_builder
                            .start()
                            .await?;

                        info!("Subscribed to queue: {} (ID: {})", queue_path, subscription_id);
                        connected = true;
                    }

                    SessionEvent::Message {
                        destination, frame, ..
                    } => {
                        if !connected {
                            continue;
                        }

                        let body_str = std::str::from_utf8(&frame.body)
                            .unwrap_or("<non-UTF8>")
                            .to_string();

                        let message_id = frame
                            .headers
                            .headers
                            .iter()
                            .find(|h| h.0 == "message-id")
                            .map(|h| h.1.as_str())
                            .unwrap_or("unknown");

                        debug!(
                            "Received queue message from {}: {} (ID: {})",
                            destination,
                            body_str.chars().take(50).collect::<String>(),
                            message_id
                        );

                        // Process message with handler
                        match handler(body_str).await {
                            Ok(()) => {
                                trace!("✅ Queue message processed successfully");

                                // Acknowledge the message
                                if let Err(e) =
                                    session.acknowledge_frame(&frame, AckOrNack::Ack).await
                                {
                                    error!("❌ Failed to acknowledge queue message: {}", e);
                                } else {
                                    trace!("✓ Queue message acknowledged");
                                }
                            }
                            Err(e) => {
                                error!("Queue message processing failed: {}", e);

                                // Negative acknowledge (NACK) the message
                                if let Err(nack_err) =
                                    session.acknowledge_frame(&frame, AckOrNack::Nack).await
                                {
                                    error!("Failed to NACK queue message: {}", nack_err);
                                }
                            }
                        }
                    }

                    SessionEvent::Receipt { id, .. } => {
                        debug!("📄 Received receipt: {}", id);
                    }

                    SessionEvent::ErrorFrame(frame) => {
                        error!("[{}] Error frame received: {:?}", queue_path, frame);
                        self.mark_unhealthy();
                        should_reconnect = true;
                        break;
                    }

                    SessionEvent::Disconnected(reason) => {
                        warn!("[{}] Session disconnected: {:?}", queue_path, reason);
                        self.mark_unhealthy();
                        should_reconnect = true;
                        break;
                    }

                    _ => {
                        // Ignore other events
                    }
                }
            }

            // Store the session back if we're not reconnecting
            if !should_reconnect {
                self.session = Some(session);
                break;
            } else {
                debug!("Connection lost for queue {}, will attempt reconnection...", queue_path);
                // Session will be recreated on next iteration
                sleep(Duration::from_millis(1000)).await; // Brief pause before reconnecting
            }
        }

        Ok(())
    }

    /// Disconnect from the STOMP broker
    pub async fn disconnect(&mut self) -> Result<()> {
        if let Some(mut session) = self.session.take() {
            debug!("Disconnecting from STOMP broker");

            if let Err(e) = session.disconnect().await {
                warn!("Disconnect error: {}", e);
            } else {
                debug!("Disconnected gracefully from STOMP broker");
            }
        }

        // Mark as disconnected
        self.is_connected.store(false, Ordering::Relaxed);

        // Clear active subscriptions
        {
            let mut subs = self.active_subscriptions.lock().unwrap();
            subs.clear();
        }

        // Send shutdown signal if we have a sender
        if let Some(ref tx) = self.shutdown_tx {
            let _ = tx.send(());
        }

        Ok(())
    }

    /// Check if service is connected
    pub fn is_connected(&self) -> bool {
        self.session.is_some() && self.is_connected.load(Ordering::Relaxed)
    }

    /// Check if connection is healthy (alias for is_connected for clarity)
    pub fn is_healthy(&self) -> bool {
        self.is_connected()
    }

    /// Mark connection as unhealthy
    fn mark_unhealthy(&self) {
        self.is_connected.store(false, Ordering::Relaxed);
        warn!("Connection marked as unhealthy");
    }

    /// Determine if an error is temporary and retryable
    fn is_temporary_error(error: &anyhow::Error) -> bool {
        let error_str = error.to_string().to_lowercase();

        // Check for common temporary/network errors
        error_str.contains("connection refused")
            || error_str.contains("connection reset")
            || error_str.contains("timeout")
            || error_str.contains("network")
            || error_str.contains("broken pipe")
            || error_str.contains("connection aborted")
            || error_str.contains("host unreachable")
            || error_str.contains("no route to host")
    }

    /// Attempt to reconnect with exponential backoff
    pub async fn reconnect(&mut self) -> Result<()> {
        info!("🔄 Starting reconnection process...");

        let retry_config = self.config.retry.clone();
        let mut attempt = 0u32;

        // Mark as disconnected
        self.mark_unhealthy();
        self.session = None;

        while retry_config.should_retry(attempt) {
            // Check for shutdown signal
            if let Some(ref mut rx) = self.shutdown_rx {
                if rx.try_recv().is_ok() {
                    info!("🚨 Shutdown signal received, stopping reconnection");
                    return Err(anyhow::anyhow!("Shutdown requested during reconnection"));
                }
            }

            let delay = retry_config.calculate_delay(attempt);
            if retry_config.max_attempts < 0 {
                debug!(
                    "🔁 Reconnection attempt {} (infinite retries) in {}ms",
                    attempt + 1,
                    delay.as_millis()
                );
            } else {
                debug!(
                    "🔁 Reconnection attempt {} of {} in {}ms",
                    attempt + 1,
                    retry_config.max_attempts,
                    delay.as_millis()
                );
            }

            // Wait before attempting reconnection (except for first attempt)
            if attempt > 0 {
                sleep(delay).await;
            }

            // Attempt to connect
            match self.connect().await {
                Ok(()) => {
                    info!("✅ Successfully reconnected to STOMP broker");

                    // Resubscribe to previously active subscriptions
                    if let Err(e) = self.resubscribe_destinations().await {
                        warn!("⚠️ Failed to resubscribe to destinations: {}", e);
                        // Continue anyway, as basic connection is established
                    }

                    return Ok(());
                }
                Err(e) => {
                    if Self::is_temporary_error(&e) {
                        warn!(
                            "⚠️ Reconnection attempt {} failed (retryable): {}",
                            attempt + 1,
                            e
                        );
                    } else {
                        error!(
                            "❌ Reconnection attempt {} failed (non-retryable): {}",
                            attempt + 1,
                            e
                        );
                        return Err(e);
                    }
                }
            }

            attempt += 1;
        }

        let final_error = if retry_config.max_attempts < 0 {
            anyhow::anyhow!("Failed to reconnect (this should not happen with infinite retries)")
        } else {
            anyhow::anyhow!(
                "Failed to reconnect after {} attempts",
                retry_config.max_attempts
            )
        };
        Err(final_error)
    }

    /// Resubscribe to previously active destinations after reconnection
    async fn resubscribe_destinations(&mut self) -> Result<()> {
        let subscriptions = {
            let subs = self.active_subscriptions.lock().unwrap();
            subs.clone()
        };

        if subscriptions.is_empty() {
            debug!("📏 No previous subscriptions to restore");
            return Ok(());
        }

        info!("🔄 Resubscribing to {} destinations", subscriptions.len());

        for destination in subscriptions {
            debug!("🔄 Resubscribing to: {}", destination);
            // Note: The actual resubscription logic will be handled by the
            // individual receive_topic/receive_queue methods when they detect
            // a reconnection has occurred
        }

        Ok(())
    }

    /// Get service configuration
    pub fn get_config(&self) -> &Config {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, RetryConfig};

    #[test]
    fn test_is_temporary_error() {
        let temp_errors = vec![
            anyhow::anyhow!("Connection refused"),
            anyhow::anyhow!("Connection reset by peer"),
            anyhow::anyhow!("Network timeout occurred"),
            anyhow::anyhow!("Broken pipe error"),
            anyhow::anyhow!("Host unreachable"),
        ];

        for error in temp_errors {
            assert!(
                StompClient::is_temporary_error(&error),
                "Should be temporary: {}",
                error
            );
        }

        let non_temp_errors = vec![
            anyhow::anyhow!("Invalid credentials"),
            anyhow::anyhow!("Authentication failed"),
            anyhow::anyhow!("Permission denied"),
            anyhow::anyhow!("Configuration error"),
        ];

        for error in non_temp_errors {
            assert!(
                !StompClient::is_temporary_error(&error),
                "Should not be temporary: {}",
                error
            );
        }
    }

    #[test]
    fn test_retry_config_calculations() {
        let retry_config = RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 100,
            max_delay_ms: 1000,
            backoff_multiplier: 2.0,
        };

        // Test delay calculations
        let delay_0 = retry_config.calculate_delay(0);
        assert_eq!(delay_0.as_millis(), 100);

        let delay_1 = retry_config.calculate_delay(1);
        assert_eq!(delay_1.as_millis(), 200); // 100 * 2^1

        let delay_2 = retry_config.calculate_delay(2);
        assert_eq!(delay_2.as_millis(), 400); // 100 * 2^2

        // Test should_retry logic with finite attempts
        assert!(retry_config.should_retry(0));
        assert!(retry_config.should_retry(1));
        assert!(retry_config.should_retry(2));
        assert!(!retry_config.should_retry(3)); // Should not retry after max_attempts

        // Test infinite retry logic
        let infinite_retry_config = RetryConfig {
            max_attempts: -1, // -1 means infinite retries
            initial_delay_ms: 100,
            max_delay_ms: 1000,
            backoff_multiplier: 2.0,
        };

        assert!(infinite_retry_config.should_retry(0));
        assert!(infinite_retry_config.should_retry(100));
        assert!(infinite_retry_config.should_retry(1000000)); // Should always retry with infinite retries
        assert!(infinite_retry_config.max_attempts == -1);
    }

    #[test]
    fn test_retry_config_delay_capping() {
        let retry_config = RetryConfig {
            max_attempts: 10,
            initial_delay_ms: 100,
            max_delay_ms: 500, // Cap at 500ms
            backoff_multiplier: 2.0,
        };

        // Test that delay is capped at max_delay_ms
        let delay_5 = retry_config.calculate_delay(5);
        // Without capping: 100 * 2^5 = 3200ms, but should be capped at 500ms
        assert_eq!(delay_5.as_millis(), 500);
    }

    #[tokio::test]
    async fn test_stomp_client_creation() {
        let config = create_test_config();
        let client = StompClient::new(config).await;
        assert!(client.is_ok());

        let client = client.unwrap();
        assert!(!client.is_connected());
        assert!(!client.is_healthy());
    }

    fn create_test_config() -> Config {
        use crate::config::*;
        use std::collections::HashMap;

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
            retry: RetryConfig {
                max_attempts: 3,
                initial_delay_ms: 100,
                max_delay_ms: 1000,
                backoff_multiplier: 2.0,
            },
        }
    }

    fn create_artemis_test_config() -> Config {
        use crate::config::*;
        use std::collections::HashMap;

        Config {
            service: ServiceConfig {
                name: "test-artemis-service".to_string(),
                version: "1.0.0".to_string(),
                description: "Test Artemis service".to_string(),
            },
            broker: BrokerConfig {
                broker_type: BrokerType::Artemis,
                host: "localhost".to_string(),
                stomp_port: 61613,
                web_port: 8161,
                username: "admin".to_string(),
                password: "admin".to_string(),
                heartbeat_secs: 30,
                broker_name: "broker".to_string(), // Default Artemis broker name
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
            retry: RetryConfig {
                max_attempts: 3,
                initial_delay_ms: 100,
                max_delay_ms: 1000,
                backoff_multiplier: 2.0,
            },
        }
    }

    #[tokio::test]
    async fn test_artemis_stomp_client_creation() {
        let config = create_artemis_test_config();
        let client = StompClient::new(config).await;
        assert!(client.is_ok());

        let client = client.unwrap();
        assert!(!client.is_connected());
        assert!(!client.is_healthy());
        assert_eq!(client.config.broker.broker_type, crate::config::BrokerType::Artemis);
        assert_eq!(client.config.broker.broker_name, "broker");
    }
}
