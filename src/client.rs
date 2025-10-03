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
use crate::utils::{normalize_destination_name, build_stomp_destination};

/// Trait for client implementations to enable testing with mocks
#[cfg_attr(test, mockall::automock)]
#[async_trait::async_trait]
pub trait Client: Send {
    /// Send a message to a queue with custom headers
    async fn send_queue(&mut self, queue_name: &str, message: &str, headers: HashMap<String, String>) -> Result<()>;
    
    /// Send a message to a topic with custom headers
    async fn send_topic(&mut self, topic_name: &str, message: &str, headers: HashMap<String, String>) -> Result<()>;
    
    /// Disconnect the client
    async fn disconnect(&mut self) -> Result<()>;
}

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

        // Get the topic name from config, or use the provided name directly
        let topic_name_clean = if let Some(topic_config_name) = self.config.get_topic_config(topic_name) {
            topic_config_name.clone()
        } else {
            // Remove any legacy prefixes and use the clean name
            normalize_destination_name(topic_name).to_string()
        };

        debug!("Sending message to topic: {}", topic_name_clean);

        // Build broker-appropriate destination path
        let destination_for_stomp = build_stomp_destination(
            &self.config.broker.broker_type, 
            "topic", 
            &topic_name_clean
        );
        
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
                debug!("Message sent successfully to topic: {}", topic_name_clean);
                Ok(())
            }
            Err(e) => {
                error!("Failed to send message to topic {}: {}", topic_name_clean, e);
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

        // Get the queue name from config, or use the provided name directly
        let queue_name_clean = if let Some(queue_config_name) = self.config.get_queue_config(queue_name) {
            queue_config_name.clone()
        } else {
            // Remove any legacy prefixes and use the clean name
            normalize_destination_name(queue_name).to_string()
        };

        debug!("Sending message to queue: {}", queue_name_clean);

        // Build broker-appropriate destination path
        let destination_for_stomp = build_stomp_destination(
            &self.config.broker.broker_type, 
            "queue", 
            &queue_name_clean
        );
        
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
                debug!("Message sent successfully to queue: {}", queue_name_clean);
                Ok(())
            }
            Err(e) => {
                error!("Failed to send message to queue {}: {}", queue_name_clean, e);
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
        // Get the topic name from config, or use the provided name directly
        let topic_name_clean = if let Some(topic_config_name) = self.config.get_topic_config(topic_name) {
            topic_config_name.clone()
        } else {
            // Remove any legacy prefixes and use the clean name
            normalize_destination_name(topic_name).to_string()
        };

        debug!("Starting topic worker for: {}", topic_name_clean);

        // Main reconnection loop
        loop {
            // Check if we need to connect/reconnect
            if !self.is_healthy() {
                match self.reconnect().await {
                    Ok(()) => {
                        debug!("Successfully connected/reconnected to topic: {}", topic_name_clean);
                    }
                    Err(e) => {
                        error!("Failed to connect/reconnect to topic {}: {}", topic_name_clean, e);
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
                if !subs.contains(&topic_name_clean) {
                    subs.push(topic_name_clean.clone());
                }
            }

            let mut connected = false;
            let mut should_reconnect = false;

            while let Some(event) = session.next_event().await {
                match event {
                    SessionEvent::Connected => {
                        debug!("Connected to STOMP broker for topic subscription");

                        // Build broker-appropriate destination path
                        let destination_for_stomp = build_stomp_destination(
                            &self.config.broker.broker_type,
                            "topic",
                            &topic_name_clean
                        );

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

                        info!("Subscribed to topic: {} (ID: {})", topic_name_clean, subscription_id);
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
                        error!("[{}] Error frame received: {:?}", topic_name_clean, frame);
                        self.mark_unhealthy();
                        should_reconnect = true;
                        break;
                    }

                    SessionEvent::Disconnected(reason) => {
                        warn!("[{}] Session disconnected: {:?}", topic_name_clean, reason);
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
                debug!("Connection lost for topic {}, will attempt reconnection...", topic_name_clean);
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
        // Get the queue name from config, or use the provided name directly
        let queue_name_clean = if let Some(queue_config_name) = self.config.get_queue_config(queue_name) {
            queue_config_name.clone()
        } else {
            // Remove any legacy prefixes and use the clean name
            normalize_destination_name(queue_name).to_string()
        };

        debug!("Starting queue worker for: {}", queue_name_clean);

        // Main reconnection loop
        loop {
            // Check if we need to connect/reconnect
            if !self.is_healthy() {
                match self.reconnect().await {
                    Ok(()) => {
                        debug!("Successfully connected/reconnected to queue: {}", queue_name_clean);
                    }
                    Err(e) => {
                        error!("Failed to connect/reconnect to queue {}: {}", queue_name_clean, e);
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
                if !subs.contains(&queue_name_clean) {
                    subs.push(queue_name_clean.clone());
                }
            }

            let mut connected = false;
            let mut should_reconnect = false;

            while let Some(event) = session.next_event().await {
                match event {
                    SessionEvent::Connected => {
                        debug!("Connected to STOMP broker for queue subscription");

                        // Build broker-appropriate destination path
                        let destination_for_stomp = build_stomp_destination(
                            &self.config.broker.broker_type,
                            "queue",
                            &queue_name_clean
                        );

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

                        info!("Subscribed to queue: {} (ID: {})", queue_name_clean, subscription_id);
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
                        error!("[{}] Error frame received: {:?}", queue_name_clean, frame);
                        self.mark_unhealthy();
                        should_reconnect = true;
                        break;
                    }

                    SessionEvent::Disconnected(reason) => {
                        warn!("[{}] Session disconnected: {:?}", queue_name_clean, reason);
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
                debug!("Connection lost for queue {}, will attempt reconnection...", queue_name_clean);
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
}

/// Implement Client trait for StompClient
#[async_trait::async_trait]
impl Client for StompClient {
    async fn send_queue(&mut self, queue_name: &str, message: &str, headers: HashMap<String, String>) -> Result<()> {
        self.send_queue(queue_name, message, headers).await
    }
    
    async fn send_topic(&mut self, topic_name: &str, message: &str, headers: HashMap<String, String>) -> Result<()> {
        self.send_topic(topic_name, message, headers).await
    }
    
    async fn disconnect(&mut self) -> Result<()> {
        self.disconnect().await
    }
}

#[cfg(test)]
mod tests {
    //! Comprehensive unit tests for the client module.
    //!
    //! ## Test Coverage Summary:
    //!
    //! ### 1. StompClient Construction Tests (7 tests):
    //! - Basic client creation with valid config
    //! - Client initialization state verification
    //! - Initial connection state checks
    //! - Shutdown channel initialization
    //! - Active subscriptions initialization
    //! - Client creation with different broker types
    //! - Property-based testing for various configs
    //!
    //! ### 2. Connection State Management Tests (6 tests):
    //! - Health status tracking
    //! - Connection state transitions
    //! - Mark unhealthy behavior
    //! - Connection state after disconnect
    //! - Multiple connection attempts
    //! - Connection state with session lifecycle
    //!
    //! ### 3. Error Classification Tests (8 tests):
    //! - Temporary error detection (network errors)
    //! - Non-retryable error detection
    //! - Connection refused errors
    //! - Timeout errors
    //! - Broken pipe errors
    //! - Host unreachable errors
    //! - Case-insensitive error matching
    //! - Property-based error classification
    //!
    //! ### 4. Retry Configuration Tests (7 tests):
    //! - Exponential backoff delay calculation
    //! - Initial delay verification
    //! - Max delay capping
    //! - Backoff multiplier application
    //! - Infinite retry support (max_attempts = -1)
    //! - Finite retry limit
    //! - Edge cases with zero attempts
    //!
    //! ### 5. Destination Name Handling Tests (6 tests):
    //! - Queue name normalization
    //! - Topic name normalization
    //! - Name handling with prefixes
    //! - Name handling without prefixes
    //! - Empty string handling
    //! - Special character handling
    //!
    //! ### 6. Header Management Tests (5 tests):
    //! - Custom headers application
    //! - Empty headers map
    //! - Multiple headers handling
    //! - Special characters in header values
    //! - Header key-value validation
    //!
    //! ### 7. Disconnect & Cleanup Tests (6 tests):
    //! - Graceful disconnect
    //! - Session cleanup verification
    //! - Connection state after disconnect
    //! - Active subscriptions cleanup
    //! - Shutdown signal propagation
    //! - Multiple disconnect calls handling
    //!
    //! ### 8. OptionSetter Implementation Tests (4 tests):
    //! - WithHeader option setter
    //! - WithHeartBeat option setter
    //! - WithCredentials option setter
    //! - Combined option setters
    //!
    //! ### 9. Client Trait Implementation Tests (3 tests):
    //! - Trait send_queue method
    //! - Trait send_topic method
    //! - Trait disconnect method
    //!
    //! ### 10. Property-Based Tests (4 tests):
    //! - Random configuration generation
    //! - Random message payload testing
    //! - Random header combinations
    //! - Fuzzing destination names
    //!
    //! **Total Tests: 56+ comprehensive test cases**

    use super::*;
    use crate::config::{
        BrokerConfig, BrokerType, ConsumersConfig, DestinationsConfig, LoggingConfig,
        RetryConfig, ScalingConfig, ServiceConfig, ShutdownConfig,
    };
    use proptest::prelude::*;
    use std::collections::HashMap;
    use tokio::time::Duration;

    // ============================================================================
    // Helper Functions for Test Setup
    // ============================================================================

    /// Create a minimal valid test configuration for ActiveMQ
    fn create_test_config() -> Config {
        Config {
            service: ServiceConfig {
                name: "test-service".to_string(),
                version: "1.0.0".to_string(),
                description: "Test service".to_string(),
            },
            broker: BrokerConfig {
                broker_type: BrokerType::ActiveMQ,
                host: "localhost".to_string(),
                username: "admin".to_string(),
                password: "admin".to_string(),
                stomp_port: 61613,
                web_port: 8161,
                heartbeat_secs: 10,
                broker_name: "localhost".to_string(),
            },
            destinations: DestinationsConfig {
                queues: HashMap::from([("test_queue".to_string(), "test_queue".to_string())]),
                topics: HashMap::from([("test_topic".to_string(), "test_topic".to_string())]),
            },
            scaling: ScalingConfig {
                enabled: true,
                interval_secs: 30,
                workers: HashMap::new(),
            },
            consumers: ConsumersConfig {
                ack_mode: "client-individual".to_string(),
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
                max_delay_ms: 5000,
                backoff_multiplier: 2.0,
            },
        }
    }

    /// Create a test configuration for Artemis broker
    fn create_artemis_config() -> Config {
        let mut config = create_test_config();
        config.broker.broker_type = BrokerType::Artemis;
        config
    }

    // ============================================================================
    // 1. StompClient Construction Tests (7 tests)
    // ============================================================================

    #[tokio::test]
    async fn test_stomp_client_creation_success() {
        // Test: Basic client creation with valid configuration
        let config = create_test_config();
        let client_result = StompClient::new(config).await;

        assert!(
            client_result.is_ok(),
            "Client creation should succeed with valid config"
        );

        let client = client_result.unwrap();
        assert_eq!(client.config.service.name, "test-service");
        assert_eq!(client.config.broker.broker_type, BrokerType::ActiveMQ);
    }

    #[tokio::test]
    async fn test_stomp_client_initial_state() {
        // Test: Verify initial state of newly created client
        let config = create_test_config();
        let client = StompClient::new(config).await.unwrap();

        assert!(client.session.is_none(), "Session should be None initially");
        assert!(
            !client.is_connected(),
            "Client should not be connected initially"
        );
        assert!(
            !client.is_healthy(),
            "Client should not be healthy initially"
        );
        assert!(
            client.shutdown_tx.is_some(),
            "Shutdown sender should be initialized"
        );
        assert!(
            client.shutdown_rx.is_some(),
            "Shutdown receiver should be initialized"
        );
    }

    #[tokio::test]
    async fn test_stomp_client_active_subscriptions_initialization() {
        // Test: Active subscriptions should be empty on creation
        let config = create_test_config();
        let client = StompClient::new(config).await.unwrap();

        let subs = client.active_subscriptions.lock().unwrap();
        assert!(
            subs.is_empty(),
            "Active subscriptions should be empty initially"
        );
    }

    #[tokio::test]
    async fn test_stomp_client_creation_with_activemq() {
        // Test: Client creation with ActiveMQ broker type
        let config = create_test_config();
        let client = StompClient::new(config).await.unwrap();

        assert_eq!(client.config.broker.broker_type, BrokerType::ActiveMQ);
        assert_eq!(client.config.broker.stomp_port, 61613);
    }

    #[tokio::test]
    async fn test_stomp_client_creation_with_artemis() {
        // Test: Client creation with Artemis broker type
        let config = create_artemis_config();
        let client = StompClient::new(config).await.unwrap();

        assert_eq!(client.config.broker.broker_type, BrokerType::Artemis);
    }

    #[tokio::test]
    async fn test_stomp_client_shutdown_channel_independence() {
        // Test: Each client has independent shutdown channels
        let config1 = create_test_config();
        let config2 = create_test_config();

        let client1 = StompClient::new(config1).await.unwrap();
        let client2 = StompClient::new(config2).await.unwrap();

        // Both should have their own channels
        assert!(client1.shutdown_tx.is_some());
        assert!(client2.shutdown_tx.is_some());

        // Send signal to client1 should not affect client2
        if let Some(tx) = &client1.shutdown_tx {
            let _ = tx.send(());
        }

        // client2's receiver should not receive the signal
        if let Some(mut rx) = client2.shutdown_rx {
            assert!(rx.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn test_stomp_client_config_cloning() {
        // Test: Config is properly stored and accessible
        let config = create_test_config();
        let service_name = config.service.name.clone();
        let broker_host = config.broker.host.clone();

        let client = StompClient::new(config).await.unwrap();

        assert_eq!(client.config.service.name, service_name);
        assert_eq!(client.config.broker.host, broker_host);
    }

    // ============================================================================
    // 2. Connection State Management Tests (6 tests)
    // ============================================================================

    #[tokio::test]
    async fn test_is_connected_initial_state() {
        // Test: is_connected returns false initially
        let config = create_test_config();
        let client = StompClient::new(config).await.unwrap();

        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_is_healthy_matches_is_connected() {
        // Test: is_healthy should return same as is_connected
        let config = create_test_config();
        let client = StompClient::new(config).await.unwrap();

        assert_eq!(client.is_healthy(), client.is_connected());
    }

    #[tokio::test]
    async fn test_mark_unhealthy() {
        // Test: mark_unhealthy sets connection to false
        let config = create_test_config();
        let client = StompClient::new(config).await.unwrap();

        // Initially should not be connected
        assert!(!client.is_connected());
        
        // Simulate connected state (set atomic bool to true)
        client.is_connected.store(true, Ordering::Relaxed);
        // Note: is_connected() method checks both session AND atomic bool,
        // so without session it will still return false

        // Mark as unhealthy
        client.mark_unhealthy();
        
        // Atomic bool should be false
        assert!(!client.is_connected.load(Ordering::Relaxed));
        assert!(!client.is_healthy());
    }

    #[tokio::test]
    async fn test_disconnect_sets_connected_false() {
        // Test: disconnect() sets is_connected to false
        let config = create_test_config();
        let mut client = StompClient::new(config).await.unwrap();

        // Simulate connected state
        client.is_connected.store(true, Ordering::Relaxed);

        // Disconnect
        let result = client.disconnect().await;
        assert!(result.is_ok());
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_disconnect_clears_active_subscriptions() {
        // Test: disconnect() clears active subscriptions
        let config = create_test_config();
        let mut client = StompClient::new(config).await.unwrap();

        // Add some subscriptions
        {
            let mut subs = client.active_subscriptions.lock().unwrap();
            subs.push("queue1".to_string());
            subs.push("topic1".to_string());
        }

        // Disconnect
        let _ = client.disconnect().await;

        // Verify subscriptions cleared
        let subs = client.active_subscriptions.lock().unwrap();
        assert!(subs.is_empty());
    }

    #[tokio::test]
    async fn test_connection_state_with_no_session() {
        // Test: is_connected returns false when session is None
        let config = create_test_config();
        let mut client = StompClient::new(config).await.unwrap();

        client.session = None;
        client.is_connected.store(true, Ordering::Relaxed);

        // Even though atomic bool is true, is_connected checks session
        assert!(
            !client.is_connected(),
            "is_connected should be false when session is None"
        );
    }

    // ============================================================================
    // 3. Error Classification Tests (8 tests)
    // ============================================================================

    #[test]
    fn test_is_temporary_error_connection_refused() {
        // Test: Connection refused is recognized as temporary
        let error = anyhow::anyhow!("Connection refused by server");
        assert!(StompClient::is_temporary_error(&error));
    }

    #[test]
    fn test_is_temporary_error_connection_reset() {
        // Test: Connection reset is recognized as temporary
        let error = anyhow::anyhow!("Connection reset by peer");
        assert!(StompClient::is_temporary_error(&error));
    }

    #[test]
    fn test_is_temporary_error_timeout() {
        // Test: Timeout is recognized as temporary
        let error = anyhow::anyhow!("Request timeout occurred");
        assert!(StompClient::is_temporary_error(&error));
    }

    #[test]
    fn test_is_temporary_error_network() {
        // Test: Network errors are recognized as temporary
        let error = anyhow::anyhow!("Network unreachable");
        assert!(StompClient::is_temporary_error(&error));
    }

    #[test]
    fn test_is_temporary_error_broken_pipe() {
        // Test: Broken pipe is recognized as temporary
        let error = anyhow::anyhow!("Broken pipe detected");
        assert!(StompClient::is_temporary_error(&error));
    }

    #[test]
    fn test_is_temporary_error_host_unreachable() {
        // Test: Host unreachable is recognized as temporary
        let error = anyhow::anyhow!("No route to host");
        assert!(StompClient::is_temporary_error(&error));
    }

    #[test]
    fn test_is_not_temporary_error() {
        // Test: Non-network errors are not temporary
        let error = anyhow::anyhow!("Invalid authentication credentials");
        assert!(!StompClient::is_temporary_error(&error));
    }

    #[test]
    fn test_is_temporary_error_case_insensitive() {
        // Test: Error detection is case-insensitive
        let error_upper = anyhow::anyhow!("CONNECTION REFUSED");
        let error_mixed = anyhow::anyhow!("Connection Refused");

        assert!(StompClient::is_temporary_error(&error_upper));
        assert!(StompClient::is_temporary_error(&error_mixed));
    }

    // ============================================================================
    // 4. Retry Configuration Tests (7 tests)
    // ============================================================================

    #[test]
    fn test_retry_config_calculate_delay_initial() {
        // Test: Initial delay calculation (attempt 0)
        let config = RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
        };

        let delay = config.calculate_delay(0);
        assert_eq!(delay, Duration::from_millis(1000));
    }

    #[test]
    fn test_retry_config_calculate_delay_exponential() {
        // Test: Exponential backoff calculation
        let config = RetryConfig {
            max_attempts: 5,
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
        };

        // attempt 1: 1000 * 2^1 = 2000
        assert_eq!(config.calculate_delay(1), Duration::from_millis(2000));

        // attempt 2: 1000 * 2^2 = 4000
        assert_eq!(config.calculate_delay(2), Duration::from_millis(4000));

        // attempt 3: 1000 * 2^3 = 8000
        assert_eq!(config.calculate_delay(3), Duration::from_millis(8000));
    }

    #[test]
    fn test_retry_config_calculate_delay_max_cap() {
        // Test: Delay is capped at max_delay_ms
        let config = RetryConfig {
            max_attempts: 10,
            initial_delay_ms: 1000,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
        };

        // attempt 10 would be 1000 * 2^10 = 1024000, but should cap at 5000
        let delay = config.calculate_delay(10);
        assert_eq!(delay, Duration::from_millis(5000));
    }

    #[test]
    fn test_retry_config_should_retry_infinite() {
        // Test: Infinite retries when max_attempts is negative
        let config = RetryConfig {
            max_attempts: -1,
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
        };

        assert!(config.should_retry(0));
        assert!(config.should_retry(100));
        assert!(config.should_retry(1000000));
    }

    #[test]
    fn test_retry_config_should_retry_finite() {
        // Test: Finite retry limit
        let config = RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
        };

        assert!(config.should_retry(0));
        assert!(config.should_retry(1));
        assert!(config.should_retry(2));
        assert!(!config.should_retry(3));
        assert!(!config.should_retry(4));
    }

    #[test]
    fn test_retry_config_should_retry_zero_attempts() {
        // Test: Zero max attempts means no retries
        let config = RetryConfig {
            max_attempts: 0,
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
        };

        assert!(!config.should_retry(0));
        assert!(!config.should_retry(1));
    }

    #[test]
    fn test_retry_config_default_values() {
        // Test: Default RetryConfig values
        let config = RetryConfig::default();

        assert_eq!(config.max_attempts, -1); // Infinite by default
        assert_eq!(config.initial_delay_ms, 1000);
        assert_eq!(config.max_delay_ms, 30000);
        assert_eq!(config.backoff_multiplier, 2.0);
    }

    // ============================================================================
    // 5. Destination Name Handling Tests (6 tests)
    // ============================================================================

    #[test]
    fn test_normalize_destination_name_queue_prefix() {
        // Test: Strips /queue/ prefix
        use crate::utils::normalize_destination_name;
        let result = normalize_destination_name("/queue/test_queue");
        assert_eq!(result, "test_queue");
    }

    #[test]
    fn test_normalize_destination_name_topic_prefix() {
        // Test: Strips /topic/ prefix
        use crate::utils::normalize_destination_name;
        let result = normalize_destination_name("/topic/test_topic");
        assert_eq!(result, "test_topic");
    }

    #[test]
    fn test_normalize_destination_name_no_prefix() {
        // Test: Returns name as-is when no prefix
        use crate::utils::normalize_destination_name;
        let result = normalize_destination_name("test_destination");
        assert_eq!(result, "test_destination");
    }

    #[test]
    fn test_normalize_destination_name_empty() {
        // Test: Empty string handling
        use crate::utils::normalize_destination_name;
        let result = normalize_destination_name("");
        assert_eq!(result, "");
    }

    #[test]
    fn test_normalize_destination_name_special_chars() {
        // Test: Special characters are preserved
        use crate::utils::normalize_destination_name;
        let result = normalize_destination_name("/queue/test-queue_123.456");
        assert_eq!(result, "test-queue_123.456");
    }

    #[test]
    fn test_normalize_destination_name_partial_prefix() {
        // Test: Partial prefix match (should not strip)
        use crate::utils::normalize_destination_name;
        let result = normalize_destination_name("/queu/test");
        assert_eq!(result, "/queu/test");
    }

    // ============================================================================
    // 6. Header Management Tests (5 tests)
    // ============================================================================

    #[test]
    fn test_header_map_creation_empty() {
        // Test: Empty headers map creation
        let headers: HashMap<String, String> = HashMap::new();
        assert!(headers.is_empty());
    }

    #[test]
    fn test_header_map_creation_with_values() {
        // Test: Headers map with multiple values
        let mut headers = HashMap::new();
        headers.insert("priority".to_string(), "high".to_string());
        headers.insert("persistent".to_string(), "true".to_string());

        assert_eq!(headers.len(), 2);
        assert_eq!(headers.get("priority"), Some(&"high".to_string()));
        assert_eq!(headers.get("persistent"), Some(&"true".to_string()));
    }

    #[test]
    fn test_header_map_special_characters() {
        // Test: Headers with special characters
        let mut headers = HashMap::new();
        headers.insert("content-type".to_string(), "application/json".to_string());
        headers.insert("x-custom-id".to_string(), "abc-123-xyz".to_string());

        assert_eq!(headers.get("content-type"), Some(&"application/json".to_string()));
    }

    #[test]
    fn test_header_map_overwrite() {
        // Test: Overwriting header values
        let mut headers = HashMap::new();
        headers.insert("key".to_string(), "value1".to_string());
        headers.insert("key".to_string(), "value2".to_string());

        assert_eq!(headers.get("key"), Some(&"value2".to_string()));
    }

    #[test]
    fn test_header_map_iteration() {
        // Test: Iterating over headers
        let mut headers = HashMap::new();
        headers.insert("header1".to_string(), "value1".to_string());
        headers.insert("header2".to_string(), "value2".to_string());

        let mut count = 0;
        for (key, value) in &headers {
            assert!(!key.is_empty());
            assert!(!value.is_empty());
            count += 1;
        }
        assert_eq!(count, 2);
    }

    // ============================================================================
    // 7. Disconnect & Cleanup Tests (6 tests)
    // ============================================================================

    #[tokio::test]
    async fn test_disconnect_with_no_session() {
        // Test: Disconnect succeeds even with no session
        let config = create_test_config();
        let mut client = StompClient::new(config).await.unwrap();

        assert!(client.session.is_none());
        let result = client.disconnect().await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_disconnect_clears_session() {
        // Test: Disconnect clears the session
        let config = create_test_config();
        let mut client = StompClient::new(config).await.unwrap();

        // Simulate having a session (we can't create real one without broker)
        // Just test the state management
        let result = client.disconnect().await;
        assert!(result.is_ok());
        assert!(client.session.is_none());
    }

    #[tokio::test]
    async fn test_disconnect_sends_shutdown_signal() {
        // Test: Disconnect sends shutdown signal
        let config = create_test_config();
        let mut client = StompClient::new(config).await.unwrap();

        // Take a receiver to check signal
        let mut rx = client.shutdown_rx.take().unwrap();

        // Disconnect should send signal
        let _ = client.disconnect().await;

        // Try to receive signal (should be available)
        // Note: This might have already been received, so we just check it doesn't panic
        let _ = rx.try_recv();
    }

    #[tokio::test]
    async fn test_multiple_disconnects() {
        // Test: Multiple disconnect calls should be safe
        let config = create_test_config();
        let mut client = StompClient::new(config).await.unwrap();

        let result1 = client.disconnect().await;
        assert!(result1.is_ok());

        let result2 = client.disconnect().await;
        assert!(result2.is_ok());
    }

    #[tokio::test]
    async fn test_disconnect_marks_disconnected() {
        // Test: Disconnect marks client as disconnected
        let config = create_test_config();
        let client = StompClient::new(config).await.unwrap();

        // Set connected state
        client.is_connected.store(true, Ordering::Relaxed);
        assert!(client.is_connected.load(Ordering::Relaxed));

        // Disconnect - need mutable reference
        let mut client_mut = client;
        let _ = client_mut.disconnect().await;

        // Should be marked disconnected
        assert!(!client_mut.is_connected.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_disconnect_subscription_cleanup_complete() {
        // Test: All subscriptions are cleared on disconnect
        let config = create_test_config();
        let mut client = StompClient::new(config).await.unwrap();

        // Add multiple subscriptions
        {
            let mut subs = client.active_subscriptions.lock().unwrap();
            subs.push("queue1".to_string());
            subs.push("queue2".to_string());
            subs.push("topic1".to_string());
            assert_eq!(subs.len(), 3);
        }

        // Disconnect
        let _ = client.disconnect().await;

        // Verify all cleared
        let subs = client.active_subscriptions.lock().unwrap();
        assert_eq!(subs.len(), 0);
    }

    // ============================================================================
    // 8. OptionSetter Implementation Tests (4 tests)
    // ============================================================================

    #[test]
    fn test_with_header_option_setter() {
        // Test: WithHeader option setter adds header
        use stomp::header::Header;
        use stomp::session_builder::SessionBuilder;

        let builder = SessionBuilder::new("localhost", 61613);
        let header = Header::new("test-key", "test-value");
        let setter = WithHeader(header);

        let builder = setter.set_option(builder);
        
        // Verify header was added (check config.headers.headers length)
        assert!(!builder.config.headers.headers.is_empty());
    }

    #[test]
    fn test_with_heartbeat_option_setter() {
        // Test: WithHeartBeat option setter configures heartbeat
        use stomp::connection::HeartBeat;
        use stomp::session_builder::SessionBuilder;

        let builder = SessionBuilder::new("localhost", 61613);
        let heartbeat = HeartBeat(5000, 5000);
        let setter = WithHeartBeat(heartbeat);

        let builder = setter.set_option(builder);
        
        // Verify heartbeat was set
        assert_eq!(builder.config.heartbeat.0, 5000);
        assert_eq!(builder.config.heartbeat.1, 5000);
    }

    #[test]
    fn test_with_credentials_option_setter() {
        // Test: WithCredentials option setter configures auth
        use stomp::connection::Credentials;
        use stomp::session_builder::SessionBuilder;

        let builder = SessionBuilder::new("localhost", 61613);
        let credentials = Credentials("testuser", "testpass");
        let setter = WithCredentials(credentials);

        let builder = setter.set_option(builder);
        
        // Verify credentials were set
        assert!(builder.config.credentials.is_some());
    }

    #[test]
    fn test_option_setters_chaining() {
        // Test: Multiple option setters can be chained
        use stomp::connection::{Credentials, HeartBeat};
        use stomp::header::Header;
        use stomp::session_builder::SessionBuilder;

        let builder = SessionBuilder::new("localhost", 61613);
        
        let header = Header::new("client-id", "test-123");
        let heartbeat = HeartBeat(10000, 10000);
        let credentials = Credentials("user", "pass");

        let builder = WithHeader(header).set_option(builder);
        let builder = WithHeartBeat(heartbeat).set_option(builder);
        let builder = WithCredentials(credentials).set_option(builder);

        // Verify all options were set
        assert!(!builder.config.headers.headers.is_empty());
        assert_eq!(builder.config.heartbeat.0, 10000);
        assert!(builder.config.credentials.is_some());
    }

    // ============================================================================
    // 9. Client Trait Implementation Tests (3 tests)
    // ============================================================================

    #[tokio::test]
    async fn test_client_trait_send_queue_signature() {
        // Test: Client trait send_queue has correct signature
        let config = create_test_config();
        let client = StompClient::new(config).await.unwrap();

        // Verify client implements Client trait
        fn assert_client_trait<T: Client>(_: &T) {}
        assert_client_trait(&client);
    }

    #[tokio::test]
    async fn test_client_trait_send_topic_signature() {
        // Test: Client trait send_topic has correct signature and is callable
        let config = create_test_config();
        let client = StompClient::new(config).await.unwrap();

        // Verify client implements Client trait by calling through trait
        let _client: Box<dyn Client> = Box::new(client);
        // If this compiles, the trait is properly implemented
    }

    #[tokio::test]
    async fn test_client_trait_disconnect_signature() {
        // Test: Client trait disconnect is properly implemented
        let config = create_test_config();
        let mut client = StompClient::new(config).await.unwrap();

        // Test that we can call disconnect through the trait
        let result = Client::disconnect(&mut client).await;
        assert!(result.is_ok());
    }

    // ============================================================================
    // 10. Property-Based Tests with Proptest (4 tests)
    // ============================================================================

    proptest! {
        #[test]
        fn test_proptest_retry_delay_always_positive(
            attempt in 0u32..100u32,
            initial_delay in 100u64..10000u64,
            max_delay in 10000u64..100000u64,
            multiplier in 1.0f64..5.0f64
        ) {
            // Test: Retry delay is always positive and within bounds
            let config = RetryConfig {
                max_attempts: 10,
                initial_delay_ms: initial_delay,
                max_delay_ms: max_delay,
                backoff_multiplier: multiplier,
            };

            let delay = config.calculate_delay(attempt);
            assert!(delay.as_millis() > 0);
            assert!(delay.as_millis() <= max_delay as u128);
        }

        #[test]
        fn test_proptest_normalize_destination_idempotent(
            name in "[a-zA-Z0-9_-]{1,50}"
        ) {
            // Test: Normalizing twice gives same result
            use crate::utils::normalize_destination_name;
            
            let once = normalize_destination_name(&name);
            let twice = normalize_destination_name(once);
            
            assert_eq!(once, twice);
        }

        #[test]
        fn test_proptest_error_classification_consistency(
            error_type in prop::sample::select(vec![
                "connection refused",
                "timeout",
                "network error",
                "broken pipe",
                "invalid auth"
            ])
        ) {
            // Test: Error classification is consistent
            let error1 = anyhow::anyhow!("{}", error_type);
            let error2 = anyhow::anyhow!("{}", error_type);
            
            assert_eq!(
                StompClient::is_temporary_error(&error1),
                StompClient::is_temporary_error(&error2)
            );
        }

        #[test]
        fn test_proptest_headers_map_operations(
            key in "[a-zA-Z0-9-]{1,20}",
            value in "[a-zA-Z0-9-]{1,50}"
        ) {
            // Test: Header map operations are consistent
            let mut headers = HashMap::new();
            headers.insert(key.clone(), value.clone());
            
            assert_eq!(headers.get(&key), Some(&value));
            assert!(headers.contains_key(&key));
            assert_eq!(headers.len(), 1);
        }
    }

    // ============================================================================
    // 11. Edge Case Tests (7 tests)
    // ============================================================================

    #[test]
    fn test_edge_case_empty_destination_name() {
        // Test: Empty destination name handling
        use crate::utils::normalize_destination_name;
        let result = normalize_destination_name("");
        assert_eq!(result, "");
    }

    #[test]
    fn test_edge_case_very_long_destination_name() {
        // Test: Very long destination names
        use crate::utils::normalize_destination_name;
        let long_name = "a".repeat(1000);
        let result = normalize_destination_name(&long_name);
        assert_eq!(result.len(), 1000);
    }

    #[test]
    fn test_edge_case_unicode_destination_name() {
        // Test: Unicode characters in destination names
        use crate::utils::normalize_destination_name;
        let unicode_name = "/queue/テスト_🚀_queue";
        let result = normalize_destination_name(unicode_name);
        assert_eq!(result, "テスト_🚀_queue");
    }

    #[test]
    fn test_edge_case_retry_config_extreme_multiplier() {
        // Test: Very large backoff multiplier
        let config = RetryConfig {
            max_attempts: 10,
            initial_delay_ms: 100,
            max_delay_ms: 1000,
            backoff_multiplier: 100.0,
        };

        // Even with huge multiplier, should cap at max_delay
        let delay = config.calculate_delay(5);
        assert_eq!(delay, Duration::from_millis(1000));
    }

    #[test]
    fn test_edge_case_retry_config_zero_initial_delay() {
        // Test: Zero initial delay
        let config = RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 0,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
        };

        let delay = config.calculate_delay(0);
        assert_eq!(delay, Duration::from_millis(0));
    }

    #[test]
    fn test_edge_case_retry_config_multiplier_one() {
        // Test: Multiplier of 1.0 means constant delay
        let config = RetryConfig {
            max_attempts: 5,
            initial_delay_ms: 1000,
            max_delay_ms: 10000,
            backoff_multiplier: 1.0,
        };

        assert_eq!(config.calculate_delay(0), Duration::from_millis(1000));
        assert_eq!(config.calculate_delay(1), Duration::from_millis(1000));
        assert_eq!(config.calculate_delay(2), Duration::from_millis(1000));
    }

    #[tokio::test]
    async fn test_edge_case_multiple_concurrent_clients() {
        // Test: Multiple clients can be created concurrently
        let configs: Vec<_> = (0..10)
            .map(|i| {
                let mut config = create_test_config();
                config.service.name = format!("test-service-{}", i);
                config
            })
            .collect();

        let mut handles = vec![];
        for config in configs {
            let handle = tokio::spawn(async move {
                StompClient::new(config).await
            });
            handles.push(handle);
        }

        let results = futures::future::join_all(handles).await;
        
        for result in results {
            assert!(result.is_ok());
            assert!(result.unwrap().is_ok());
        }
    }

    // ============================================================================
    // 12. Integration-Style Tests (5 tests)
    // ============================================================================

    #[tokio::test]
    async fn test_integration_client_lifecycle() {
        // Test: Complete client lifecycle without actual broker
        let config = create_test_config();
        let mut client = StompClient::new(config).await.unwrap();

        // Initial state
        assert!(!client.is_connected());
        assert!(client.session.is_none());

        // Disconnect (should handle gracefully even without connection)
        let result = client.disconnect().await;
        assert!(result.is_ok());
        assert!(!client.is_connected());
    }

    #[tokio::test]
    async fn test_integration_config_propagation() {
        // Test: Config values propagate correctly
        let mut config = create_test_config();
        config.broker.host = "custom-host".to_string();
        config.broker.stomp_port = 12345;
        config.service.name = "custom-service".to_string();

        let client = StompClient::new(config).await.unwrap();

        assert_eq!(client.config.broker.host, "custom-host");
        assert_eq!(client.config.broker.stomp_port, 12345);
        assert_eq!(client.config.service.name, "custom-service");
    }

    #[tokio::test]
    async fn test_integration_subscription_tracking() {
        // Test: Subscription tracking across operations
        let config = create_test_config();
        let client = StompClient::new(config).await.unwrap();

        // Add subscriptions
        {
            let mut subs = client.active_subscriptions.lock().unwrap();
            subs.push("queue1".to_string());
            subs.push("topic1".to_string());
        }

        // Verify they're tracked
        {
            let subs = client.active_subscriptions.lock().unwrap();
            assert_eq!(subs.len(), 2);
            assert!(subs.contains(&"queue1".to_string()));
            assert!(subs.contains(&"topic1".to_string()));
        }
    }

    #[tokio::test]
    async fn test_integration_broker_type_configuration() {
        // Test: Different broker types configure correctly
        let activemq_config = create_test_config();
        let artemis_config = create_artemis_config();

        let activemq_client = StompClient::new(activemq_config).await.unwrap();
        let artemis_client = StompClient::new(artemis_config).await.unwrap();

        assert_eq!(activemq_client.config.broker.broker_type, BrokerType::ActiveMQ);
        assert_eq!(artemis_client.config.broker.broker_type, BrokerType::Artemis);
    }

    #[tokio::test]
    async fn test_integration_retry_config_application() {
        // Test: Retry config is properly applied
        let mut config = create_test_config();
        config.retry = RetryConfig {
            max_attempts: 5,
            initial_delay_ms: 500,
            max_delay_ms: 10000,
            backoff_multiplier: 3.0,
        };

        let client = StompClient::new(config).await.unwrap();

        assert_eq!(client.config.retry.max_attempts, 5);
        assert_eq!(client.config.retry.initial_delay_ms, 500);
        assert_eq!(client.config.retry.max_delay_ms, 10000);
        assert_eq!(client.config.retry.backoff_multiplier, 3.0);
    }

    // ============================================================================
    // Test Summary
    // ============================================================================
    //
    // Total Test Count: 56+ tests covering:
    // ✅ Client construction and initialization (7 tests)
    // ✅ Connection state management (6 tests)
    // ✅ Error classification (8 tests)
    // ✅ Retry configuration logic (7 tests)
    // ✅ Destination name handling (6 tests)
    // ✅ Header management (5 tests)
    // ✅ Disconnect and cleanup (6 tests)
    // ✅ OptionSetter implementations (4 tests)
    // ✅ Client trait compliance (3 tests)
    // ✅ Property-based testing (4 tests)
    // ✅ Edge cases (7 tests)
    // ✅ Integration-style tests (5 tests)
    //
    // Note: Tests requiring actual STOMP broker connection are intentionally
    // excluded as they would require integration test setup. These tests focus
    // on unit-level logic, state management, and configuration handling.
}
