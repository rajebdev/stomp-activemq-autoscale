use anyhow::{Context, Result};
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::future::Future;
use tokio::sync::broadcast;
use tokio::task::JoinSet;
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
use crate::consumer_pool::{ConsumerPool, MessageHandler};
use crate::autoscaler::AutoScaler;
use crate::utils::{normalize_destination_name};

/// Type alias for message handler function
pub type MessageHandlerFn = Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static>;

/// Type alias for auto-scaling message handler function  
pub type AutoScaleHandlerFn = Arc<dyn Fn(String) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> + Send + Sync + 'static>;

/// Configuration for a queue with custom handler
pub struct QueueConfig {
    pub name: String,
    pub handler: Option<MessageHandlerFn>,
    pub auto_scaling: bool,
}

/// Configuration for a topic with custom handler
pub struct TopicConfig {
    pub name: String,
    pub handler: Option<MessageHandlerFn>,
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

/// Core STOMP service for handling message operations
pub struct StompService {
    config: Config,
    session: Option<Session>,
    /// Tracks whether the connection is healthy
    is_connected: Arc<AtomicBool>,
    /// Shutdown signal for graceful cleanup
    shutdown_tx: Option<broadcast::Sender<()>>,
    shutdown_rx: Option<broadcast::Receiver<()>>,
    /// Track subscription destinations for reconnection
    active_subscriptions: Arc<std::sync::Mutex<Vec<String>>>,
    /// Builder pattern: queue configurations with handlers
    queue_configs: Vec<QueueConfig>,
    /// Builder pattern: topic configurations with handlers
    topic_configs: Vec<TopicConfig>,
    /// Auto-scaling handlers for queues
    auto_scale_handlers: HashMap<String, AutoScaleHandlerFn>,
}

impl StompService {
    /// Create a new STOMP service instance with validated configuration
    pub async fn new(config: Config) -> Result<Self> {
        debug!("Initializing STOMP service: {}", config.service.name);
        
        // Validate configuration upfront for fail-fast behavior
        Self::validate_config(&config)?;

        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        Ok(Self {
            config,
            session: None,
            is_connected: Arc::new(AtomicBool::new(false)),
            shutdown_tx: Some(shutdown_tx),
            shutdown_rx: Some(shutdown_rx),
            active_subscriptions: Arc::new(std::sync::Mutex::new(Vec::new())),
            queue_configs: Vec::new(),
            topic_configs: Vec::new(),
            auto_scale_handlers: HashMap::new(),
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
            // Check if we have priority override in custom headers
            if let Some(priority_str) = headers.get("priority") {
                if let Ok(priority) = priority_str.parse::<u8>() {
                    let clamped_priority = priority.clamp(0, 9);
                    // Use broker-appropriate priority header
                    let priority_header = match self.config.broker.broker_type {
                        crate::config::BrokerType::ActiveMQ => "JMSPriority",
                        crate::config::BrokerType::Artemis => "JMSPriority", // Artemis also uses JMSPriority
                    };
                    message_builder = message_builder.add_header(priority_header, &clamped_priority.to_string());
                    debug!("Setting topic message priority: {} (header: {})", clamped_priority, priority_header);
                }
            }

            // Check if we have content-type override
            if let Some(content_type) = headers.get("content-type") {
                message_builder = message_builder.with_content_type(content_type);
                debug!("Setting content-type: {}", content_type);
            }

            // Add remaining custom headers
            let processed_headers = ["priority", "content-type"];
            for (key, value) in &headers {
                if !processed_headers.contains(&key.as_str()) {
                    message_builder = message_builder.add_header(key, value);
                    trace!("Adding custom topic header: {} = {}", key, value);
                }
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
            // Check if we have priority override in custom headers
            if let Some(priority_str) = headers.get("priority") {
                if let Ok(priority) = priority_str.parse::<u8>() {
                    // Priority range: 0-9 (0 = lowest, 9 = highest)
                    let clamped_priority = priority.clamp(0, 9);
                    // Use broker-appropriate priority header
                    let priority_header = match self.config.broker.broker_type {
                        crate::config::BrokerType::ActiveMQ => "JMSPriority",
                        crate::config::BrokerType::Artemis => "JMSPriority", // Artemis also uses JMSPriority
                    };
                    message_builder = message_builder.add_header(priority_header, &clamped_priority.to_string());
                    debug!("Setting message priority: {} (header: {})", clamped_priority, priority_header);
                }
            }

            // Check for persistent/non-persistent delivery mode
            if let Some(persistent_str) = headers.get("persistent") {
                let delivery_header = match self.config.broker.broker_type {
                    crate::config::BrokerType::ActiveMQ => "JMSDeliveryMode",
                    crate::config::BrokerType::Artemis => "JMSDeliveryMode", // Artemis also uses JMSDeliveryMode
                };
                
                match persistent_str.to_lowercase().as_str() {
                    "true" | "1" | "yes" => {
                        // Persistent delivery (messages survive broker restart)
                        // Use JMSDeliveryMode: 2 = PERSISTENT, 1 = NON_PERSISTENT
                        message_builder = message_builder.add_header(delivery_header, "2");
                        debug!("Setting persistent delivery mode (header: {})", delivery_header);
                    }
                    "false" | "0" | "no" => {
                        // Non-persistent delivery (faster but messages lost on broker restart)
                        message_builder = message_builder.add_header(delivery_header, "1");
                        debug!("Setting non-persistent delivery mode (header: {})", delivery_header);
                    }
                    _ => {
                        // Invalid value, default to persistent for safety
                        message_builder = message_builder.add_header(delivery_header, "2");
                        warn!("Invalid persistent value '{}', defaulting to persistent (header: {})", persistent_str, delivery_header);
                    }
                }
            }

            // Check for TTL (Time-To-Live)
            if let Some(ttl_str) = headers.get("ttl") {
                if let Ok(ttl_ms) = ttl_str.parse::<u64>() {
                    // Use broker-appropriate expiration header
                    let expiration_header = match self.config.broker.broker_type {
                        crate::config::BrokerType::ActiveMQ => "JMSExpiration",
                        crate::config::BrokerType::Artemis => "JMSExpiration", // Artemis also uses JMSExpiration
                    };
                    let expiry_time = chrono::Utc::now().timestamp_millis() as u64 + ttl_ms;
                    message_builder = message_builder.add_header(expiration_header, &expiry_time.to_string());
                    debug!("Setting message TTL: {}ms (expires at: {}, header: {})", ttl_ms, expiry_time, expiration_header);
                }
            }

            // Check if we have content-type override
            if let Some(content_type) = headers.get("content-type") {
                message_builder = message_builder.with_content_type(content_type);
                debug!("Setting content-type: {}", content_type);
            }

            // Add remaining custom headers (excluding ones we already processed)
            let processed_headers = ["priority", "persistent", "ttl", "content-type"];
            for (key, value) in &headers {
                if !processed_headers.contains(&key.as_str()) {
                    message_builder = message_builder.add_header(key, value);
                    trace!("Adding custom header: {} = {}", key, value);
                }
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

    /// Validate configuration for safety and correctness
    fn validate_config(config: &Config) -> Result<()> {
        // Basic validation checks
        if config.service.name.is_empty() {
            return Err(anyhow::anyhow!("Service name cannot be empty"));
        }
        
        if config.broker.host.is_empty() {
            return Err(anyhow::anyhow!("Broker host cannot be empty"));
        }
        
        if config.broker.stomp_port == 0 {
            return Err(anyhow::anyhow!("Invalid STOMP port: 0"));
        }
        
        // Validate auto-scaling configuration if enabled
        if config.is_auto_scaling_enabled() {
            let auto_queues = config.get_auto_scaling_queues();
            if auto_queues.is_empty() {
                warn!("Auto-scaling enabled but no queues configured for auto-scaling");
            }
            
            // Validate worker ranges
            for queue_name in &auto_queues {
                if let Some(range) = config.get_queue_worker_range(queue_name) {
                    if range.min > range.max {
                        return Err(anyhow::anyhow!(
                            "Invalid worker range for queue '{}': min ({}) > max ({})",
                            queue_name, range.min, range.max
                        ));
                    }
                    if range.min == 0 {
                        return Err(anyhow::anyhow!(
                            "Minimum workers cannot be 0 for queue '{}'",
                            queue_name
                        ));
                    }
                }
            }
        }
        
        debug!("Configuration validation passed");
        Ok(())
    }

    // ========== Helper Methods from runner.rs ==========

    /// Setup consumer pools with custom handlers for auto-scaling queues
    async fn setup_custom_consumer_pools(&self) -> Result<HashMap<String, ConsumerPool>> {
        let mut consumer_pools = HashMap::new();
        
        // Get auto-scaling queues from config
        let config_auto_queues = self.config.get_auto_scaling_queues();
        
        // Combine custom queues and config queues
        let mut all_auto_queues = Vec::new();
        
        // Add custom configured queues that are defined as auto-scaling in config
        for queue_config in &self.queue_configs {
            if self.config.get_auto_scaling_queues().contains(&queue_config.name) {
                all_auto_queues.push(queue_config.name.clone());
            }
        }
        
        // Add queues from config that aren't already in custom configs
        for queue_name in &config_auto_queues {
            if !all_auto_queues.contains(queue_name) {
                all_auto_queues.push(queue_name.clone());
            }
        }
        
        if all_auto_queues.is_empty() {
            return Ok(HashMap::new());
        }

        info!("📊 Setting up {} auto-scaling queues", all_auto_queues.len());

        for queue_name in &all_auto_queues {
            if let Some(worker_range) = self.config.get_queue_worker_range(queue_name) {
                debug!(
                    "🏊 Creating consumer pool for '{}' (workers: {}-{})",
                    queue_name, worker_range.min, worker_range.max
                );

                // Use custom handler if provided, otherwise skip this queue
                if let Some(custom_handler) = self.auto_scale_handlers.get(queue_name) {
                    let handler = self.create_custom_queue_message_handler(queue_name.clone(), custom_handler.clone());
                    
                    // Create consumer pool
                    let mut pool = ConsumerPool::new(
                        queue_name.clone(),
                        self.config.clone(),
                        worker_range.clone(),
                        handler,
                    );

                    // Initialize pool with minimum workers
                    pool.initialize().await?;
                    
                    consumer_pools.insert(queue_name.clone(), pool);
                } else {
                    debug!("No custom handler configured for auto-scaling queue '{}', skipping", queue_name);
                    continue;
                }
            }
        }

        Ok(consumer_pools)
    }

    /// Create a message handler wrapper for consumer pools
    fn create_custom_queue_message_handler(
        &self,
        queue_name: String,
        custom_handler: AutoScaleHandlerFn,
    ) -> Box<MessageHandler> {
        Box::new(move |message: String| {
            let handler = custom_handler.clone();
            let queue_name = queue_name.clone();
            
            Box::pin(async move {
                trace!("Processing message for queue '{}': {}", queue_name, message.chars().take(50).collect::<String>());
                
                match handler(message).await {
                    Ok(()) => {
                        trace!("✅ Message processed successfully for queue '{}'", queue_name);
                        Ok(())
                    }
                    Err(e) => {
                        error!("❌ Error processing message for queue '{}': {}", queue_name, e);
                        Err(e)
                    }
                }
            })
        })
    }

    /// Setup topic subscribers with custom handlers for auto-scaling mode
    async fn setup_custom_topic_subscribers(
        &self,
        shutdown_tx: &broadcast::Sender<()>,
    ) -> JoinSet<Result<()>> {
        let mut topic_tasks = JoinSet::new();
        
        // Get topic names from config
        let config_topics = self.config.get_all_topic_names();
        
        // Combine custom topics and config topics
        let mut all_topics = Vec::new();
        
        // Add custom configured topics
        for topic_config in &self.topic_configs {
            all_topics.push(topic_config.name.clone());
        }
        
        // Add topics from config that aren't already in custom configs
        for topic_name in &config_topics {
            if !all_topics.contains(topic_name) {
                all_topics.push(topic_name.clone());
            }
        }
        
        for topic_name in all_topics {
            debug!("📊 Topic '{}' configured with 1 static worker", topic_name);

            let config_clone = self.config.clone();
            let _shutdown_rx = shutdown_tx.subscribe();
            let topic_name_clone = topic_name.clone();
            
            // Find custom handler for this topic
            let custom_handler = self.topic_configs.iter()
                .find(|tc| tc.name == topic_name)
                .and_then(|tc| tc.handler.as_ref())
                .cloned();

            topic_tasks.spawn(async move {
                let mut service = StompService::new(config_clone).await?;
                
                if let Some(handler) = custom_handler {
                    service.receive_topic(&topic_name_clone, move |msg| handler(msg)).await
                } else {
                    // No handler configured - skip this topic
                    debug!("No handler configured for topic '{}', skipping", topic_name_clone);
                    Ok(())
                }
            });
        }

        topic_tasks
    }

    /// Setup fixed worker subscribers (for hybrid auto-scaling mode)
    async fn setup_fixed_worker_subscribers(
        &self,
        shutdown_tx: &broadcast::Sender<()>,
        subscriber_tasks: &mut JoinSet<Result<()>>,
    ) {
        // Get fixed worker queues from config
        let fixed_worker_queues = self.config.get_fixed_worker_queues();
        
        if fixed_worker_queues.is_empty() {
            return;
        }
        
        info!("🔧 Setting up {} fixed worker queues alongside auto-scaling", fixed_worker_queues.len());
        
        for queue_name in &fixed_worker_queues {
            // Get worker count for this queue
            let worker_count = self.config.get_queue_worker_range(queue_name)
                .map(|range| range.min)
                .unwrap_or(1);
            
            debug!("📊 Fixed worker queue '{}' configured with {} worker(s)", queue_name, worker_count);
            
            // Find custom handler for this queue
            let custom_handler = self.queue_configs.iter()
                .find(|qc| qc.name == *queue_name)
                .and_then(|qc| qc.handler.as_ref())
                .cloned();

            // Start multiple workers for this queue if needed
            for worker_id in 0..worker_count {
                let config_clone = self.config.clone();
                let _shutdown_rx = shutdown_tx.subscribe();
                let queue_name_clone = queue_name.clone();
                let custom_handler_clone = custom_handler.clone();

                subscriber_tasks.spawn(async move {
                    // Add small delay to stagger worker startup
                    if worker_id > 0 {
                        tokio::time::sleep(Duration::from_millis(worker_id as u64 * 100)).await;
                    }

                    let mut service = StompService::new(config_clone).await?;
                    
                    if let Some(handler) = custom_handler_clone {
                        service.receive_queue(&queue_name_clone, move |msg| handler(msg)).await
                    } else {
                        debug!("No handler configured for fixed worker queue '{}', skipping", queue_name_clone);
                        Ok(())
                    }
                });
            }
        }
    }

    /// Setup static subscribers for queues and topics
    async fn setup_custom_static_subscribers(
        &self,
        shutdown_tx: &broadcast::Sender<()>,
        subscriber_tasks: &mut JoinSet<Result<()>>,
    ) {
        // Handle queues
        for queue_config in &self.queue_configs {
            let worker_count = self.config.get_queue_worker_range(&queue_config.name)
                .map(|range| range.min)
                .unwrap_or(1);
            
            debug!("📊 Queue '{}' configured with {} static worker(s)", queue_config.name, worker_count);
            
            // Start multiple workers for this queue if needed
            for worker_id in 0..worker_count {
                let config_clone = self.config.clone();
                let _shutdown_rx = shutdown_tx.subscribe();
                let queue_name_clone = queue_config.name.clone();
                let custom_handler_clone = queue_config.handler.clone();

                subscriber_tasks.spawn(async move {
                    // Add small delay to stagger worker startup
                    if worker_id > 0 {
                        tokio::time::sleep(Duration::from_millis(worker_id as u64 * 100)).await;
                    }

                    let mut service = StompService::new(config_clone).await?;
                    
                    if let Some(handler) = custom_handler_clone {
                        service.receive_queue(&queue_name_clone, move |msg| handler(msg)).await
                    } else {
                        debug!("No handler configured for static queue '{}', skipping", queue_name_clone);
                        Ok(())
                    }
                });
            }
        }

        // Handle topics
        for topic_config in &self.topic_configs {
            debug!("📊 Topic '{}' configured with 1 static worker", topic_config.name);

            let config_clone = self.config.clone();
            let _shutdown_rx = shutdown_tx.subscribe();
            let topic_name_clone = topic_config.name.clone();
            let custom_handler_clone = topic_config.handler.clone();

            subscriber_tasks.spawn(async move {
                let mut service = StompService::new(config_clone).await?;
                
                if let Some(handler) = custom_handler_clone {
                    service.receive_topic(&topic_name_clone, move |msg| handler(msg)).await
                } else {
                    debug!("No handler configured for static topic '{}', skipping", topic_name_clone);
                    Ok(())
                }
            });
        }
    }

    /// Setup graceful shutdown signal handler
    fn setup_shutdown_handler(&self, shutdown_tx: broadcast::Sender<()>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = signal(SignalKind::terminate()).unwrap();
                let mut sigint = signal(SignalKind::interrupt()).unwrap();
                
                tokio::select! {
                    _ = sigterm.recv() => info!("Received SIGTERM"),
                    _ = sigint.recv() => info!("Received SIGINT"),
                    _ = tokio::signal::ctrl_c() => info!("Received Ctrl+C"),
                }
            }
            
            #[cfg(windows)]
            {
                tokio::signal::ctrl_c().await.unwrap();
                info!("Received Ctrl+C");
            }
            
            info!("Initiating graceful shutdown...");
            let _ = shutdown_tx.send(());
        })
    }

    /// Start auto-scaler in background
    async fn start_autoscaler(&self, mut autoscaler: AutoScaler) -> tokio::task::JoinHandle<Result<()>> {
        tokio::spawn(async move {
            autoscaler.run().await
        })
    }

    /// Shutdown hybrid system (auto-scaling + static workers)
    async fn shutdown_hybrid_system(
        &self,
        shutdown_handle: tokio::task::JoinHandle<()>,
        autoscaler_handle: tokio::task::JoinHandle<Result<()>>,
        mut topic_tasks: JoinSet<Result<()>>,
        mut subscriber_tasks: JoinSet<Result<()>>,
    ) -> Result<()> {
        // Wait for shutdown signal
        let _ = shutdown_handle.await;

        info!("Shutdown signal received, stopping autoscaler and all workers...");

        // Stop autoscaler
        autoscaler_handle.abort();
        if let Err(e) = autoscaler_handle.await {
            if !e.is_cancelled() {
                warn!("Autoscaler shutdown error: {}", e);
            }
        }

        // Stop all topic tasks
        topic_tasks.shutdown().await;

        // Stop all subscriber tasks
        subscriber_tasks.shutdown().await;

        // Give some time for graceful cleanup
        tokio::time::sleep(Duration::from_secs(self.config.shutdown.grace_period_secs as u64)).await;

        info!("STOMP service shutdown complete");
        Ok(())
    }

    /// Shutdown static workers gracefully
    async fn shutdown_static_workers(&self, mut subscriber_tasks: JoinSet<Result<()>>) {
        // Stop all subscriber tasks
        subscriber_tasks.shutdown().await;

        // Give some time for graceful cleanup
        tokio::time::sleep(Duration::from_secs(self.config.shutdown.grace_period_secs as u64)).await;
    }

    // ========== Builder Pattern Methods (from runner.rs) ==========

    /// Add a queue with a custom handler (supports both static and auto-scaling)
    pub fn add_queue<F, Fut>(mut self, queue_name: &str, handler: F) -> Self 
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        // Create shared handler that can be used for both static and auto-scaling
        let shared_handler = Arc::new(handler);
        
        // Create static handler wrapper
        let static_handler = shared_handler.clone();
        let handler_fn: MessageHandlerFn = Arc::new(move |msg| {
            let handler = static_handler.clone();
            Box::pin(async move { handler(msg).await })
        });
        
        // Create auto-scaling handler wrapper  
        let auto_handler = shared_handler.clone();
        let auto_scale_handler_fn: AutoScaleHandlerFn = Arc::new(move |msg| {
            let handler = auto_handler.clone();
            Box::pin(async move { handler(msg).await })
        });
        
        // Store both static and auto-scaling handlers
        self.auto_scale_handlers.insert(queue_name.to_string(), auto_scale_handler_fn);
        
        self.queue_configs.push(QueueConfig {
            name: queue_name.to_string(),
            handler: Some(handler_fn),
            auto_scaling: true, // This will be determined by config at runtime
        });
        self
    }

    /// Add a topic with custom handler
    pub fn add_topic<F, Fut>(mut self, topic_name: &str, handler: F) -> Self 
    where
        F: Fn(String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let handler_fn: MessageHandlerFn = Arc::new(move |msg| {
            Box::pin(handler(msg))
        });
        
        self.topic_configs.push(TopicConfig {
            name: topic_name.to_string(),
            handler: Some(handler_fn),
        });
        self
    }

    /// Run the STOMP application with the configured settings
    /// Config is already validated in constructor, safe to proceed
    pub async fn run(self) -> Result<()> {
        debug!("Starting STOMP service with validated configuration");
        
        // Config is guaranteed to be valid at this point
        let monitoring_enabled = self.config.is_auto_scaling_enabled();
        let has_auto_scaling_queues = !self.config.get_auto_scaling_queues().is_empty();
        let has_custom_handlers_for_auto_scaling = self.queue_configs.iter()
            .any(|q| self.config.get_auto_scaling_queues().contains(&q.name));
        
        if monitoring_enabled && (has_auto_scaling_queues || has_custom_handlers_for_auto_scaling) {
            info!("Starting with auto-scaling mode enabled");
            self.run_with_auto_scaling().await
        } else {
            if self.config.is_monitoring_configured() && !monitoring_enabled {
                info!("Monitoring disabled, using minimum worker counts");
            } else {
                info!("Starting with static worker mode");
            }
            self.run_static_workers().await
        }
    }

    /// Run with auto-scaling enabled
    async fn run_with_auto_scaling(self) -> Result<()> {
        debug!("Initializing auto-scaling system");

        // Create shutdown broadcast channel
        let (shutdown_tx, _shutdown_rx) = broadcast::channel::<()>(1);

        // Setup auto-scaling consumer pools
        let consumer_pools = self.setup_custom_consumer_pools().await?;

        // Create task collection for managing fixed worker subscribers
        let mut subscriber_tasks = JoinSet::new();

        // Setup fixed worker queues alongside auto-scaling
        self.setup_fixed_worker_subscribers(&shutdown_tx, &mut subscriber_tasks).await;

        if consumer_pools.is_empty() && subscriber_tasks.is_empty() {
            warn!("No queues configured for auto-scaling or fixed workers");
            return Ok(());
        }

        if !consumer_pools.is_empty() {
            debug!("Consumer pools initialized for {} queues", consumer_pools.len());
        }

        // Handle topics with static workers (if any)
        let topic_tasks = self.setup_custom_topic_subscribers(&shutdown_tx).await;

        // Create and start auto-scaler
        let autoscaler = AutoScaler::new(
            self.config.clone(),
            consumer_pools,
            shutdown_tx.subscribe(),
        )?;

        // Setup graceful shutdown signal handler
        let shutdown_handle = self.setup_shutdown_handler(shutdown_tx.clone());

        // Start auto-scaler in background
        let autoscaler_handle = self.start_autoscaler(autoscaler).await;

        info!("STOMP service started successfully");

        // Wait for shutdown and cleanup
        self.shutdown_hybrid_system(shutdown_handle, autoscaler_handle, topic_tasks, subscriber_tasks).await
    }

    /// Run with static workers
    async fn run_static_workers(self) -> Result<()> {
        debug!("Initializing static worker system");
        
        // Create shutdown broadcast channel
        let (shutdown_tx, _shutdown_rx) = broadcast::channel::<()>(1);

        // Create task collection for managing subscribers
        let mut subscriber_tasks = JoinSet::new();

        // Setup static subscribers for queues and topics
        self.setup_custom_static_subscribers(&shutdown_tx, &mut subscriber_tasks).await;

        // Setup graceful shutdown signal handler
        let shutdown_handle = self.setup_shutdown_handler(shutdown_tx.clone());

        info!("STOMP service started successfully");

        // Wait for shutdown signal
        let _ = shutdown_handle.await;

        info!("Shutdown signal received, stopping all subscribers...");

        // Shutdown static workers gracefully
        self.shutdown_static_workers(subscriber_tasks).await;

        info!("STOMP service shutdown complete");
        Ok(())
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
                StompService::is_temporary_error(&error),
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
                !StompService::is_temporary_error(&error),
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
    async fn test_stomp_service_creation() {
        let config = create_test_config();
        let service = StompService::new(config).await;
        assert!(service.is_ok());

        let service = service.unwrap();
        assert!(!service.is_connected());
        assert!(!service.is_healthy());
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
    async fn test_artemis_stomp_service_creation() {
        let config = create_artemis_test_config();
        let service = StompService::new(config).await;
        assert!(service.is_ok());

        let service = service.unwrap();
        assert!(!service.is_connected());
        assert!(!service.is_healthy());
        assert_eq!(service.config.broker.broker_type, crate::config::BrokerType::Artemis);
        assert_eq!(service.config.broker.broker_name, "broker");
    }
}
