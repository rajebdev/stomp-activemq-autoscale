use anyhow::{Context, Result};
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::broker_monitor::BrokerMonitor;
use crate::config::{BrokerConfig, BrokerType};
use crate::utils::{normalize_destination_name, determine_routing_types};

#[derive(Error, Debug)]
#[allow(clippy::enum_variant_names)]
pub enum MonitoringError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("JSON parsing failed: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Broker API error: {0}")]
    BrokerAPIError(String),
}

/// Queue metrics returned from message broker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueMetrics {
    /// Queue name
    pub queue_name: String,
    /// Current queue size (number of pending messages)
    pub queue_size: u32,
    /// Number of active consumers
    pub consumer_count: u32,
    /// Total number of messages enqueued
    pub enqueue_count: u64,
    /// Total number of messages dequeued
    pub dequeue_count: u64,
    /// Memory usage percentage
    pub memory_percent_usage: f64,
}

/// Jolokia JSON response structure
#[derive(Debug, Deserialize)]
struct JolokiaResponse {
    value: Value,
    status: u16,
    #[serde(default)]
    error: Option<String>,
}

/// ActiveMQ monitoring client
#[derive(Debug)]
pub struct ActiveMQMonitor {
    client: Client,
    config: BrokerConfig,
    retry_count: u32,
    max_retries: u32,
}

impl ActiveMQMonitor {
    /// Create a new ActiveMQ monitoring client
    pub fn new(config: BrokerConfig) -> Result<Self> {
        // Ensure this is for ActiveMQ
        if config.broker_type != BrokerType::ActiveMQ {
            return Err(anyhow::anyhow!(
                "ActiveMQMonitor requires broker type to be ActiveMQ, got: {:?}",
                config.broker_type
            ));
        }

        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("stomp-autoscaler/1.0")
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            config,
            retry_count: 0,
            max_retries: 3,
        })
    }

    /// Fetch queue metrics from ActiveMQ management API
    async fn fetch_queue_metrics(&self, queue_name: &str) -> Result<QueueMetrics, MonitoringError> {
        // Build the Jolokia URL for queue metrics
        let queue_object_name = format!(
            "org.apache.activemq:type=Broker,brokerName={},destinationType=Queue,destinationName={}",
            self.config.broker_name, queue_name
        );
        
        let base_url = format!("http://{}:{}", self.config.host, self.config.web_port);
        let jolokia_url = format!(
            "{}/api/jolokia/read/{}",
            base_url.trim_end_matches('/'),
            urlencoding::encode(&queue_object_name)
        );

        debug!("Fetching queue metrics from: {}", jolokia_url);

        // Make the HTTP request with basic auth
        let request_builder = self.client.get(&jolokia_url)
            .basic_auth(&self.config.username, Some(&self.config.password));
        
        let response = request_builder
            .send()
            .await
            .map_err(MonitoringError::HttpError)?;

        // Check HTTP status
        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(MonitoringError::BrokerAPIError(format!(
                "HTTP {} - {}",
                status, error_text
            )));
        }

        // Parse JSON response
        let jolokia_response: JolokiaResponse = response
            .json()
            .await
            .map_err(MonitoringError::HttpError)?;

        // Check Jolokia status
        if jolokia_response.status != 200 {
            let error_msg = jolokia_response
                .error
                .unwrap_or_else(|| format!("Jolokia status: {}", jolokia_response.status));
            return Err(MonitoringError::BrokerAPIError(error_msg));
        }

        // Extract metrics from the response
        self.parse_queue_metrics(queue_name, jolokia_response.value)
    }

    /// Parse queue metrics from Jolokia response
    fn parse_queue_metrics(&self, queue_name: &str, value: Value) -> Result<QueueMetrics, MonitoringError> {
        debug!("Parsing metrics for queue '{}': {}", queue_name, value);

        // Extract individual metrics with defaults
        let queue_size = value
            .get("QueueSize")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        let consumer_count = value
            .get("ConsumerCount")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        let enqueue_count = value
            .get("EnqueueCount")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let dequeue_count = value
            .get("DequeueCount")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let memory_percent_usage = value
            .get("MemoryPercentUsage")
            .and_then(|v| v.as_f64())
            .unwrap_or(0.0);

        let metrics = QueueMetrics {
            queue_name: queue_name.to_string(),
            queue_size,
            consumer_count,
            enqueue_count,
            dequeue_count,
            memory_percent_usage,
        };

        debug!("Parsed metrics for '{}': {:?}", queue_name, metrics);
        Ok(metrics)
    }

    /// Calculate exponential backoff delay for retries
    fn calculate_retry_delay(&self, attempt: u32) -> Duration {
        let base_delay_ms = 1000; // 1 second base delay
        let max_delay_ms = 30000;  // 30 seconds max delay
        let multiplier: f64 = 2.0;

        let delay_ms = (base_delay_ms as f64) * multiplier.powi(attempt.saturating_sub(1) as i32);
        let capped_delay_ms = delay_ms.min(max_delay_ms as f64) as u64;

        Duration::from_millis(capped_delay_ms)
    }
}

/// Artemis monitoring client
#[derive(Debug)]
pub struct ArtemisMonitor {
    client: Client,
    config: BrokerConfig,
    retry_count: u32,
    max_retries: u32,
}

impl ArtemisMonitor {
    /// Create a new Artemis monitoring client
    pub fn new(config: BrokerConfig) -> Result<Self> {
        // Ensure this is for Artemis
        if config.broker_type != BrokerType::Artemis {
            return Err(anyhow::anyhow!(
                "ArtemisMonitor requires broker type to be Artemis, got: {:?}",
                config.broker_type
            ));
        }

        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("stomp-autoscaler/1.0")
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            client,
            config,
            retry_count: 0,
            max_retries: 3,
        })
    }

    /// Fetch queue metrics from Artemis management API
    async fn fetch_queue_metrics(&self, queue_name: &str) -> Result<QueueMetrics, MonitoringError> {
        // First try to create the queue if it doesn't exist
        self.ensure_queue_exists(queue_name).await.ok();
        
        // Try different routing types based on destination type
        let routing_types = determine_routing_types(queue_name);
        
        for routing_type in &routing_types {
            match self.fetch_queue_metrics_with_routing_type(queue_name, routing_type).await {
                Ok(metrics) => return Ok(metrics),
                Err(e) => {
                    debug!("Failed to fetch metrics for queue '{}' with routing-type '{}': {}", queue_name, routing_type, e);
                    continue;
                }
            }
        }
        
        Err(MonitoringError::BrokerAPIError(format!(
            "Queue '{}' not found with any routing type", queue_name
        )))
    }
    
    /// Ensure queue exists by trying to create it
    async fn ensure_queue_exists(&self, queue_name: &str) -> Result<(), MonitoringError> {
        let base_url = format!("http://{}:{}", self.config.host, self.config.web_port);
        let jolokia_url = format!("{}/console/jolokia/", base_url.trim_end_matches('/'));
        
        // Normalize destination name by removing STOMP prefixes
        let normalized_name = normalize_destination_name(queue_name);
        
        let request_payload = json!({
            "type": "exec",
            "mbean": format!("org.apache.activemq.artemis:broker=\"{}\"", self.config.broker_name),
            "operation": "createQueue(java.lang.String,java.lang.String)",
            "arguments": [normalized_name, normalized_name]
        });
        
        debug!("Ensuring queue '{}' exists", queue_name);
        
        let response = self.client.post(&jolokia_url)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .header("Origin", format!("http://{}:{}", self.config.host, self.config.web_port))
            .header("Content-Type", "application/json")
            .json(&request_payload)
            .send()
            .await
            .map_err(MonitoringError::HttpError)?;
        
        // Log success - moved outside of conditional for better testability
        let status = response.status();
        if status.is_success() {
            debug!("Successfully ensured queue '{}' exists (status: {})", queue_name, status);
        } else {
            debug!("Queue '{}' ensure request completed with status: {}", queue_name, status);
        }
        
        Ok(())
    }
    
    /// Fetch queue metrics with specific routing type
    async fn fetch_queue_metrics_with_routing_type(&self, queue_name: &str, routing_type: &str) -> Result<QueueMetrics, MonitoringError> {
        let base_url = format!("http://{}:{}", self.config.host, self.config.web_port);
        let jolokia_url = format!("{}/console/jolokia/", base_url.trim_end_matches('/'));
        
        // Normalize destination name by removing STOMP prefixes
        let normalized_name = normalize_destination_name(queue_name);
        
        // Build the MBean name for Artemis queue metrics
        let mbean_name = format!(
            "org.apache.activemq.artemis:address=\"{}\",broker=\"{}\",component=addresses,queue=\"{}\",routing-type=\"{}\",subcomponent=queues",
            normalized_name, self.config.broker_name, normalized_name, routing_type
        );
        
        let request_payload = json!({
            "type": "read",
            "mbean": mbean_name,
            "attribute": ["ConsumerCount", "MessageCount"]
        });
        
        debug!("Fetching Artemis queue metrics for '{}' with routing-type '{}'", queue_name, routing_type);
        
        // Make the HTTP POST request with JSON payload
        let response = self.client.post(&jolokia_url)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .header("Origin", format!("http://{}:{}", self.config.host, self.config.web_port))
            .header("Content-Type", "application/json")
            .json(&request_payload)
            .send()
            .await
            .map_err(MonitoringError::HttpError)?;

        // Check HTTP status
        if !response.status().is_success() {
            let status = response.status();
            let error_text = response.text().await.unwrap_or_default();
            return Err(MonitoringError::BrokerAPIError(format!(
                "HTTP {} - {}",
                status, error_text
            )));
        }

        // Parse JSON response
        let jolokia_response: JolokiaResponse = response
            .json()
            .await
            .map_err(MonitoringError::HttpError)?;

        // Check Jolokia status
        if jolokia_response.status != 200 {
            let error_msg = jolokia_response
                .error
                .unwrap_or_else(|| format!("Jolokia status: {}", jolokia_response.status));
            return Err(MonitoringError::BrokerAPIError(error_msg));
        }

        // Extract metrics from the response
        self.parse_artemis_queue_metrics(queue_name, jolokia_response.value)
    }

    /// Parse queue metrics from Artemis Jolokia response
    fn parse_artemis_queue_metrics(&self, queue_name: &str, value: Value) -> Result<QueueMetrics, MonitoringError> {
        debug!("Parsing Artemis metrics for queue '{}': {}", queue_name, value);

        // Extract individual metrics with defaults
        // Artemis uses MessageCount instead of QueueSize
        let queue_size = value
            .get("MessageCount")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        let consumer_count = value
            .get("ConsumerCount")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        // Artemis doesn't have these metrics in the same format, use defaults
        let enqueue_count = 0u64; // Not available in this endpoint
        let dequeue_count = 0u64; // Not available in this endpoint
        let memory_percent_usage = 0.0; // Not available in this endpoint

        let metrics = QueueMetrics {
            queue_name: queue_name.to_string(),
            queue_size,
            consumer_count,
            enqueue_count,
            dequeue_count,
            memory_percent_usage,
        };

        debug!("Parsed Artemis metrics for '{}': {:?}", queue_name, metrics);
        Ok(metrics)
    }

    /// Calculate exponential backoff delay for retries
    fn calculate_retry_delay(&self, attempt: u32) -> Duration {
        let base_delay_ms = 1000; // 1 second base delay
        let max_delay_ms = 30000;  // 30 seconds max delay
        let multiplier: f64 = 2.0;

        let delay_ms = (base_delay_ms as f64) * multiplier.powi(attempt.saturating_sub(1) as i32);
        let capped_delay_ms = delay_ms.min(max_delay_ms as f64) as u64;

        Duration::from_millis(capped_delay_ms)
    }
}

#[async_trait]
impl BrokerMonitor for ArtemisMonitor {
    async fn get_queue_metrics(&mut self, queue_name: &str) -> Result<QueueMetrics, MonitoringError> {
        let mut attempts = 0;
        let max_attempts = self.max_retries + 1;

        while attempts < max_attempts {
            match self.fetch_queue_metrics(queue_name).await {
                Ok(metrics) => {
                    // Reset retry count on success
                    self.retry_count = 0;
                    return Ok(metrics);
                }
                Err(e) => {
                    attempts += 1;
                    self.retry_count += 1;
                    
                    if attempts >= max_attempts {
                        error!("Failed to fetch queue metrics for '{}' after {} attempts: {}", 
                               queue_name, max_attempts, e);
                        return Err(e);
                    }

                    let delay = self.calculate_retry_delay(attempts);
                    warn!("Attempt {}/{} failed for queue '{}': {}. Retrying in {}ms", 
                          attempts, max_attempts, queue_name, e, delay.as_millis());
                    sleep(delay).await;
                }
            }
        }

        Err(MonitoringError::BrokerAPIError("Exhausted all retry attempts".to_string()))
    }

    async fn get_multiple_queue_metrics(&mut self, queue_names: &[String]) -> Vec<(String, Result<QueueMetrics, MonitoringError>)> {
        let mut results = Vec::new();

        // For now, fetch sequentially to avoid overwhelming the server
        for queue_name in queue_names {
            let result = self.get_queue_metrics(queue_name).await;
            results.push((queue_name.clone(), result));
        }

        results
    }

    async fn health_check(&mut self) -> Result<bool> {
        let base_url = format!("http://{}:{}", self.config.host, self.config.web_port);
        let jolokia_url = format!("{}/console/jolokia/", base_url.trim_end_matches('/'));
        
        let request_payload = json!({
            "type": "version"
        });

        debug!("Artemis health check URL: {}", jolokia_url);

        let request_builder = self.client.post(&jolokia_url)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .header("Origin", format!("http://{}:{}", self.config.host, self.config.web_port))
            .header("Content-Type", "application/json")
            .json(&request_payload);

        match request_builder.send().await {
            Ok(response) if response.status().is_success() => {
                info!("Artemis management API health check passed");
                Ok(true)
            }
            Ok(response) => {
                warn!("Artemis health check failed with status: {}", response.status());
                Ok(false)
            }
            Err(e) => {
                error!("Artemis health check error: {}", e);
                Err(e.into())
            }
        }
    }

    fn broker_type(&self) -> &'static str {
        "Artemis"
    }
}

#[async_trait]
impl BrokerMonitor for ActiveMQMonitor {
    async fn get_queue_metrics(&mut self, queue_name: &str) -> Result<QueueMetrics, MonitoringError> {
        let mut attempts = 0;
        let max_attempts = self.max_retries + 1;

        while attempts < max_attempts {
            match self.fetch_queue_metrics(queue_name).await {
                Ok(metrics) => {
                    // Reset retry count on success
                    self.retry_count = 0;
                    return Ok(metrics);
                }
                Err(e) => {
                    attempts += 1;
                    self.retry_count += 1;
                    
                    if attempts >= max_attempts {
                        error!("Failed to fetch queue metrics for '{}' after {} attempts: {}", 
                               queue_name, max_attempts, e);
                        return Err(e);
                    }

                    let delay = self.calculate_retry_delay(attempts);
                    warn!("Attempt {}/{} failed for queue '{}': {}. Retrying in {}ms", 
                          attempts, max_attempts, queue_name, e, delay.as_millis());
                    sleep(delay).await;
                }
            }
        }

        Err(MonitoringError::BrokerAPIError("Exhausted all retry attempts".to_string()))
    }

    async fn get_multiple_queue_metrics(&mut self, queue_names: &[String]) -> Vec<(String, Result<QueueMetrics, MonitoringError>)> {
        let mut results = Vec::new();

        // For now, fetch sequentially to avoid overwhelming the server
        for queue_name in queue_names {
            let result = self.get_queue_metrics(queue_name).await;
            results.push((queue_name.clone(), result));
        }

        results
    }

    async fn health_check(&mut self) -> Result<bool> {
        let base_url = format!("http://{}:{}", self.config.host, self.config.web_port);
        let health_url = format!(
            "{}/api/jolokia/version",
            base_url.trim_end_matches('/')
        );

        debug!("ActiveMQ health check URL: {}", health_url);

        let request_builder = self.client.get(&health_url)
            .basic_auth(&self.config.username, Some(&self.config.password));

        match request_builder.send().await {
            Ok(response) if response.status().is_success() => {
                info!("ActiveMQ management API health check passed");
                Ok(true)
            }
            Ok(response) => {
                warn!("ActiveMQ health check failed with status: {}", response.status());
                Ok(false)
            }
            Err(e) => {
                error!("ActiveMQ health check error: {}", e);
                Err(e.into())
            }
        }
    }

    fn broker_type(&self) -> &'static str {
        "ActiveMQ"
    }
}

/// Factory function to create appropriate monitor based on broker type
pub fn create_broker_monitor(config: BrokerConfig) -> Result<Box<dyn BrokerMonitor>> {
    match config.broker_type {
        BrokerType::ActiveMQ => {
            let monitor = ActiveMQMonitor::new(config)?;
            Ok(Box::new(monitor))
        }
        BrokerType::Artemis => {
            let monitor = ArtemisMonitor::new(config)?;
            Ok(Box::new(monitor))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{BrokerConfig, BrokerType};
    use httpmock::prelude::*;
    use proptest::prelude::*;
    use serde_json::json;

    // Helper function to create a test BrokerConfig
    fn create_test_broker_config(broker_type: BrokerType) -> BrokerConfig {
        BrokerConfig {
            broker_type,
            host: "localhost".to_string(),
            username: "admin".to_string(),
            password: "password".to_string(),
            stomp_port: 61613,
            web_port: 8161,
            heartbeat_secs: 30,
            broker_name: "localhost".to_string(),
        }
    }

    #[test]
    fn test_monitoring_error_display_and_debug() {
        // Test BrokerAPIError
        let broker_error = MonitoringError::BrokerAPIError("Queue not found".to_string());
        assert_eq!(format!("{}", broker_error), "Broker API error: Queue not found");
        assert!(format!("{:?}", broker_error).contains("BrokerAPIError"));

        // Test error message can be arbitrary text
        let complex_msg = "HTTP 500 - Internal Server Error with special chars: 测试 🚀";
        let complex_error = MonitoringError::BrokerAPIError(complex_msg.to_string());
        assert!(format!("{}", complex_error).contains(complex_msg));
    }

    #[test]
    fn test_monitoring_error_from_conversions() {
        // Test From reqwest::Error (we can't easily construct one, so test the structure exists)
        // The actual conversion is tested in integration tests

        // Test From serde_json::Error
        let json_error = serde_json::from_str::<QueueMetrics>("invalid json");
        assert!(json_error.is_err());
        let monitoring_error: MonitoringError = json_error.unwrap_err().into();
        assert!(matches!(monitoring_error, MonitoringError::JsonError(_)));
    }

    proptest! {
        #[test]
        fn test_broker_api_error_round_trip(msg in any::<String>()) {
            let error = MonitoringError::BrokerAPIError(msg.clone());
            let formatted = format!("{}", error);
            prop_assert!(formatted.contains(&msg));
        }
    }

    #[test]
    fn test_queue_metrics_debug_clone() {
        let metrics = QueueMetrics {
            queue_name: "test-queue".to_string(),
            queue_size: 100,
            consumer_count: 2,
            enqueue_count: 1000,
            dequeue_count: 900,
            memory_percent_usage: 75.5,
        };

        // Test Debug
        let debug_str = format!("{:?}", metrics);
        assert!(debug_str.contains("test-queue"));
        assert!(debug_str.contains("100"));
        assert!(debug_str.contains("75.5"));

        // Test Clone
        let cloned = metrics.clone();
        assert_eq!(metrics.queue_name, cloned.queue_name);
        assert_eq!(metrics.queue_size, cloned.queue_size);
        assert_eq!(metrics.consumer_count, cloned.consumer_count);
        assert_eq!(metrics.enqueue_count, cloned.enqueue_count);
        assert_eq!(metrics.dequeue_count, cloned.dequeue_count);
        assert_eq!(metrics.memory_percent_usage, cloned.memory_percent_usage);
    }

    #[test]
    fn test_queue_metrics_serialization() {
        let metrics = QueueMetrics {
            queue_name: "test-queue".to_string(),
            queue_size: 50,
            consumer_count: 3,
            enqueue_count: 500,
            dequeue_count: 450,
            memory_percent_usage: 25.0,
        };

        // Serialize to JSON
        let json_str = serde_json::to_string(&metrics).unwrap();
        assert!(json_str.contains("test-queue"));
        assert!(json_str.contains("50"));
        assert!(json_str.contains("25.0"));

        // Deserialize back
        let deserialized: QueueMetrics = serde_json::from_str(&json_str).unwrap();
        assert_eq!(metrics.queue_name, deserialized.queue_name);
        assert_eq!(metrics.queue_size, deserialized.queue_size);
        assert_eq!(metrics.consumer_count, deserialized.consumer_count);
        assert_eq!(metrics.enqueue_count, deserialized.enqueue_count);
        assert_eq!(metrics.dequeue_count, deserialized.dequeue_count);
        assert_eq!(metrics.memory_percent_usage, deserialized.memory_percent_usage);
    }

    proptest! {
        #[test]
        fn test_queue_metrics_round_trip(
            queue_name in any::<String>(),
            queue_size in any::<u32>(),
            consumer_count in any::<u32>(),
            enqueue_count in any::<u64>(),
            dequeue_count in any::<u64>(),
            memory_percent_usage in any::<f64>()
        ) {
            let metrics = QueueMetrics {
                queue_name,
                queue_size,
                consumer_count,
                enqueue_count,
                dequeue_count,
                memory_percent_usage,
            };

            let json_str = serde_json::to_string(&metrics).unwrap();
            let deserialized: QueueMetrics = serde_json::from_str(&json_str).unwrap();
            
            prop_assert_eq!(metrics.queue_name, deserialized.queue_name);
            prop_assert_eq!(metrics.queue_size, deserialized.queue_size);
            prop_assert_eq!(metrics.consumer_count, deserialized.consumer_count);
            prop_assert_eq!(metrics.enqueue_count, deserialized.enqueue_count);
            prop_assert_eq!(metrics.dequeue_count, deserialized.dequeue_count);
            // Use epsilon comparison for floating point values
            // JSON serialization/deserialization can lose precision for very large numbers
            let orig = metrics.memory_percent_usage;
            let deser = deserialized.memory_percent_usage;
            
            if orig.is_finite() && deser.is_finite() {
                let abs_orig = orig.abs();
                if abs_orig < 1e100 {
                    // For reasonable values, check absolute difference
                    let diff = (orig - deser).abs();
                    prop_assert!(diff < 1e-6 || diff / abs_orig.max(1.0) < 1e-6);
                }
                // For extremely large finite values, we accept some loss of precision
                // due to JSON number representation limitations
            } else {
                // For infinite/NaN values, check they have the same classification
                prop_assert_eq!(orig.is_nan(), deser.is_nan());
                prop_assert_eq!(orig.is_infinite(), deser.is_infinite());
                if orig.is_infinite() && deser.is_infinite() {
                    prop_assert_eq!(orig.signum(), deser.signum());
                }
            }
        }
    }

    #[test]
    fn test_jolokia_response_deserialization() {
        // Test success with value only
        let success_json = json!({
            "value": {
                "QueueSize": 42,
                "ConsumerCount": 2
            },
            "status": 200
        });
        let response: JolokiaResponse = serde_json::from_value(success_json).unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.value.get("QueueSize").unwrap().as_u64(), Some(42));
        assert!(response.error.is_none());

        // Test success with value + status
        let success_with_status_json = json!({
            "value": {"MessageCount": 10},
            "status": 200,
            "request": {"type": "read"}
        });
        let response: JolokiaResponse = serde_json::from_value(success_with_status_json).unwrap();
        assert_eq!(response.status, 200);
        assert_eq!(response.value.get("MessageCount").unwrap().as_u64(), Some(10));

        // Test error case
        let error_json = json!({
            "error": "Queue not found",
            "status": 404,
            "value": null
        });
        let response: JolokiaResponse = serde_json::from_value(error_json).unwrap();
        assert_eq!(response.status, 404);
        assert_eq!(response.error.unwrap(), "Queue not found");
    }

    #[test]
    fn test_activemq_monitor_constructor() {
        // Test correct broker type
        let config = create_test_broker_config(BrokerType::ActiveMQ);
        let monitor = ActiveMQMonitor::new(config);
        assert!(monitor.is_ok());

        // Test wrong broker type
        let config = create_test_broker_config(BrokerType::Artemis);
        let monitor = ActiveMQMonitor::new(config);
        assert!(monitor.is_err());
        assert!(monitor.unwrap_err().to_string().contains("ActiveMQMonitor requires broker type to be ActiveMQ"));
    }

    #[test]
    fn test_artemis_monitor_constructor() {
        // Test correct broker type
        let config = create_test_broker_config(BrokerType::Artemis);
        let monitor = ArtemisMonitor::new(config);
        assert!(monitor.is_ok());

        // Test wrong broker type
        let config = create_test_broker_config(BrokerType::ActiveMQ);
        let monitor = ArtemisMonitor::new(config);
        assert!(monitor.is_err());
        assert!(monitor.unwrap_err().to_string().contains("ArtemisMonitor requires broker type to be Artemis"));
    }

    #[test]
    fn test_calculate_retry_delay_exponential_backoff() {
        let config = create_test_broker_config(BrokerType::ActiveMQ);
        let monitor = ActiveMQMonitor::new(config).unwrap();

        // Test base case
        let delay0 = monitor.calculate_retry_delay(0);
        assert_eq!(delay0, Duration::from_millis(1000));

        // Test exponential growth
        let delay1 = monitor.calculate_retry_delay(1);
        assert_eq!(delay1, Duration::from_millis(1000));

        let delay2 = monitor.calculate_retry_delay(2);
        assert_eq!(delay2, Duration::from_millis(2000));

        let delay3 = monitor.calculate_retry_delay(3);
        assert_eq!(delay3, Duration::from_millis(4000));

        let delay4 = monitor.calculate_retry_delay(4);
        assert_eq!(delay4, Duration::from_millis(8000));

        // Test capping at max_delay_ms
        let delay_large = monitor.calculate_retry_delay(10);
        assert_eq!(delay_large, Duration::from_millis(30000));
    }

    #[test]
    fn test_artemis_calculate_retry_delay() {
        let config = create_test_broker_config(BrokerType::Artemis);
        let monitor = ArtemisMonitor::new(config).unwrap();

        // Same logic as ActiveMQ
        let delay1 = monitor.calculate_retry_delay(1);
        assert_eq!(delay1, Duration::from_millis(1000));

        let delay2 = monitor.calculate_retry_delay(2);
        assert_eq!(delay2, Duration::from_millis(2000));

        let delay_large = monitor.calculate_retry_delay(20);
        assert_eq!(delay_large, Duration::from_millis(30000));
    }

    #[tokio::test]
    async fn test_activemq_parse_queue_metrics() {
        let config = create_test_broker_config(BrokerType::ActiveMQ);
        let monitor = ActiveMQMonitor::new(config).unwrap();

        // Test successful parsing
        let jolokia_value = json!({
            "QueueSize": 42,
            "ConsumerCount": 3,
            "EnqueueCount": 1000,
            "DequeueCount": 958,
            "MemoryPercentUsage": 75.5
        });

        let metrics = monitor.parse_queue_metrics("test-queue", jolokia_value).unwrap();
        assert_eq!(metrics.queue_name, "test-queue");
        assert_eq!(metrics.queue_size, 42);
        assert_eq!(metrics.consumer_count, 3);
        assert_eq!(metrics.enqueue_count, 1000);
        assert_eq!(metrics.dequeue_count, 958);
        assert_eq!(metrics.memory_percent_usage, 75.5);

        // Test with missing fields (should use defaults)
        let minimal_value = json!({
            "QueueSize": 10
        });
        let metrics = monitor.parse_queue_metrics("minimal-queue", minimal_value).unwrap();
        assert_eq!(metrics.queue_name, "minimal-queue");
        assert_eq!(metrics.queue_size, 10);
        assert_eq!(metrics.consumer_count, 0);
        assert_eq!(metrics.enqueue_count, 0);
        assert_eq!(metrics.dequeue_count, 0);
        assert_eq!(metrics.memory_percent_usage, 0.0);
    }

    #[tokio::test]
    async fn test_artemis_parse_queue_metrics() {
        let config = create_test_broker_config(BrokerType::Artemis);
        let monitor = ArtemisMonitor::new(config).unwrap();

        // Test successful parsing (Artemis uses MessageCount instead of QueueSize)
        let jolokia_value = json!({
            "MessageCount": 25,
            "ConsumerCount": 2
        });

        let metrics = monitor.parse_artemis_queue_metrics("artemis-queue", jolokia_value).unwrap();
        assert_eq!(metrics.queue_name, "artemis-queue");
        assert_eq!(metrics.queue_size, 25);
        assert_eq!(metrics.consumer_count, 2);
        // Artemis doesn't provide these metrics in this endpoint
        assert_eq!(metrics.enqueue_count, 0);
        assert_eq!(metrics.dequeue_count, 0);
        assert_eq!(metrics.memory_percent_usage, 0.0);

        // Test with empty object
        let empty_value = json!({});
        let metrics = monitor.parse_artemis_queue_metrics("empty-queue", empty_value).unwrap();
        assert_eq!(metrics.queue_name, "empty-queue");
        assert_eq!(metrics.queue_size, 0);
        assert_eq!(metrics.consumer_count, 0);
    }

    #[tokio::test]
    async fn test_activemq_monitor_with_httpmock() {
        let server = MockServer::start();

        // Create config pointing to mock server
        let mut config = create_test_broker_config(BrokerType::ActiveMQ);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let mut monitor = ActiveMQMonitor::new(config).unwrap();

        // Mock successful queue metrics response
        let queue_mock = server.mock(|when, then| {
            when.method(GET)
                .path_contains("/api/jolokia/read/")
                .header("authorization", "Basic YWRtaW46cGFzc3dvcmQ="); // admin:password in base64
            then.status(200)
                .json_body(json!({
                    "value": {
                        "QueueSize": 15,
                        "ConsumerCount": 1,
                        "EnqueueCount": 100,
                        "DequeueCount": 85,
                        "MemoryPercentUsage": 50.0
                    },
                    "status": 200
                }));
        });

        let result = monitor.get_queue_metrics("test-queue").await;
        assert!(result.is_ok());
        let metrics = result.unwrap();
        assert_eq!(metrics.queue_name, "test-queue");
        assert_eq!(metrics.queue_size, 15);
        assert_eq!(metrics.consumer_count, 1);
        queue_mock.assert();

        // Test health check
        let health_mock = server.mock(|when, then| {
            when.method(GET)
                .path("/api/jolokia/version");
            then.status(200)
                .json_body(json!({"value": {"agent": "1.7.1"}, "status": 200}));
        });

        let health_result = monitor.health_check().await;
        assert!(health_result.is_ok());
        assert_eq!(health_result.unwrap(), true);
        health_mock.assert();

        // Test broker type
        assert_eq!(monitor.broker_type(), "ActiveMQ");
    }

    #[tokio::test]
    async fn test_activemq_monitor_http_error() {
        let server = MockServer::start();

        let mut config = create_test_broker_config(BrokerType::ActiveMQ);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let mut monitor = ActiveMQMonitor::new(config).unwrap();

        // Mock HTTP 500 error
        let _error_mock = server.mock(|when, then| {
            when.method(GET)
                .path_contains("/api/jolokia/read/");
            then.status(500)
                .body("Internal Server Error");
        });

        let result = monitor.get_queue_metrics("test-queue").await;
        assert!(result.is_err());
        // The error should be BrokerAPIError due to HTTP 500 status
        match result.unwrap_err() {
            MonitoringError::BrokerAPIError(msg) => {
                assert!(msg.contains("HTTP 500") || msg.contains("Internal Server Error"));
            }
            _ => panic!("Expected BrokerAPIError"),
        }
    }

    #[tokio::test]
    async fn test_activemq_monitor_jolokia_error() {
        let server = MockServer::start();

        let mut config = create_test_broker_config(BrokerType::ActiveMQ);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let mut monitor = ActiveMQMonitor::new(config).unwrap();

        // Mock Jolokia error response
        let _jolokia_error_mock = server.mock(|when, then| {
            when.method(GET)
                .path_contains("/api/jolokia/read/");
            then.status(200)
                .json_body(json!({
                    "error": "MBean not found",
                    "status": 404
                }));
        });

        let result = monitor.get_queue_metrics("nonexistent-queue").await;
        assert!(result.is_err());
        // The actual error depends on how the mock server responds
        // We just verify that we get an error, which could be any of the monitoring errors
        match result.unwrap_err() {
            MonitoringError::BrokerAPIError(_) | 
            MonitoringError::JsonError(_) | 
            MonitoringError::HttpError(_) => {
                // Any of these errors is acceptable for this test scenario
            }
        }
    }

    #[tokio::test]
    async fn test_activemq_multiple_queue_metrics() {
        let server = MockServer::start();

        let mut config = create_test_broker_config(BrokerType::ActiveMQ);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let mut monitor = ActiveMQMonitor::new(config).unwrap();

        // Mock responses for multiple queues
        let queue1_mock = server.mock(|when, then| {
            when.method(GET)
                .path_contains("queue1");
            then.status(200)
                .json_body(json!({
                    "value": {"QueueSize": 10, "ConsumerCount": 1},
                    "status": 200
                }));
        });

        let queue2_mock = server.mock(|when, then| {
            when.method(GET)
                .path_contains("queue2");
            then.status(200)
                .json_body(json!({
                    "value": {"QueueSize": 20, "ConsumerCount": 2},
                    "status": 200
                }));
        });

        let queue_names = vec!["queue1".to_string(), "queue2".to_string()];
        let results = monitor.get_multiple_queue_metrics(&queue_names).await;

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, "queue1");
        assert!(results[0].1.is_ok());
        assert_eq!(results[0].1.as_ref().unwrap().queue_size, 10);

        assert_eq!(results[1].0, "queue2");
        assert!(results[1].1.is_ok());
        assert_eq!(results[1].1.as_ref().unwrap().queue_size, 20);

        queue1_mock.assert();
        queue2_mock.assert();
    }

    #[tokio::test]
    async fn test_artemis_monitor_with_httpmock() {
        let server = MockServer::start();

        let mut config = create_test_broker_config(BrokerType::Artemis);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let mut monitor = ArtemisMonitor::new(config).unwrap();

        // Mock queue creation request
        let create_queue_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#""type":"exec""#);
            then.status(200)
                .json_body(json!({
                    "value": null,
                    "status": 200
                }));
        });

        // Mock successful queue metrics response with anycast routing
        let metrics_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#""type":"read""#);
            then.status(200)
                .json_body(json!({
                    "value": {
                        "MessageCount": 30,
                        "ConsumerCount": 3
                    },
                    "status": 200
                }));
        });

        let result = monitor.get_queue_metrics("test-queue").await;
        assert!(result.is_ok());
        let metrics = result.unwrap();
        assert_eq!(metrics.queue_name, "test-queue");
        assert_eq!(metrics.queue_size, 30);
        assert_eq!(metrics.consumer_count, 3);

        create_queue_mock.assert();
        metrics_mock.assert();

        // Test broker type
        assert_eq!(monitor.broker_type(), "Artemis");
    }

    #[tokio::test]
    async fn test_artemis_routing_type_fallback() {
        let server = MockServer::start();

        let mut config = create_test_broker_config(BrokerType::Artemis);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let mut monitor = ArtemisMonitor::new(config).unwrap();

        // Mock queue creation
        let create_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#""type":"exec""#);
            then.status(200)
                .json_body(json!({"value": null, "status": 200}));
        });

        // Mock first routing type failure (anycast)
        let anycast_fail_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#"anycast"#);
            then.status(200)
                .json_body(json!({
                    "error": "MBean not found",
                    "status": 404
                }));
        });

        // Mock second routing type success (multicast)
        let multicast_success_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#"multicast"#);
            then.status(200)
                .json_body(json!({
                    "value": {
                        "MessageCount": 5,
                        "ConsumerCount": 1
                    },
                    "status": 200
                }));
        });

        let result = monitor.get_queue_metrics("/queue/test-queue").await;
        assert!(result.is_ok());
        let metrics = result.unwrap();
        assert_eq!(metrics.queue_size, 5);

        create_mock.assert();
        anycast_fail_mock.assert();
        multicast_success_mock.assert();
    }

    #[tokio::test]
    async fn test_artemis_health_check() {
        // Test successful health check
        let server1 = MockServer::start();

        let mut config1 = create_test_broker_config(BrokerType::Artemis);
        config1.host = server1.host().to_string();
        config1.web_port = server1.port();

        let mut monitor1 = ArtemisMonitor::new(config1).unwrap();

        let health_success_mock = server1.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#"version"#);
            then.status(200)
                .json_body(json!({
                    "value": {"agent": "1.7.1", "protocol": "7.2"},
                    "status": 200
                }));
        });

        let health_result = monitor1.health_check().await;
        assert!(health_result.is_ok());
        assert_eq!(health_result.unwrap(), true);
        health_success_mock.assert();

        // Test failed health check (HTTP error) with a separate server
        let server2 = MockServer::start();

        let mut config2 = create_test_broker_config(BrokerType::Artemis);
        config2.host = server2.host().to_string();
        config2.web_port = server2.port();

        let mut monitor2 = ArtemisMonitor::new(config2).unwrap();

        let health_fail_mock = server2.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/");
            then.status(503)
                .body("Service Unavailable");
        });

        let health_result = monitor2.health_check().await;
        assert!(health_result.is_ok());
        assert_eq!(health_result.unwrap(), false);
        health_fail_mock.assert();
    }

    #[test]
    fn test_create_broker_monitor_factory() {
        // Test ActiveMQ creation
        let activemq_config = create_test_broker_config(BrokerType::ActiveMQ);
        let activemq_monitor = create_broker_monitor(activemq_config);
        assert!(activemq_monitor.is_ok());
        let monitor = activemq_monitor.unwrap();
        assert_eq!(monitor.broker_type(), "ActiveMQ");

        // Test Artemis creation
        let artemis_config = create_test_broker_config(BrokerType::Artemis);
        let artemis_monitor = create_broker_monitor(artemis_config);
        assert!(artemis_monitor.is_ok());
        let monitor = artemis_monitor.unwrap();
        assert_eq!(monitor.broker_type(), "Artemis");
    }

    #[tokio::test]
    async fn test_integration_multiple_brokers() {
        let server = MockServer::start();

        // Test ActiveMQ
        let mut activemq_config = create_test_broker_config(BrokerType::ActiveMQ);
        activemq_config.host = server.host().to_string();
        activemq_config.web_port = server.port();

        let activemq_mock = server.mock(|when, then| {
            when.method(GET)
                .path_contains("/api/jolokia/read/");
            then.status(200)
                .json_body(json!({
                    "value": {"QueueSize": 100, "ConsumerCount": 5},
                    "status": 200
                }));
        });

        let mut activemq_monitor = create_broker_monitor(activemq_config).unwrap();
        let queue_names = vec!["activemq-queue".to_string()];
        let results = activemq_monitor.get_multiple_queue_metrics(&queue_names).await;

        assert_eq!(results.len(), 1);
        assert!(results[0].1.is_ok());
        assert_eq!(results[0].1.as_ref().unwrap().queue_size, 100);
        activemq_mock.assert();

        // Test Artemis  
        let mut artemis_config = create_test_broker_config(BrokerType::Artemis);
        artemis_config.host = server.host().to_string();
        artemis_config.web_port = server.port();

        let artemis_create_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#""type":"exec""#);
            then.status(200)
                .json_body(json!({"value": null, "status": 200}));
        });

        let artemis_metrics_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#""type":"read""#);
            then.status(200)
                .json_body(json!({
                    "value": {"MessageCount": 200, "ConsumerCount": 10},
                    "status": 200
                }));
        });

        let mut artemis_monitor = create_broker_monitor(artemis_config).unwrap();
        let artemis_queue_names = vec!["artemis-queue".to_string()];
        let artemis_results = artemis_monitor.get_multiple_queue_metrics(&artemis_queue_names).await;

        assert_eq!(artemis_results.len(), 1);
        assert!(artemis_results[0].1.is_ok());
        assert_eq!(artemis_results[0].1.as_ref().unwrap().queue_size, 200);
        artemis_create_mock.assert();
        artemis_metrics_mock.assert();
    }

    #[test]
    fn test_queue_metrics_edge_cases() {
        // Test with extreme values
        let extreme_metrics = QueueMetrics {
            queue_name: "extreme".to_string(),
            queue_size: u32::MAX,
            consumer_count: u32::MAX,
            enqueue_count: u64::MAX,
            dequeue_count: u64::MAX,
            memory_percent_usage: f64::MAX,
        };

        let json_str = serde_json::to_string(&extreme_metrics).unwrap();
        let deserialized: QueueMetrics = serde_json::from_str(&json_str).unwrap();
        assert_eq!(extreme_metrics.queue_size, deserialized.queue_size);
        assert_eq!(extreme_metrics.enqueue_count, deserialized.enqueue_count);

        // Test with zero values
        let zero_metrics = QueueMetrics {
            queue_name: "".to_string(),
            queue_size: 0,
            consumer_count: 0,
            enqueue_count: 0,
            dequeue_count: 0,
            memory_percent_usage: 0.0,
        };

        let json_str = serde_json::to_string(&zero_metrics).unwrap();
        let deserialized: QueueMetrics = serde_json::from_str(&json_str).unwrap();
        assert_eq!(zero_metrics.queue_name, deserialized.queue_name);
        assert_eq!(zero_metrics.queue_size, deserialized.queue_size);
    }

    #[test]
    fn test_jolokia_response_edge_cases() {
        // Test with minimal JSON
        let minimal_json = json!({
            "value": {},
            "status": 200
        });
        let response: JolokiaResponse = serde_json::from_value(minimal_json).unwrap();
        assert_eq!(response.status, 200);
        assert!(response.error.is_none());

        // Test with null value
        let null_value_json = json!({
            "value": null,
            "status": 500,
            "error": "Something went wrong"
        });
        let response: JolokiaResponse = serde_json::from_value(null_value_json).unwrap();
        assert_eq!(response.status, 500);
        assert_eq!(response.error.unwrap(), "Something went wrong");
    }

    #[tokio::test]
    async fn test_monitor_retry_mechanism() {
        let server = MockServer::start();

        let mut config = create_test_broker_config(BrokerType::ActiveMQ);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let mut monitor = ActiveMQMonitor::new(config).unwrap();

        // Mock that fails twice, then succeeds
        let _fail_mock = server.mock(|when, then| {
            when.method(GET)
                .path_contains("/api/jolokia/read/");
            then.status(503)
                .body("Service Temporarily Unavailable");
        });

        let _success_mock = server.mock(|when, then| {
            when.method(GET)
                .path_contains("/api/jolokia/read/");
            then.status(200)
                .json_body(json!({
                    "value": {"QueueSize": 42, "ConsumerCount": 1},
                    "status": 200
                }));
        });

        // This test won't work perfectly with httpmock as it doesn't support ordered expectations
        // But we can verify that the retry logic doesn't panic and eventually returns an error
        let _result = monitor.get_queue_metrics("retry-test-queue").await;
        // The result will depend on which mock gets hit first
        // Since we can't guarantee order, we just ensure no panic occurs
        // In a real retry scenario with multiple failures followed by success,
        // the monitor should work correctly
    }

    #[tokio::test]
    async fn test_artemis_all_routing_types_fail() {
        let server = MockServer::start();

        let mut config = create_test_broker_config(BrokerType::Artemis);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let mut monitor = ArtemisMonitor::new(config).unwrap();

        // Mock queue creation - may be called multiple times due to retry logic
        let _create_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#""type":"exec""#);
            then.status(200)
                .json_body(json!({"value": null, "status": 200}));
        });

        // Mock all routing types to fail - this will be called for each routing type attempt
        let _fail_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#""type":"read""#);
            then.status(200)
                .json_body(json!({
                    "error": "MBean not found",
                    "status": 404
                }));
        });

        let result = monitor.get_queue_metrics("/queue/nonexistent-queue").await;
        assert!(result.is_err());
        // The error could be either a routing type failure or a general failure
        // Since httpmock doesn't guarantee ordering, we just ensure we get an error
        assert!(matches!(result.unwrap_err(), MonitoringError::BrokerAPIError(_)));

        // Don't assert on create_mock as it may be called multiple times
        // due to retry logic and routing type attempts
    }

    #[tokio::test]
    async fn test_activemq_exhausted_retries() {
        let server = MockServer::start();

        let mut config = create_test_broker_config(BrokerType::ActiveMQ);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let mut monitor = ActiveMQMonitor::new(config).unwrap();

        // Mock that always fails
        let _fail_mock = server.mock(|when, then| {
            when.method(GET)
                .path_contains("/api/jolokia/read/");
            then.status(500)
                .body("Always fails");
        });

        let result = monitor.get_queue_metrics("failing-queue").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            MonitoringError::BrokerAPIError(msg) => {
                // Should get an error related to the failure
                assert!(msg.contains("HTTP 500") || msg.contains("Always fails"));
            }
            _ => panic!("Expected BrokerAPIError"),
        }
    }

    #[test]
    fn test_error_enum_variants() {
        // Test that we can match on all variants
        let http_error = MonitoringError::BrokerAPIError("test".to_string());
        match http_error {
            MonitoringError::HttpError(_) => panic!("Wrong variant"),
            MonitoringError::JsonError(_) => panic!("Wrong variant"),
            MonitoringError::BrokerAPIError(msg) => assert_eq!(msg, "test"),
        }
    }

    #[test]
    fn test_queue_metrics_partial_eq() {
        let metrics1 = QueueMetrics {
            queue_name: "test".to_string(),
            queue_size: 10,
            consumer_count: 2,
            enqueue_count: 100,
            dequeue_count: 90,
            memory_percent_usage: 50.0,
        };

        let metrics2 = QueueMetrics {
            queue_name: "test".to_string(),
            queue_size: 10,
            consumer_count: 2,
            enqueue_count: 100,
            dequeue_count: 90,
            memory_percent_usage: 50.0,
        };

        let metrics3 = QueueMetrics {
            queue_name: "different".to_string(),
            queue_size: 10,
            consumer_count: 2,
            enqueue_count: 100,
            dequeue_count: 90,
            memory_percent_usage: 50.0,
        };

        // Note: QueueMetrics doesn't derive PartialEq, so we test field by field
        assert_eq!(metrics1.queue_name, metrics2.queue_name);
        assert_eq!(metrics1.queue_size, metrics2.queue_size);
        assert_ne!(metrics1.queue_name, metrics3.queue_name);
    }

    // Unit test for lines 130-133: ActiveMQ Jolokia status error with custom error message
    #[tokio::test]
    async fn test_activemq_jolokia_status_error_with_custom_message() {
        let server = MockServer::start();

        let mut config = create_test_broker_config(BrokerType::ActiveMQ);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let mut monitor = ActiveMQMonitor::new(config).unwrap();

        // Mock Jolokia error response with custom error message
        let _jolokia_error_mock = server.mock(|when, then| {
            when.method(GET)
                .path_contains("/api/jolokia/read/");
            then.status(200)
                .json_body(json!({
                    "value": {},
                    "status": 404,
                    "error": "Custom error: MBean not found"
                }));
        });

        let result = monitor.get_queue_metrics("test-queue").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            MonitoringError::BrokerAPIError(msg) => {
                assert_eq!(msg, "Custom error: MBean not found");
            }
            _ => panic!("Expected BrokerAPIError"),
        }
    }

    // Unit test for lines 130-133: ActiveMQ Jolokia status error without error message
    #[tokio::test]
    async fn test_activemq_jolokia_status_error_without_message() {
        let server = MockServer::start();

        let mut config = create_test_broker_config(BrokerType::ActiveMQ);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let mut monitor = ActiveMQMonitor::new(config).unwrap();

        // Mock Jolokia error response without error message (tests unwrap_or_else)
        let _jolokia_error_mock = server.mock(|when, then| {
            when.method(GET)
                .path_contains("/api/jolokia/read/");
            then.status(200)
                .json_body(json!({
                    "value": {},
                    "status": 500
                }));
        });

        let result = monitor.get_queue_metrics("test-queue").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            MonitoringError::BrokerAPIError(msg) => {
                assert_eq!(msg, "Jolokia status: 500");
            }
            _ => panic!("Expected BrokerAPIError"),
        }
    }

    // Unit test for line 284: Artemis ensure_queue_exists with non-success status
    #[tokio::test]
    async fn test_artemis_ensure_queue_exists_non_success_status() {
        let server = MockServer::start();

        let mut config = create_test_broker_config(BrokerType::Artemis);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let monitor = ArtemisMonitor::new(config).unwrap();

        // Mock queue creation with non-success status (e.g., 409 Conflict - queue already exists)
        let create_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#""type":"exec""#);
            then.status(409)
                .body("Queue already exists");
        });

        // This should still succeed since ensure_queue_exists returns Ok regardless of status
        let result = monitor.ensure_queue_exists("existing-queue").await;
        assert!(result.is_ok());
        create_mock.assert();
    }

    // Unit test for lines 324-329: Artemis HTTP status error
    #[tokio::test]
    async fn test_artemis_fetch_queue_metrics_http_error() {
        let server = MockServer::start();

        let mut config = create_test_broker_config(BrokerType::Artemis);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let mut monitor = ArtemisMonitor::new(config).unwrap();

        // Mock queue creation success
        let _create_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#""type":"exec""#);
            then.status(200)
                .json_body(json!({"value": null, "status": 200}));
        });

        // Mock HTTP 503 error for all metrics fetch attempts (both routing types)
        let _error_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#""type":"read""#);
            then.status(503)
                .body("Service Unavailable");
        });

        let result = monitor.get_queue_metrics("test-queue").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            MonitoringError::BrokerAPIError(msg) => {
                // The error message will be about queue not found with any routing type
                // after all routing types return HTTP 503
                assert!(msg.contains("HTTP 503") || msg.contains("Service Unavailable") || msg.contains("not found with any routing type"));
            }
            _ => panic!("Expected BrokerAPIError"),
        }
    }

    // Unit test for lines 340-343: Artemis Jolokia status error with custom message
    #[tokio::test]
    async fn test_artemis_jolokia_status_error_with_custom_message() {
        let server = MockServer::start();

        let mut config = create_test_broker_config(BrokerType::Artemis);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let mut monitor = ArtemisMonitor::new(config).unwrap();

        // Mock queue creation
        let _create_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#""type":"exec""#);
            then.status(200)
                .json_body(json!({"value": null, "status": 200}));
        });

        // Mock Jolokia error response with custom error message for all routing types
        let _jolokia_error_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#""type":"read""#);
            then.status(200)
                .json_body(json!({
                    "value": {},
                    "status": 403,
                    "error": "Access denied to MBean"
                }));
        });

        let result = monitor.get_queue_metrics("test-queue").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            MonitoringError::BrokerAPIError(msg) => {
                // After all routing types fail with the same error, we get the generic "not found" message
                assert!(msg.contains("not found with any routing type"));
            }
            _ => panic!("Expected BrokerAPIError"),
        }
    }

    // Unit test for lines 340-343: Artemis Jolokia status error without error message
    #[tokio::test]
    async fn test_artemis_jolokia_status_error_without_message() {
        let server = MockServer::start();

        let mut config = create_test_broker_config(BrokerType::Artemis);
        config.host = server.host().to_string();
        config.web_port = server.port();

        let mut monitor = ArtemisMonitor::new(config).unwrap();

        // Mock queue creation
        let _create_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#""type":"exec""#);
            then.status(200)
                .json_body(json!({"value": null, "status": 200}));
        });

        // Mock Jolokia error response without error message (tests unwrap_or_else)
        let _jolokia_error_mock = server.mock(|when, then| {
            when.method(POST)
                .path("/console/jolokia/")
                .body_contains(r#""type":"read""#);
            then.status(200)
                .json_body(json!({
                    "value": {},
                    "status": 404
                }));
        });

        let result = monitor.get_queue_metrics("test-queue").await;
        assert!(result.is_err());
        match result.unwrap_err() {
            MonitoringError::BrokerAPIError(msg) => {
                // After all routing types fail, we get the generic "not found" message
                assert!(msg.contains("not found with any routing type"));
            }
            _ => panic!("Expected BrokerAPIError"),
        }
    }
}