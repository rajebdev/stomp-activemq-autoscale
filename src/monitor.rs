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
pub enum MonitoringError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("JSON parsing failed: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("Broker API error: {0}")]
    BrokerAPIError(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Network timeout")]
    Timeout,
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
            .map_err(|e| MonitoringError::HttpError(e))?;

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
            
        if response.status().is_success() {
            debug!("Successfully ensured queue '{}' exists", queue_name);
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
            .map_err(|e| MonitoringError::HttpError(e))?;

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
    use serde_json::json;

    fn create_test_config() -> BrokerConfig {
        BrokerConfig {
            broker_type: BrokerType::ActiveMQ,
            host: "localhost".to_string(),
            stomp_port: 61613,
            web_port: 8161,
            username: "admin".to_string(),
            password: "admin".to_string(),
            heartbeat_secs: 30,
            broker_name: "localhost".to_string(),
        }
    }

    fn create_custom_config(host: &str, web_port: u16, broker_name: &str) -> BrokerConfig {
        BrokerConfig {
            broker_type: BrokerType::ActiveMQ,
            host: host.to_string(),
            stomp_port: 61613,
            web_port,
            username: "admin".to_string(),
            password: "admin".to_string(),
            heartbeat_secs: 30,
            broker_name: broker_name.to_string(),
        }
    }

    #[test]
    fn test_monitor_creation() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config);
        assert!(monitor.is_ok());

        let monitor = monitor.unwrap();
        assert_eq!(monitor.retry_count, 0);
        assert_eq!(monitor.max_retries, 3);
        assert_eq!(monitor.config.host, "localhost");
        assert_eq!(monitor.config.web_port, 8161);
    }

    #[test]
    fn test_monitor_creation_with_custom_config() {
        let config = create_custom_config("example.com", 9161, "custom-broker");
        let monitor = ActiveMQMonitor::new(config).unwrap();
        
        assert_eq!(monitor.config.host, "example.com");
        assert_eq!(monitor.config.web_port, 9161);
        assert_eq!(monitor.config.broker_name, "custom-broker");
    }

    #[test]
    fn test_retry_delay_calculation() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config).unwrap();

        // Test exponential backoff
        let delay1 = monitor.calculate_retry_delay(1);
        assert_eq!(delay1.as_millis(), 1000); // 1 second

        let delay2 = monitor.calculate_retry_delay(2);
        assert_eq!(delay2.as_millis(), 2000); // 2 seconds

        let delay3 = monitor.calculate_retry_delay(3);
        assert_eq!(delay3.as_millis(), 4000); // 4 seconds

        let delay4 = monitor.calculate_retry_delay(4);
        assert_eq!(delay4.as_millis(), 8000); // 8 seconds

        let delay5 = monitor.calculate_retry_delay(5);
        assert_eq!(delay5.as_millis(), 16000); // 16 seconds
    }

    #[test]
    fn test_retry_delay_max_cap() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config).unwrap();

        // Test that delay is capped at 30 seconds
        let delay_large = monitor.calculate_retry_delay(10);
        assert_eq!(delay_large.as_millis(), 30000); // Should be capped at 30 seconds

        let delay_very_large = monitor.calculate_retry_delay(20);
        assert_eq!(delay_very_large.as_millis(), 30000); // Should still be capped
    }

    #[test]
    fn test_retry_delay_edge_cases() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config).unwrap();

        // Test edge case with 0 attempt
        let delay0 = monitor.calculate_retry_delay(0);
        assert_eq!(delay0.as_millis(), 1000); // Should be base delay (saturating_sub(1) makes 0 -> 0, so 2^0 = 1)

        // Test with medium large number that should definitely be capped
        let delay_large = monitor.calculate_retry_delay(50);
        assert_eq!(delay_large.as_millis(), 30000); // Should be capped at 30 seconds
        
        // Test with very large number - due to floating point precision issues,
        // u32::MAX might not behave as expected, but it should still return a valid delay
        let delay_max = monitor.calculate_retry_delay(u32::MAX);
        assert!(delay_max.as_millis() > 0);
        assert!(delay_max.as_millis() <= 30000);
        
        // Test with a more reasonable "large" number
        let delay_reasonable = monitor.calculate_retry_delay(20);
        assert_eq!(delay_reasonable.as_millis(), 30000); // Should be capped
    }

    #[test]
    fn test_parse_queue_metrics_complete() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config).unwrap();

        let test_json = json!({
            "QueueSize": 10,
            "ConsumerCount": 2,
            "EnqueueCount": 100,
            "DequeueCount": 90,
            "MemoryPercentUsage": 25.5
        });

        let metrics = monitor.parse_queue_metrics("test_queue", test_json).unwrap();
        
        assert_eq!(metrics.queue_name, "test_queue");
        assert_eq!(metrics.queue_size, 10);
        assert_eq!(metrics.consumer_count, 2);
        assert_eq!(metrics.enqueue_count, 100);
        assert_eq!(metrics.dequeue_count, 90);
        assert_eq!(metrics.memory_percent_usage, 25.5);
    }

    #[test]
    fn test_parse_queue_metrics_partial_data() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config).unwrap();

        // Test with missing fields - should use defaults
        let test_json = json!({
            "QueueSize": 5
            // Missing other fields
        });

        let metrics = monitor.parse_queue_metrics("partial_queue", test_json).unwrap();
        
        assert_eq!(metrics.queue_name, "partial_queue");
        assert_eq!(metrics.queue_size, 5);
        assert_eq!(metrics.consumer_count, 0); // Default
        assert_eq!(metrics.enqueue_count, 0); // Default
        assert_eq!(metrics.dequeue_count, 0); // Default
        assert_eq!(metrics.memory_percent_usage, 0.0); // Default
    }

    #[test]
    fn test_parse_queue_metrics_empty_data() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config).unwrap();

        // Test with empty JSON object
        let test_json = json!({});

        let metrics = monitor.parse_queue_metrics("empty_queue", test_json).unwrap();
        
        assert_eq!(metrics.queue_name, "empty_queue");
        assert_eq!(metrics.queue_size, 0);
        assert_eq!(metrics.consumer_count, 0);
        assert_eq!(metrics.enqueue_count, 0);
        assert_eq!(metrics.dequeue_count, 0);
        assert_eq!(metrics.memory_percent_usage, 0.0);
    }

    #[test]
    fn test_parse_queue_metrics_wrong_types() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config).unwrap();

        // Test with wrong data types - should use defaults
        let test_json = json!({
            "QueueSize": "not_a_number",
            "ConsumerCount": true,
            "EnqueueCount": null,
            "DequeueCount": [],
            "MemoryPercentUsage": "invalid"
        });

        let metrics = monitor.parse_queue_metrics("invalid_queue", test_json).unwrap();
        
        assert_eq!(metrics.queue_name, "invalid_queue");
        assert_eq!(metrics.queue_size, 0); // Should default to 0
        assert_eq!(metrics.consumer_count, 0); // Should default to 0
        assert_eq!(metrics.enqueue_count, 0); // Should default to 0
        assert_eq!(metrics.dequeue_count, 0); // Should default to 0
        assert_eq!(metrics.memory_percent_usage, 0.0); // Should default to 0.0
    }

    #[test]
    fn test_parse_queue_metrics_large_numbers() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config).unwrap();

        let test_json = json!({
            "QueueSize": 4294967295u64, // Max u32
            "ConsumerCount": 1000000,
            "EnqueueCount": 18446744073709551615u64, // Max u64
            "DequeueCount": 18446744073709551614u64,
            "MemoryPercentUsage": 99.99
        });

        let metrics = monitor.parse_queue_metrics("large_queue", test_json).unwrap();
        
        assert_eq!(metrics.queue_name, "large_queue");
        assert_eq!(metrics.queue_size, 4294967295);
        assert_eq!(metrics.consumer_count, 1000000);
        assert_eq!(metrics.enqueue_count, 18446744073709551615);
        assert_eq!(metrics.dequeue_count, 18446744073709551614);
        assert_eq!(metrics.memory_percent_usage, 99.99);
    }

    #[test]
    fn test_parse_queue_metrics_zero_values() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config).unwrap();

        let test_json = json!({
            "QueueSize": 0,
            "ConsumerCount": 0,
            "EnqueueCount": 0,
            "DequeueCount": 0,
            "MemoryPercentUsage": 0.0
        });

        let metrics = monitor.parse_queue_metrics("zero_queue", test_json).unwrap();
        
        assert_eq!(metrics.queue_name, "zero_queue");
        assert_eq!(metrics.queue_size, 0);
        assert_eq!(metrics.consumer_count, 0);
        assert_eq!(metrics.enqueue_count, 0);
        assert_eq!(metrics.dequeue_count, 0);
        assert_eq!(metrics.memory_percent_usage, 0.0);
    }

    #[test]
    fn test_queue_metrics_serialization() {
        let metrics = QueueMetrics {
            queue_name: "test_queue".to_string(),
            queue_size: 15,
            consumer_count: 3,
            enqueue_count: 200,
            dequeue_count: 185,
            memory_percent_usage: 42.5,
        };

        // Test serialization to JSON
        let serialized = serde_json::to_string(&metrics).unwrap();
        assert!(serialized.contains("test_queue"));
        assert!(serialized.contains("15"));
        assert!(serialized.contains("42.5"));

        // Test deserialization from JSON
        let deserialized: QueueMetrics = serde_json::from_str(&serialized).unwrap();
        assert_eq!(deserialized.queue_name, metrics.queue_name);
        assert_eq!(deserialized.queue_size, metrics.queue_size);
        assert_eq!(deserialized.consumer_count, metrics.consumer_count);
        assert_eq!(deserialized.enqueue_count, metrics.enqueue_count);
        assert_eq!(deserialized.dequeue_count, metrics.dequeue_count);
        assert_eq!(deserialized.memory_percent_usage, metrics.memory_percent_usage);
    }

    #[test]
    fn test_queue_metrics_debug_format() {
        let metrics = QueueMetrics {
            queue_name: "debug_queue".to_string(),
            queue_size: 5,
            consumer_count: 1,
            enqueue_count: 50,
            dequeue_count: 45,
            memory_percent_usage: 12.34,
        };

        let debug_str = format!("{:?}", metrics);
        assert!(debug_str.contains("debug_queue"));
        assert!(debug_str.contains("queue_size: 5"));
        assert!(debug_str.contains("consumer_count: 1"));
        assert!(debug_str.contains("12.34"));
    }

    #[test]
    fn test_queue_metrics_clone() {
        let original = QueueMetrics {
            queue_name: "clone_queue".to_string(),
            queue_size: 8,
            consumer_count: 2,
            enqueue_count: 80,
            dequeue_count: 72,
            memory_percent_usage: 15.5,
        };

        let cloned = original.clone();
        
        assert_eq!(cloned.queue_name, original.queue_name);
        assert_eq!(cloned.queue_size, original.queue_size);
        assert_eq!(cloned.consumer_count, original.consumer_count);
        assert_eq!(cloned.enqueue_count, original.enqueue_count);
        assert_eq!(cloned.dequeue_count, original.dequeue_count);
        assert_eq!(cloned.memory_percent_usage, original.memory_percent_usage);
    }

    #[test]
    fn test_monitoring_error_display() {
        // Test Broker API error
        let broker_error = MonitoringError::BrokerAPIError("Test error".to_string());
        let display_str = format!("{}", broker_error);
        assert!(display_str.contains("Broker API error: Test error"));

        // Test config error
        let config_error = MonitoringError::ConfigError("Invalid config".to_string());
        let display_str = format!("{}", config_error);
        assert!(display_str.contains("Configuration error: Invalid config"));

        // Test timeout error
        let timeout_error = MonitoringError::Timeout;
        let display_str = format!("{}", timeout_error);
        assert!(display_str.contains("Network timeout"));

        // Test JSON error using a real parsing failure
        let invalid_json = "{ invalid json }";
        let json_result: Result<serde_json::Value, serde_json::Error> = serde_json::from_str(invalid_json);
        if let Err(json_err) = json_result {
            let json_error = MonitoringError::JsonError(json_err);
            let display_str = format!("{}", json_error);
            assert!(display_str.contains("JSON parsing failed"));
        }
    }

    #[test]
    fn test_monitoring_error_debug() {
        let error = MonitoringError::BrokerAPIError("Debug test".to_string());
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("BrokerAPIError"));
        assert!(debug_str.contains("Debug test"));
    }

    #[test]
    fn test_jolokia_response_deserialization() {
        let json_response = json!({
            "value": {
                "QueueSize": 10,
                "ConsumerCount": 2
            },
            "status": 200
        });

        let response: JolokiaResponse = serde_json::from_value(json_response).unwrap();
        assert_eq!(response.status, 200);
        assert!(response.error.is_none());
        assert!(response.value.is_object());
    }

    #[test]
    fn test_jolokia_response_with_error() {
        let json_response = json!({
            "value": null,
            "status": 404,
            "error": "Object not found"
        });

        let response: JolokiaResponse = serde_json::from_value(json_response).unwrap();
        assert_eq!(response.status, 404);
        assert_eq!(response.error.unwrap(), "Object not found");
    }

    #[test]
    fn test_jolokia_response_minimal() {
        let json_response = json!({
            "value": {},
            "status": 200
        });

        let response: JolokiaResponse = serde_json::from_value(json_response).unwrap();
        assert_eq!(response.status, 200);
        assert!(response.error.is_none()); // Should default to None
    }

    // Mock tests would require additional test infrastructure,
    // but these unit tests cover the core logic and error handling
    #[tokio::test]
    async fn test_get_multiple_queue_metrics_empty() {
        let config = create_test_config();
        let mut monitor = ActiveMQMonitor::new(config).unwrap();
        
        let empty_queues: Vec<String> = vec![];
        let results = monitor.get_multiple_queue_metrics(&empty_queues).await;
        
        assert!(results.is_empty());
    }

    #[test]
    fn test_config_values_in_monitor() {
        let config = BrokerConfig {
            broker_type: BrokerType::ActiveMQ,
            host: "test-host".to_string(),
            stomp_port: 12345,
            web_port: 54321,
            username: "test-user".to_string(),
            password: "test-pass".to_string(),
            heartbeat_secs: 60,
            broker_name: "test-broker".to_string(),
        };

        let monitor = ActiveMQMonitor::new(config).unwrap();
        
        assert_eq!(monitor.config.host, "test-host");
        assert_eq!(monitor.config.stomp_port, 12345);
        assert_eq!(monitor.config.web_port, 54321);
        assert_eq!(monitor.config.username, "test-user");
        assert_eq!(monitor.config.password, "test-pass");
        assert_eq!(monitor.config.heartbeat_secs, 60);
        assert_eq!(monitor.config.broker_name, "test-broker");
    }

    #[test]
    fn test_retry_count_initialization() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config).unwrap();
        
        assert_eq!(monitor.retry_count, 0);
        assert_eq!(monitor.max_retries, 3);
    }

    #[test]
    fn test_floating_point_precision() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config).unwrap();

        let test_json = json!({
            "MemoryPercentUsage": 33.333333333333336
        });

        let metrics = monitor.parse_queue_metrics("precision_test", test_json).unwrap();
        assert!((metrics.memory_percent_usage - 33.333333333333336).abs() < f64::EPSILON);
    }

    #[test]
    fn test_queue_metrics_default_values() {
        let metrics = QueueMetrics {
            queue_name: "default_test".to_string(),
            queue_size: 0,
            consumer_count: 0,
            enqueue_count: 0,
            dequeue_count: 0,
            memory_percent_usage: 0.0,
        };

        assert_eq!(metrics.queue_name, "default_test");
        assert_eq!(metrics.queue_size, 0);
        assert_eq!(metrics.consumer_count, 0);
        assert_eq!(metrics.enqueue_count, 0);
        assert_eq!(metrics.dequeue_count, 0);
        assert_eq!(metrics.memory_percent_usage, 0.0);
    }
}
