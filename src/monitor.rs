use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;
use thiserror::Error;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use crate::config::ActiveMQConfig;

#[derive(Error, Debug)]
pub enum MonitoringError {
    #[error("HTTP request failed: {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("JSON parsing failed: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("ActiveMQ API error: {0}")]
    ActiveMQError(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Network timeout")]
    Timeout,
}

/// Queue metrics returned from ActiveMQ
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
    config: ActiveMQConfig,
    retry_count: u32,
    max_retries: u32,
}

impl ActiveMQMonitor {
    /// Create a new ActiveMQ monitoring client
    pub fn new(config: ActiveMQConfig) -> Result<Self> {
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

    /// Get queue metrics for a specific queue
    pub async fn get_queue_metrics(&mut self, queue_name: &str) -> Result<QueueMetrics> {
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
                        return Err(e.into());
                    }

                    let delay = self.calculate_retry_delay(attempts);
                    warn!("Attempt {}/{} failed for queue '{}': {}. Retrying in {}ms", 
                          attempts, max_attempts, queue_name, e, delay.as_millis());
                    sleep(delay).await;
                }
            }
        }

        Err(anyhow::anyhow!("Exhausted all retry attempts"))
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
            return Err(MonitoringError::ActiveMQError(format!(
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
            return Err(MonitoringError::ActiveMQError(error_msg));
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

    /// Get multiple queue metrics in parallel
    pub async fn get_multiple_queue_metrics(&mut self, queue_names: &[String]) -> Vec<(String, Result<QueueMetrics>)> {
        let mut results = Vec::new();

        // For now, fetch sequentially to avoid overwhelming the server
        // In production, you might want to implement concurrent fetching with rate limiting
        for queue_name in queue_names {
            let result = self.get_queue_metrics(queue_name).await;
            results.push((queue_name.clone(), result));
        }

        results
    }

    /// Health check - verify connectivity to ActiveMQ management API
    pub async fn health_check(&mut self) -> Result<bool> {
        let base_url = format!("http://{}:{}", self.config.host, self.config.web_port);
        let health_url = format!(
            "{}/api/jolokia/version",
            base_url.trim_end_matches('/')
        );

        debug!("Health check URL: {}", health_url);

        let request_builder = self.client.get(&health_url)
            .basic_auth(&self.config.username, Some(&self.config.password));

        match request_builder.send().await {
            Ok(response) if response.status().is_success() => {
                info!("ActiveMQ management API health check passed");
                Ok(true)
            }
            Ok(response) => {
                warn!("ActiveMQ management API health check failed: HTTP {}", response.status());
                Ok(false)
            }
            Err(e) => {
                error!("ActiveMQ management API health check error: {}", e);
                Err(e.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CredentialsConfig;

    fn create_test_config() -> ActiveMQMonitoringConfig {
        ActiveMQMonitoringConfig {
            base_url: "http://localhost:8161".to_string(),
            broker_name: "localhost".to_string(),
            credentials: Some(CredentialsConfig {
                username: "admin".to_string(),
                password: "admin".to_string(),
            }),
        }
    }

    #[test]
    fn test_monitor_creation() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config);
        assert!(monitor.is_ok());
    }

    #[test]
    fn test_retry_delay_calculation() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config).unwrap();

        let delay1 = monitor.calculate_retry_delay(1);
        assert_eq!(delay1.as_millis(), 1000);

        let delay2 = monitor.calculate_retry_delay(2);
        assert_eq!(delay2.as_millis(), 2000);

        let delay3 = monitor.calculate_retry_delay(3);
        assert_eq!(delay3.as_millis(), 4000);
    }

    #[test]
    fn test_parse_queue_metrics() {
        let config = create_test_config();
        let monitor = ActiveMQMonitor::new(config).unwrap();

        let test_json = serde_json::json!({
            "QueueSize": 10,
            "ConsumerCount": 2,
            "EnqueueCount": 100,
            "DequeueCount": 90,
            "MemoryPercentUsage": 25.5
        });

        let metrics = monitor.parse_queue_metrics("test", test_json).unwrap();
        
        assert_eq!(metrics.queue_name, "test");
        assert_eq!(metrics.queue_size, 10);
        assert_eq!(metrics.consumer_count, 2);
        assert_eq!(metrics.enqueue_count, 100);
        assert_eq!(metrics.dequeue_count, 90);
        assert_eq!(metrics.memory_percent_usage, 25.5);
    }
}