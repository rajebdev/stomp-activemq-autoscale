use anyhow::{Context, Result};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::time::Duration;

/// Main configuration structure that mirrors the config.yaml file
#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub service: ServiceConfig,
    pub broker: BrokerConfig,
    pub destinations: DestinationsConfig,
    #[serde(default = "ScalingConfig::default")]
    pub scaling: ScalingConfig,
    // Optional configs with defaults
    #[serde(default = "ConsumersConfig::default")]
    pub consumers: ConsumersConfig,
    #[serde(default = "LoggingConfig::default")]
    pub logging: LoggingConfig,
    #[serde(default = "ShutdownConfig::default")]
    pub shutdown: ShutdownConfig,
    #[serde(default = "RetryConfig::default")]
    pub retry: RetryConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ServiceConfig {
    pub name: String,
    pub version: String,
    pub description: String,
}

/// Supported broker types
#[derive(Debug, Deserialize, Clone, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum BrokerType {
    ActiveMQ,
    Artemis,
}

/// Unified broker configuration for both STOMP and monitoring
#[derive(Debug, Deserialize, Clone)]
pub struct BrokerConfig {
    #[serde(rename = "type")]
    pub broker_type: BrokerType,
    pub host: String,
    pub username: String,
    pub password: String,
    pub stomp_port: u16,
    pub web_port: u16,
    pub heartbeat_secs: u32,
    #[serde(default = "default_broker_name")]
    pub broker_name: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts. -1 means infinite retries.
    pub max_attempts: i32,
    pub initial_delay_ms: u64,
    pub max_delay_ms: u64,
    pub backoff_multiplier: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct DestinationsConfig {
    pub queues: HashMap<String, String>,  // queue_name -> path
    pub topics: HashMap<String, String>,  // topic_name -> path
}

#[derive(Debug, Deserialize, Clone)]
pub struct ConsumersConfig {
    pub ack_mode: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct LoggingConfig {
    pub level: String,
    pub output: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ShutdownConfig {
    pub timeout_secs: u32,
    pub grace_period_secs: u32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ScalingConfig {
    /// Enable/disable auto-scaling
    #[serde(default = "default_scaling_enabled")]
    pub enabled: bool,
    /// Polling interval in seconds for checking queue metrics
    pub interval_secs: u64,
    /// Queue-specific worker scaling rules
    pub workers: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Clone, PartialEq)]
pub struct WorkerRange {
    pub min: u32,
    pub max: u32,
    pub is_fixed: bool,
}

fn default_broker_name() -> String {
    "localhost".to_string()
}

fn default_scaling_enabled() -> bool {
    true
}


impl RetryConfig {
    /// Calculate the delay for the given retry attempt using exponential backoff
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::from_millis(self.initial_delay_ms);
        }

        let delay_ms =
            (self.initial_delay_ms as f64) * self.backoff_multiplier.powi(attempt as i32);
        let capped_delay_ms = delay_ms.min(self.max_delay_ms as f64) as u64;

        Duration::from_millis(capped_delay_ms)
    }

    /// Check if we should retry based on the current attempt number
    pub fn should_retry(&self, attempt: u32) -> bool {
        if self.max_attempts < 0 {
            true // Infinite retries when negative
        } else {
            attempt < (self.max_attempts as u32)
        }
    }

}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: -1, // Infinite retries by default
            initial_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
        }
    }
}

impl Default for ConsumersConfig {
    fn default() -> Self {
        Self {
            ack_mode: "client_individual".to_string(),
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            output: "stdout".to_string(),
        }
    }
}

impl Default for ShutdownConfig {
    fn default() -> Self {
        Self {
            timeout_secs: 30,
            grace_period_secs: 5,
        }
    }
}

impl Default for ScalingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 5,
            workers: HashMap::new(),
        }
    }
}

impl Config {
    /// Load configuration from a YAML file
    pub fn load(path: &str) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path))?;

        let config: Config = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path))?;

        Ok(config)
    }


    /// Get credentials
    pub fn get_credentials(&self) -> Option<(String, String)> {
        Some((self.broker.username.clone(), self.broker.password.clone()))
    }

    /// Get a specific queue path
    pub fn get_queue_config(&self, name: &str) -> Option<&String> {
        self.destinations.queues.get(name)
    }

    /// Get a specific topic path
    pub fn get_topic_config(&self, name: &str) -> Option<&String> {
        self.destinations.topics.get(name)
    }

    /// Get heartbeat settings in milliseconds (converting from seconds)
    pub fn get_heartbeat_ms(&self) -> (u32, u32) {
        let ms = self.broker.heartbeat_secs * 1000;
        (ms, ms)
    }

    /// Get all configured queue names
    pub fn get_all_queue_names(&self) -> Vec<String> {
        self.destinations.queues.keys().cloned().collect()
    }

    /// Get all configured topic names
    pub fn get_all_topic_names(&self) -> Vec<String> {
        self.destinations.topics.keys().cloned().collect()
    }

    /// Get auto-scaling configuration
    pub fn get_monitoring_config(&self) -> &ScalingConfig {
        &self.scaling
    }

    /// Check if auto-scaling is enabled
    pub fn is_auto_scaling_enabled(&self) -> bool {
        self.scaling.enabled
    }
    
    /// Check if monitoring is configured (even if disabled)
    pub fn is_monitoring_configured(&self) -> bool {
        true // Always configured now
    }

    /// Parse worker configuration string (e.g., "1-4" or "2")
    fn parse_worker_config(config_str: &str) -> Result<WorkerRange> {
        if config_str.contains('-') {
            // Range format: "1-4"
            let parts: Vec<&str> = config_str.split('-').collect();
            if parts.len() != 2 {
                return Err(anyhow::anyhow!("Invalid worker range format: {}", config_str));
            }
            let min = parts[0].parse::<u32>()
                .with_context(|| format!("Invalid min worker count: {}", parts[0]))?;
            let max = parts[1].parse::<u32>()
                .with_context(|| format!("Invalid max worker count: {}", parts[1]))?;
            
            if min > max {
                return Err(anyhow::anyhow!("Min worker count ({}) cannot be greater than max ({})", min, max));
            }
            
            Ok(WorkerRange { min, max, is_fixed: false })
        } else {
            // Fixed count format: "2"
            let count = config_str.parse::<u32>()
                .with_context(|| format!("Invalid worker count: {}", config_str))?;
            Ok(WorkerRange { min: count, max: count, is_fixed: true })
        }
    }
    
    /// Get worker range for a specific queue
    pub fn get_queue_worker_range(&self, queue_name: &str) -> Option<WorkerRange> {
        self.scaling.workers.get(queue_name)
            .and_then(|config_str| Self::parse_worker_config(config_str).ok())
            .map(|mut range| {
                // If scaling is disabled, set both min and max to min value
                if !self.scaling.enabled {
                    range.max = range.min;
                    range.is_fixed = true;
                }
                range
            })
    }

    /// Get all queues configured for auto-scaling
    pub fn get_auto_scaling_queues(&self) -> Vec<String> {
        if !self.scaling.enabled {
            return Vec::new();
        }
        
        self.scaling.workers.keys()
            .filter(|queue_name| {
                // Only include queues that have range format (e.g., "1-4")
                if let Some(config_str) = self.scaling.workers.get(*queue_name) {
                    config_str.contains('-')
                } else {
                    false
                }
            })
            .cloned()
            .collect()
    }
    
    /// Get all queues configured with fixed worker counts
    pub fn get_fixed_worker_queues(&self) -> Vec<String> {
        self.scaling.workers.keys()
            .filter(|queue_name| {
                if let Some(config_str) = self.scaling.workers.get(*queue_name) {
                    !config_str.contains('-')
                } else {
                    false
                }
            })
            .cloned()
            .collect()
    }
    
    /// Get all configured queue workers (both auto-scaling and fixed)
    pub fn get_all_configured_queues(&self) -> Vec<String> {
        self.scaling.workers.keys().cloned().collect()
    }

    /// Get the monitoring interval in seconds
    pub fn get_monitoring_interval_secs(&self) -> Option<u64> {
        Some(self.scaling.interval_secs)
    }


    /// Get the actual ActiveMQ queue name from config key
    /// Converts from destinations.queues.key path to just the queue name
    /// Example: "/queue/demo" -> "demo", "/queue/api.requests" -> "api.requests"
    pub fn get_activemq_queue_name(&self, queue_config_key: &str) -> Option<String> {
        self.destinations
            .queues
            .get(queue_config_key)
            .map(|path| {
                // Extract queue name from path like "/queue/demo" -> "demo"
                if path.starts_with("/queue/") {
                    path.strip_prefix("/queue/").unwrap_or(path).to_string()
                } else {
                    // Fallback to full path if it doesn't follow expected format
                    path.clone()
                }
            })
    }

    /// Get mapping of config keys to ActiveMQ queue names for all configured queues
    pub fn get_queue_key_to_activemq_name_mapping(&self) -> std::collections::HashMap<String, String> {
        let mut mapping = std::collections::HashMap::new();
        
        for (config_key, _queue_path) in &self.destinations.queues {
            if let Some(activemq_name) = self.get_activemq_queue_name(config_key) {
                mapping.insert(config_key.clone(), activemq_name);
            }
        }
        
        mapping
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn get_test_yaml() -> &'static str {
        r#"
service:
  name: "test-service"
  version: "1.0.0"
  description: "Test service"

broker:
  type: "activemq"
  host: "localhost"
  username: "admin"
  password: "admin"
  stomp_port: 61613
  web_port: 8161
  heartbeat_secs: 30
  broker_name: "localhost"

destinations:
  queues:
    default: "/queue/demo"
    api_requests: "/queue/api.requests"
  topics:
    notifications: "/topic/notifications"

scaling:
  enabled: true
  interval_secs: 5
  workers:
    default: "1-4"
    api_requests: "2-6"

consumers:
  ack_mode: "auto"

logging:
  level: "info"
  output: "stdout"

shutdown:
  timeout_secs: 30
  grace_period_secs: 5

retry:
  max_attempts: 3
  initial_delay_ms: 1000
  max_delay_ms: 30000
  backoff_multiplier: 2.0
"#
    }

    fn create_test_config_yaml() -> &'static str {
        r#"
service:
  name: "test-service"
  version: "1.0.0"
  description: "Test service"

broker:
  type: "activemq"
  host: "localhost"
  stomp_port: 61613
  web_port: 8161
  username: "admin"
  password: "admin"
  heartbeat_secs: 30
  broker_name: "localhost"

destinations:
  queues:
    test: "/queue/test"
  topics:
    notifications: "/topic/notifications"

scaling:
  enabled: true
  interval_secs: 5
  workers:
    test: "1-3"

consumers:
  ack_mode: "client_individual"

logging:
  level: "info"
  output: "stdout"

shutdown:
  timeout_secs: 30
  grace_period_secs: 5

retry:
  max_attempts: 3
  initial_delay_ms: 500
  max_delay_ms: 5000
  backoff_multiplier: 2.0
        "#
    }

    #[test]
    fn test_config_parsing() {
        let yaml_content = create_test_config_yaml();

        let config: Config = serde_yaml::from_str(yaml_content).unwrap();
        assert_eq!(config.service.name, "test-service");
        assert_eq!(config.broker.host, "localhost");
        assert_eq!(config.broker.stomp_port, 61613);

        let (username, password) = config.get_credentials().unwrap();
        assert_eq!(username, "admin");
        assert_eq!(password, "admin");

        let (send_ms, recv_ms) = config.get_heartbeat_ms();
        assert_eq!(send_ms, 30000);
        assert_eq!(recv_ms, 30000);

        // Test retry configuration
        assert_eq!(config.retry.max_attempts, 3);
        assert_eq!(config.retry.initial_delay_ms, 500);
        assert_eq!(config.retry.max_delay_ms, 5000);
        assert_eq!(config.retry.backoff_multiplier, 2.0);

        // Test retry delay calculations
        let delay_0 = config.retry.calculate_delay(0);
        assert_eq!(delay_0.as_millis(), 500);

        let delay_1 = config.retry.calculate_delay(1);
        assert_eq!(delay_1.as_millis(), 1000); // 500 * 2^1

        let delay_2 = config.retry.calculate_delay(2);
        assert_eq!(delay_2.as_millis(), 2000); // 500 * 2^2

        // Test should_retry logic
        assert!(config.retry.should_retry(0));
        assert!(config.retry.should_retry(2));
        assert!(!config.retry.should_retry(3)); // max_attempts is 3, so attempt 3 should not retry
    }

    #[test]
    fn test_config_load_from_file() {
        let yaml_content = create_test_config_yaml();
        
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "{}", yaml_content).unwrap();

        let config = Config::load(temp_file.path().to_str().unwrap()).unwrap();
        assert_eq!(config.service.name, "test-service");
        assert_eq!(config.broker.host, "localhost");
    }

    #[test]
    fn test_config_load_nonexistent_file() {
        let result = Config::load("nonexistent_file.yaml");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to read config file"));
    }

    #[test]
    fn test_config_load_invalid_yaml() {
        let invalid_yaml = "invalid: yaml: content: [unclosed";
        
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "{}", invalid_yaml).unwrap();

        let result = Config::load(temp_file.path().to_str().unwrap());
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to parse config file"));
    }

    #[test]
    fn test_config_with_defaults() {
        let minimal_yaml = r#"
service:
  name: "minimal-service"
  version: "1.0.0"
  description: "Minimal service"

broker:
  type: "activemq"
  host: "localhost"
  stomp_port: 61613
  web_port: 8161
  username: "admin"
  password: "admin"
  heartbeat_secs: 30

destinations:
  queues: {}
  topics: {}
        "#;

        let config: Config = serde_yaml::from_str(minimal_yaml).unwrap();
        
        // Check that defaults are applied correctly
        assert_eq!(config.broker.broker_name, "localhost");
        assert!(config.scaling.enabled);
        assert_eq!(config.scaling.interval_secs, 5);
        assert_eq!(config.consumers.ack_mode, "client_individual");
        assert_eq!(config.logging.level, "info");
        assert_eq!(config.logging.output, "stdout");
        assert_eq!(config.shutdown.timeout_secs, 30);
        assert_eq!(config.shutdown.grace_period_secs, 5);
        assert_eq!(config.retry.max_attempts, -1);
    }

    #[test]
    fn test_retry_config_default() {
        let retry_config = RetryConfig::default();
        assert_eq!(retry_config.max_attempts, -1); // Default is now infinite retries (-1)
        assert_eq!(retry_config.initial_delay_ms, 1000);
        assert_eq!(retry_config.max_delay_ms, 30000);
        assert_eq!(retry_config.backoff_multiplier, 2.0);

        // Test delay capping
        let large_delay = retry_config.calculate_delay(10); // Should be capped at max_delay_ms
        assert_eq!(large_delay.as_millis(), 30000);

        // Test infinite retry logic
        assert!(retry_config.should_retry(0));
        assert!(retry_config.should_retry(100));
        assert!(retry_config.should_retry(1000000)); // Should always retry with infinite retries
        assert!(retry_config.max_attempts == -1); // Should be -1 for infinite retries
    }

    #[test]
    fn test_retry_config_edge_cases() {
        // Test with zero attempts
        let retry_config = RetryConfig {
            max_attempts: 0,
            initial_delay_ms: 1000,
            max_delay_ms: 5000,
            backoff_multiplier: 2.0,
        };
        assert!(!retry_config.should_retry(0));
        assert!(!retry_config.should_retry(1));

        // Test with zero delay
        let zero_delay_config = RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 0,
            max_delay_ms: 1000,
            backoff_multiplier: 2.0,
        };
        let delay = zero_delay_config.calculate_delay(0);
        assert_eq!(delay.as_millis(), 0);

        // Test with large multiplier
        let large_multiplier_config = RetryConfig {
            max_attempts: 3,
            initial_delay_ms: 100,
            max_delay_ms: 1000,
            backoff_multiplier: 10.0,
        };
        let delay = large_multiplier_config.calculate_delay(2);
        assert_eq!(delay.as_millis(), 1000); // Should be capped at max_delay_ms
    }

    #[test]
    fn test_parse_worker_config_valid() {
        // Test range format
        let range_result = Config::parse_worker_config("1-4").unwrap();
        assert_eq!(range_result.min, 1);
        assert_eq!(range_result.max, 4);
        assert!(!range_result.is_fixed);

        // Test fixed format
        let fixed_result = Config::parse_worker_config("3").unwrap();
        assert_eq!(fixed_result.min, 3);
        assert_eq!(fixed_result.max, 3);
        assert!(fixed_result.is_fixed);

        // Test single digit range
        let single_range = Config::parse_worker_config("0-1").unwrap();
        assert_eq!(single_range.min, 0);
        assert_eq!(single_range.max, 1);
        assert!(!single_range.is_fixed);
    }

    #[test]
    fn test_parse_worker_config_invalid() {
        // Test invalid range format
        assert!(Config::parse_worker_config("1-4-6").is_err());
        assert!(Config::parse_worker_config("a-b").is_err());
        assert!(Config::parse_worker_config("1-").is_err());
        assert!(Config::parse_worker_config("-4").is_err());
        assert!(Config::parse_worker_config("").is_err());
        assert!(Config::parse_worker_config("abc").is_err());

        // Test min > max
        assert!(Config::parse_worker_config("5-2").is_err());
    }

    #[test]
    fn test_queue_and_topic_operations() {
        let yaml_content = r#"
service:
  name: "test-service"
  version: "1.0.0"
  description: "Test service"

broker:
  type: "activemq"
  host: "localhost"
  stomp_port: 61613
  web_port: 8161
  username: "admin"
  password: "admin"
  heartbeat_secs: 30

destinations:
  queues:
    queue1: "/queue/q1"
    queue2: "/queue/q2"
  topics:
    topic1: "/topic/t1"
    topic2: "/topic/t2"

scaling:
  enabled: true
  interval_secs: 10
  workers: {}
        "#;

        let config: Config = serde_yaml::from_str(yaml_content).unwrap();

        // Test queue operations
        assert_eq!(config.get_queue_config("queue1"), Some(&"/queue/q1".to_string()));
        assert_eq!(config.get_queue_config("nonexistent"), None);
        
        let queue_names = config.get_all_queue_names();
        assert_eq!(queue_names.len(), 2);
        assert!(queue_names.contains(&"queue1".to_string()));
        assert!(queue_names.contains(&"queue2".to_string()));

        // Test topic operations
        assert_eq!(config.get_topic_config("topic1"), Some(&"/topic/t1".to_string()));
        assert_eq!(config.get_topic_config("nonexistent"), None);
        
        let topic_names = config.get_all_topic_names();
        assert_eq!(topic_names.len(), 2);
        assert!(topic_names.contains(&"topic1".to_string()));
        assert!(topic_names.contains(&"topic2".to_string()));

        // Test monitoring interval
        assert_eq!(config.get_monitoring_interval_secs(), Some(10));
    }

    #[test]
    fn test_heartbeat_conversion() {
        let config = Config {
            service: ServiceConfig {
                name: "test".to_string(),
                version: "1.0".to_string(),
                description: "test".to_string(),
            },
            broker: BrokerConfig {
                broker_type: BrokerType::ActiveMQ,
                host: "localhost".to_string(),
                username: "admin".to_string(),
                password: "admin".to_string(),
                stomp_port: 61613,
                web_port: 8161,
                heartbeat_secs: 45, // Test non-standard value
                broker_name: "localhost".to_string(),
            },
            destinations: DestinationsConfig {
                queues: HashMap::new(),
                topics: HashMap::new(),
            },
            scaling: ScalingConfig::default(),
            consumers: ConsumersConfig::default(),
            logging: LoggingConfig::default(),
            shutdown: ShutdownConfig::default(),
            retry: RetryConfig::default(),
        };

        let (send_ms, recv_ms) = config.get_heartbeat_ms();
        assert_eq!(send_ms, 45000);
        assert_eq!(recv_ms, 45000);
    }

    #[test]
    fn test_activemq_queue_name_extraction() {
        let yaml_content = r#"
service:
  name: "test-service"
  version: "1.0.0"
  description: "Test service"

broker:
  type: "activemq"
  host: "localhost"
  stomp_port: 61613
  web_port: 8161
  username: "admin"
  password: "admin"
  heartbeat_secs: 30
  broker_name: "localhost"

destinations:
  queues:
    default: "/queue/demo"
    api_requests: "/queue/api.requests"
    errors: "/queue/errors"
  topics: {}

scaling:
  enabled: false
  interval_secs: 5
  workers: {}

consumers:
  ack_mode: "client_individual"

logging:
  level: "info"
  output: "stdout"

shutdown:
  timeout_secs: 30
  grace_period_secs: 5

retry:
  max_attempts: 3
  initial_delay_ms: 500
  max_delay_ms: 5000
  backoff_multiplier: 2.0
        "#;

        let config: Config = serde_yaml::from_str(yaml_content).unwrap();
        
        // Test individual queue name extraction
        assert_eq!(config.get_activemq_queue_name("default"), Some("demo".to_string()));
        assert_eq!(config.get_activemq_queue_name("api_requests"), Some("api.requests".to_string()));
        assert_eq!(config.get_activemq_queue_name("errors"), Some("errors".to_string()));
        assert_eq!(config.get_activemq_queue_name("nonexistent"), None);
        
        // Test mapping function
        let mapping = config.get_queue_key_to_activemq_name_mapping();
        assert_eq!(mapping.len(), 3);
        assert_eq!(mapping.get("default"), Some(&"demo".to_string()));
        assert_eq!(mapping.get("api_requests"), Some(&"api.requests".to_string()));
        assert_eq!(mapping.get("errors"), Some(&"errors".to_string()));
    }

    #[test]
    fn test_worker_range_parsing() {
        let yaml_content = r#"
service:
  name: "test-service"
  version: "1.0.0"
  description: "Test service"

broker:
  type: "activemq"
  host: "localhost"
  stomp_port: 61613
  web_port: 8161
  username: "admin"
  password: "admin"
  heartbeat_secs: 30
  broker_name: "localhost"

destinations:
  queues:
    test_queue: "/queue/test"
  topics: {}

scaling:
  enabled: true
  interval_secs: 5
  workers:
    auto_scaling_queue: "1-4"
    fixed_queue: "3"

consumers:
  ack_mode: "client_individual"

logging:
  level: "info"
  output: "stdout"

shutdown:
  timeout_secs: 30
  grace_period_secs: 5
        "#;

        let config: Config = serde_yaml::from_str(yaml_content).unwrap();
        
        // Test auto-scaling queue parsing
        let auto_range = config.get_queue_worker_range("auto_scaling_queue").unwrap();
        assert_eq!(auto_range.min, 1);
        assert_eq!(auto_range.max, 4);
        assert!(!auto_range.is_fixed);
        
        // Test fixed worker queue parsing
        let fixed_range = config.get_queue_worker_range("fixed_queue").unwrap();
        assert_eq!(fixed_range.min, 3);
        assert_eq!(fixed_range.max, 3);
        assert!(fixed_range.is_fixed);
        
        // Test queue categorization
        let auto_scaling_queues = config.get_auto_scaling_queues();
        assert_eq!(auto_scaling_queues, vec!["auto_scaling_queue"]);
        
        let fixed_worker_queues = config.get_fixed_worker_queues();
        assert_eq!(fixed_worker_queues, vec!["fixed_queue"]);
        
        // Test monitoring enabled
        assert!(config.is_auto_scaling_enabled());
        assert!(config.is_monitoring_configured());
    }

    #[test]
    fn test_monitoring_disabled() {
        let yaml_content = r#"
service:
  name: "test-service"
  version: "1.0.0"
  description: "Test service"

broker:
  type: "activemq"
  host: "localhost"
  stomp_port: 61613
  web_port: 8161
  username: "admin"
  password: "admin"
  heartbeat_secs: 30
  broker_name: "localhost"

destinations:
  queues:
    test_queue: "/queue/test"
  topics: {}

scaling:
  enabled: false
  interval_secs: 5
  workers:
    auto_scaling_queue: "1-4"
    fixed_queue: "3"

consumers:
  ack_mode: "client_individual"

logging:
  level: "info"
  output: "stdout"

shutdown:
  timeout_secs: 30
  grace_period_secs: 5
        "#;

        let config: Config = serde_yaml::from_str(yaml_content).unwrap();
        
        // Test that when monitoring is disabled, ranges are converted to fixed at min value
        let auto_range = config.get_queue_worker_range("auto_scaling_queue").unwrap();
        assert_eq!(auto_range.min, 1);
        assert_eq!(auto_range.max, 1); // Should be capped at min when disabled
        assert!(auto_range.is_fixed);
        
        let fixed_range = config.get_queue_worker_range("fixed_queue").unwrap();
        assert_eq!(fixed_range.min, 3);
        assert_eq!(fixed_range.max, 3);
        assert!(fixed_range.is_fixed);
        
        // Test that auto-scaling is properly disabled
        assert!(!config.is_auto_scaling_enabled());
        assert!(config.is_monitoring_configured());
        
        // When monitoring is disabled, get_auto_scaling_queues should return empty
        let auto_scaling_queues = config.get_auto_scaling_queues();
        assert_eq!(auto_scaling_queues, Vec::<String>::new());
    }

    #[test]
    fn test_default_implementations() {
        // Test all default implementations
        let consumers_default = ConsumersConfig::default();
        assert_eq!(consumers_default.ack_mode, "client_individual");

        let logging_default = LoggingConfig::default();
        assert_eq!(logging_default.level, "info");
        assert_eq!(logging_default.output, "stdout");

        let shutdown_default = ShutdownConfig::default();
        assert_eq!(shutdown_default.timeout_secs, 30);
        assert_eq!(shutdown_default.grace_period_secs, 5);

        let scaling_default = ScalingConfig::default();
        assert!(scaling_default.enabled);
        assert_eq!(scaling_default.interval_secs, 5);
        assert!(scaling_default.workers.is_empty());

        let retry_default = RetryConfig::default();
        assert_eq!(retry_default.max_attempts, -1);
        assert_eq!(retry_default.initial_delay_ms, 1000);
        assert_eq!(retry_default.max_delay_ms, 30000);
        assert_eq!(retry_default.backoff_multiplier, 2.0);
    }

    #[test]
    fn test_default_functions() {
        assert_eq!(default_broker_name(), "localhost");
        assert_eq!(default_scaling_enabled(), true);
    }

    #[test]
    fn test_worker_range_equality() {
        let range1 = WorkerRange {
            min: 1,
            max: 4,
            is_fixed: false,
        };
        let range2 = WorkerRange {
            min: 1,
            max: 4,
            is_fixed: false,
        };
        let range3 = WorkerRange {
            min: 1,
            max: 4,
            is_fixed: true,
        };

        assert_eq!(range1, range2);
        assert_ne!(range1, range3);
    }

    #[test]
    fn test_config_queue_operations_edge_cases() {
        let yaml_content = r#"
service:
  name: "test-service"
  version: "1.0.0"
  description: "Test service"

broker:
  type: "activemq"
  host: "localhost"
  stomp_port: 61613
  web_port: 8161
  username: "admin"
  password: "admin"
  heartbeat_secs: 30

destinations:
  queues: {}
  topics: {}

scaling:
  enabled: true
  interval_secs: 5
  workers: {}
        "#;

        let config: Config = serde_yaml::from_str(yaml_content).unwrap();

        // Test empty configurations
        assert_eq!(config.get_all_queue_names().len(), 0);
        assert_eq!(config.get_all_topic_names().len(), 0);
        assert_eq!(config.get_auto_scaling_queues().len(), 0);
        assert_eq!(config.get_fixed_worker_queues().len(), 0);
        assert_eq!(config.get_all_configured_queues().len(), 0);

        // Test nonexistent queue worker range
        assert!(config.get_queue_worker_range("nonexistent").is_none());

        // Test empty activemq queue name mapping
        let mapping = config.get_queue_key_to_activemq_name_mapping();
        assert!(mapping.is_empty());
    }

    #[test]
    fn test_complex_queue_path_extraction() {
        let yaml_content = r#"
service:
  name: "test-service"
  version: "1.0.0"
  description: "Test service"

broker:
  type: "activemq"
  host: "localhost"
  stomp_port: 61613
  web_port: 8161
  username: "admin"
  password: "admin"
  heartbeat_secs: 30

destinations:
  queues:
    simple: "/queue/simple"
    complex: "/queue/complex.queue.name"
    unusual: "unusual_format"
    empty_name: "/queue/"
    deep_path: "/queue/app/module/queue"
  topics: {}

scaling:
  enabled: false
  interval_secs: 5
  workers: {}
        "#;

        let config: Config = serde_yaml::from_str(yaml_content).unwrap();

        // Test various queue name extraction patterns
        assert_eq!(config.get_activemq_queue_name("simple"), Some("simple".to_string()));
        assert_eq!(config.get_activemq_queue_name("complex"), Some("complex.queue.name".to_string()));
        assert_eq!(config.get_activemq_queue_name("unusual"), Some("unusual_format".to_string()));
        assert_eq!(config.get_activemq_queue_name("empty_name"), Some("".to_string()));
        assert_eq!(config.get_activemq_queue_name("deep_path"), Some("app/module/queue".to_string()));
        assert_eq!(config.get_activemq_queue_name("nonexistent"), None);

        // Test mapping with various formats
        let mapping = config.get_queue_key_to_activemq_name_mapping();
        assert_eq!(mapping.len(), 5);
        assert_eq!(mapping.get("simple"), Some(&"simple".to_string()));
        assert_eq!(mapping.get("complex"), Some(&"complex.queue.name".to_string()));
        assert_eq!(mapping.get("unusual"), Some(&"unusual_format".to_string()));
    }

    #[test]
    fn test_worker_scaling_mixed_configurations() {
        let yaml_content = r#"
service:
  name: "test-service"
  version: "1.0.0"
  description: "Test service"

broker:
  type: "activemq"
  host: "localhost"
  stomp_port: 61613
  web_port: 8161
  username: "admin"
  password: "admin"
  heartbeat_secs: 30

destinations:
  queues:
    test_queue: "/queue/test"
  topics: {}

scaling:
  enabled: true
  interval_secs: 5
  workers:
    range_queue: "2-8"
    fixed_queue_1: "1"
    fixed_queue_5: "5"
    another_range: "0-2"
        "#;

        let config: Config = serde_yaml::from_str(yaml_content).unwrap();

        // Test mixed worker configurations
        let auto_scaling = config.get_auto_scaling_queues();
        assert_eq!(auto_scaling.len(), 2);
        assert!(auto_scaling.contains(&"range_queue".to_string()));
        assert!(auto_scaling.contains(&"another_range".to_string()));

        let fixed_workers = config.get_fixed_worker_queues();
        assert_eq!(fixed_workers.len(), 2);
        assert!(fixed_workers.contains(&"fixed_queue_1".to_string()));
        assert!(fixed_workers.contains(&"fixed_queue_5".to_string()));

        let all_configured = config.get_all_configured_queues();
        assert_eq!(all_configured.len(), 4);

        // Test specific ranges
        let range_config = config.get_queue_worker_range("range_queue").unwrap();
        assert_eq!(range_config.min, 2);
        assert_eq!(range_config.max, 8);
        assert!(!range_config.is_fixed);

        let fixed_config = config.get_queue_worker_range("fixed_queue_5").unwrap();
        assert_eq!(fixed_config.min, 5);
        assert_eq!(fixed_config.max, 5);
        assert!(fixed_config.is_fixed);
    }
}



