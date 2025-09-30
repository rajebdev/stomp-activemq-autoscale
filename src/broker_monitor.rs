use anyhow::Result;
use async_trait::async_trait;

use crate::monitor::{QueueMetrics, MonitoringError};

/// Trait for message broker monitoring implementations
#[async_trait]
pub trait BrokerMonitor: Send + Sync {
    /// Get queue metrics for a specific queue
    async fn get_queue_metrics(&mut self, queue_name: &str) -> Result<QueueMetrics, MonitoringError>;

    /// Get multiple queue metrics in parallel or sequentially
    async fn get_multiple_queue_metrics(&mut self, queue_names: &[String]) -> Vec<(String, Result<QueueMetrics, MonitoringError>)> {
        let mut results = Vec::new();
        
        // Default implementation: fetch sequentially
        for queue_name in queue_names {
            let result = self.get_queue_metrics(queue_name).await;
            results.push((queue_name.clone(), result));
        }
        
        results
    }

    /// Health check - verify connectivity to broker management API
    async fn health_check(&mut self) -> Result<bool>;

    /// Get the broker type string for logging
    fn broker_type(&self) -> &'static str;
}