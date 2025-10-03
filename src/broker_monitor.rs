use anyhow::Result;
use async_trait::async_trait;

use crate::monitor::{QueueMetrics, MonitoringError};

/// Trait for message broker monitoring implementations
#[cfg_attr(test, mockall::automock)]
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::collections::HashMap;

    // ========================================================================
    // MOCK IMPLEMENTATIONS FOR TESTING
    // ========================================================================

    /// Mock broker monitor for testing - simulates successful operations
    #[derive(Debug, Clone)]
    struct MockBrokerMonitor {
        metrics_store: Arc<Mutex<HashMap<String, QueueMetrics>>>,
        health_status: Arc<Mutex<bool>>,
        call_count: Arc<Mutex<usize>>,
    }

    impl MockBrokerMonitor {
        fn new() -> Self {
            Self {
                metrics_store: Arc::new(Mutex::new(HashMap::new())),
                health_status: Arc::new(Mutex::new(true)),
                call_count: Arc::new(Mutex::new(0)),
            }
        }

        fn with_health_status(health: bool) -> Self {
            Self {
                metrics_store: Arc::new(Mutex::new(HashMap::new())),
                health_status: Arc::new(Mutex::new(health)),
                call_count: Arc::new(Mutex::new(0)),
            }
        }

        fn add_queue_metrics(&self, queue_name: &str, metrics: QueueMetrics) {
            let mut store = self.metrics_store.lock().unwrap();
            store.insert(queue_name.to_string(), metrics);
        }

        fn get_call_count(&self) -> usize {
            *self.call_count.lock().unwrap()
        }
    }

    #[async_trait]
    impl BrokerMonitor for MockBrokerMonitor {
        async fn get_queue_metrics(&mut self, queue_name: &str) -> Result<QueueMetrics, MonitoringError> {
            // Increment call count
            {
                let mut count = self.call_count.lock().unwrap();
                *count += 1;
            }

            let store = self.metrics_store.lock().unwrap();
            store.get(queue_name).cloned().ok_or_else(|| {
                MonitoringError::BrokerAPIError(format!("Queue '{}' not found", queue_name))
            })
        }

        async fn health_check(&mut self) -> Result<bool> {
            let status = *self.health_status.lock().unwrap();
            Ok(status)
        }

        fn broker_type(&self) -> &'static str {
            "MockBroker"
        }
    }

    /// Mock broker that always fails - for error testing
    #[derive(Debug, Clone)]
    struct FailingBrokerMonitor {
        error_message: String,
    }

    impl FailingBrokerMonitor {
        fn new(error_message: &str) -> Self {
            Self {
                error_message: error_message.to_string(),
            }
        }
    }

    #[async_trait]
    impl BrokerMonitor for FailingBrokerMonitor {
        async fn get_queue_metrics(&mut self, _queue_name: &str) -> Result<QueueMetrics, MonitoringError> {
            Err(MonitoringError::BrokerAPIError(self.error_message.clone()))
        }

        async fn health_check(&mut self) -> Result<bool> {
            Ok(false)
        }

        fn broker_type(&self) -> &'static str {
            "FailingBroker"
        }
    }

    /// Mock broker with delay - for concurrency testing
    #[derive(Debug, Clone)]
    struct DelayedBrokerMonitor {
        delay_ms: u64,
        metrics: QueueMetrics,
    }

    impl DelayedBrokerMonitor {
        fn new(delay_ms: u64, metrics: QueueMetrics) -> Self {
            Self { delay_ms, metrics }
        }
    }

    #[async_trait]
    impl BrokerMonitor for DelayedBrokerMonitor {
        async fn get_queue_metrics(&mut self, _queue_name: &str) -> Result<QueueMetrics, MonitoringError> {
            tokio::time::sleep(tokio::time::Duration::from_millis(self.delay_ms)).await;
            Ok(self.metrics.clone())
        }

        async fn health_check(&mut self) -> Result<bool> {
            Ok(true)
        }

        fn broker_type(&self) -> &'static str {
            "DelayedBroker"
        }
    }

    // ========================================================================
    // HELPER FUNCTIONS
    // ========================================================================

    /// Create a sample QueueMetrics for testing
    fn create_sample_metrics(queue_name: &str, queue_size: u32, consumer_count: u32) -> QueueMetrics {
        QueueMetrics {
            queue_name: queue_name.to_string(),
            queue_size,
            consumer_count,
            enqueue_count: 1000,
            dequeue_count: 900,
            memory_percent_usage: 45.5,
        }
    }

    /// Create metrics with specific values for edge case testing
    fn create_edge_case_metrics(queue_name: &str) -> QueueMetrics {
        QueueMetrics {
            queue_name: queue_name.to_string(),
            queue_size: u32::MAX,
            consumer_count: 0,
            enqueue_count: u64::MAX,
            dequeue_count: u64::MAX,
            memory_percent_usage: 100.0,
        }
    }

    // ========================================================================
    // BASIC FUNCTIONALITY TESTS
    // ========================================================================

    #[tokio::test]
    async fn test_get_queue_metrics_success() {
        // Test successful retrieval of queue metrics
        let mut monitor = MockBrokerMonitor::new();
        let expected_metrics = create_sample_metrics("test.queue", 100, 5);
        monitor.add_queue_metrics("test.queue", expected_metrics.clone());

        let result = monitor.get_queue_metrics("test.queue").await;
        
        assert!(result.is_ok(), "Should successfully get queue metrics");
        let metrics = result.unwrap();
        assert_eq!(metrics.queue_name, "test.queue");
        assert_eq!(metrics.queue_size, 100);
        assert_eq!(metrics.consumer_count, 5);
    }

    #[tokio::test]
    async fn test_get_queue_metrics_not_found() {
        // Test retrieval of non-existent queue
        let mut monitor = MockBrokerMonitor::new();
        
        let result = monitor.get_queue_metrics("nonexistent.queue").await;
        
        assert!(result.is_err(), "Should return error for non-existent queue");
        match result {
            Err(MonitoringError::BrokerAPIError(msg)) => {
                assert!(msg.contains("not found"), "Error should mention 'not found'");
            }
            _ => panic!("Expected BrokerAPIError"),
        }
    }

    #[tokio::test]
    async fn test_health_check_healthy() {
        // Test health check when broker is healthy
        let mut monitor = MockBrokerMonitor::with_health_status(true);
        
        let result = monitor.health_check().await;
        
        assert!(result.is_ok(), "Health check should succeed");
        assert_eq!(result.unwrap(), true, "Should return healthy status");
    }

    #[tokio::test]
    async fn test_health_check_unhealthy() {
        // Test health check when broker is unhealthy
        let mut monitor = MockBrokerMonitor::with_health_status(false);
        
        let result = monitor.health_check().await;
        
        assert!(result.is_ok(), "Health check should succeed (but return false)");
        assert_eq!(result.unwrap(), false, "Should return unhealthy status");
    }

    #[tokio::test]
    async fn test_broker_type() {
        // Test broker_type method returns correct string
        let monitor = MockBrokerMonitor::new();
        
        assert_eq!(monitor.broker_type(), "MockBroker");
    }

    // ========================================================================
    // EDGE CASE TESTS
    // ========================================================================

    #[tokio::test]
    async fn test_empty_queue_name() {
        // Test with empty queue name
        let mut monitor = MockBrokerMonitor::new();
        let metrics = create_sample_metrics("", 0, 0);
        monitor.add_queue_metrics("", metrics);

        let result = monitor.get_queue_metrics("").await;
        
        assert!(result.is_ok(), "Should handle empty queue name");
        assert_eq!(result.unwrap().queue_name, "");
    }

    #[tokio::test]
    async fn test_queue_name_with_special_characters() {
        // Test queue names with special characters
        let mut monitor = MockBrokerMonitor::new();
        let special_names = vec![
            "queue-with-dashes",
            "queue.with.dots",
            "queue_with_underscores",
            "queue::with::colons",
            "queue/with/slashes",
            "queue with spaces",
        ];

        for name in &special_names {
            let metrics = create_sample_metrics(name, 10, 1);
            monitor.add_queue_metrics(name, metrics);
        }

        for name in &special_names {
            let result = monitor.get_queue_metrics(name).await;
            assert!(result.is_ok(), "Should handle queue name: {}", name);
        }
    }

    #[tokio::test]
    async fn test_very_long_queue_name() {
        // Test with very long queue name (1000 characters)
        let mut monitor = MockBrokerMonitor::new();
        let long_name = "a".repeat(1000);
        let metrics = create_sample_metrics(&long_name, 50, 2);
        monitor.add_queue_metrics(&long_name, metrics);

        let result = monitor.get_queue_metrics(&long_name).await;
        
        assert!(result.is_ok(), "Should handle very long queue name");
        assert_eq!(result.unwrap().queue_name.len(), 1000);
    }

    #[tokio::test]
    async fn test_metrics_with_zero_values() {
        // Test metrics with all zero values
        let mut monitor = MockBrokerMonitor::new();
        let metrics = QueueMetrics {
            queue_name: "empty.queue".to_string(),
            queue_size: 0,
            consumer_count: 0,
            enqueue_count: 0,
            dequeue_count: 0,
            memory_percent_usage: 0.0,
        };
        monitor.add_queue_metrics("empty.queue", metrics);

        let result = monitor.get_queue_metrics("empty.queue").await;
        
        assert!(result.is_ok());
        let retrieved = result.unwrap();
        assert_eq!(retrieved.queue_size, 0);
        assert_eq!(retrieved.consumer_count, 0);
        assert_eq!(retrieved.memory_percent_usage, 0.0);
    }

    #[tokio::test]
    async fn test_metrics_with_max_values() {
        // Test metrics with maximum boundary values
        let mut monitor = MockBrokerMonitor::new();
        let metrics = create_edge_case_metrics("max.queue");
        monitor.add_queue_metrics("max.queue", metrics);

        let result = monitor.get_queue_metrics("max.queue").await;
        
        assert!(result.is_ok());
        let retrieved = result.unwrap();
        assert_eq!(retrieved.queue_size, u32::MAX);
        assert_eq!(retrieved.enqueue_count, u64::MAX);
        assert_eq!(retrieved.dequeue_count, u64::MAX);
    }

    // ========================================================================
    // DEFAULT IMPLEMENTATION TESTS (get_multiple_queue_metrics)
    // ========================================================================

    #[tokio::test]
    async fn test_get_multiple_queue_metrics_empty_list() {
        // Test with empty queue list
        let mut monitor = MockBrokerMonitor::new();
        let queue_names: Vec<String> = vec![];

        let results = monitor.get_multiple_queue_metrics(&queue_names).await;
        
        assert_eq!(results.len(), 0, "Should return empty results for empty input");
    }

    #[tokio::test]
    async fn test_get_multiple_queue_metrics_single_queue() {
        // Test with single queue
        let mut monitor = MockBrokerMonitor::new();
        let metrics = create_sample_metrics("single.queue", 100, 5);
        monitor.add_queue_metrics("single.queue", metrics);
        
        let queue_names = vec!["single.queue".to_string()];
        let results = monitor.get_multiple_queue_metrics(&queue_names).await;
        
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0, "single.queue");
        assert!(results[0].1.is_ok());
    }

    #[tokio::test]
    async fn test_get_multiple_queue_metrics_multiple_queues() {
        // Test with multiple queues - all successful
        let mut monitor = MockBrokerMonitor::new();
        
        let queues = vec!["queue1", "queue2", "queue3"];
        for (i, queue) in queues.iter().enumerate() {
            let metrics = create_sample_metrics(queue, (i as u32 + 1) * 10, i as u32 + 1);
            monitor.add_queue_metrics(queue, metrics);
        }

        let queue_names: Vec<String> = queues.iter().map(|s| s.to_string()).collect();
        let results = monitor.get_multiple_queue_metrics(&queue_names).await;
        
        assert_eq!(results.len(), 3);
        for (i, (name, result)) in results.iter().enumerate() {
            assert_eq!(name, &queues[i]);
            assert!(result.is_ok(), "Queue {} should succeed", queues[i]);
        }
    }

    #[tokio::test]
    async fn test_get_multiple_queue_metrics_mixed_results() {
        // Test with mix of successful and failed queries
        let mut monitor = MockBrokerMonitor::new();
        
        // Add only some queues
        monitor.add_queue_metrics("exists1", create_sample_metrics("exists1", 50, 2));
        monitor.add_queue_metrics("exists2", create_sample_metrics("exists2", 75, 3));

        let queue_names = vec![
            "exists1".to_string(),
            "notfound".to_string(),
            "exists2".to_string(),
        ];
        let results = monitor.get_multiple_queue_metrics(&queue_names).await;
        
        assert_eq!(results.len(), 3);
        assert!(results[0].1.is_ok(), "First queue should succeed");
        assert!(results[1].1.is_err(), "Second queue should fail");
        assert!(results[2].1.is_ok(), "Third queue should succeed");
    }

    #[tokio::test]
    async fn test_get_multiple_queue_metrics_all_fail() {
        // Test when all queries fail
        let mut monitor = MockBrokerMonitor::new();
        
        let queue_names = vec![
            "notfound1".to_string(),
            "notfound2".to_string(),
            "notfound3".to_string(),
        ];
        let results = monitor.get_multiple_queue_metrics(&queue_names).await;
        
        assert_eq!(results.len(), 3);
        for (name, result) in &results {
            assert!(result.is_err(), "Queue {} should fail", name);
        }
    }

    #[tokio::test]
    async fn test_get_multiple_queue_metrics_sequential_execution() {
        // Test that default implementation executes sequentially
        let mut monitor = MockBrokerMonitor::new();
        
        for i in 1..=5 {
            let queue_name = format!("queue{}", i);
            monitor.add_queue_metrics(&queue_name, create_sample_metrics(&queue_name, i * 10, i));
        }

        let queue_names: Vec<String> = (1..=5).map(|i| format!("queue{}", i)).collect();
        let results = monitor.get_multiple_queue_metrics(&queue_names).await;
        
        // Verify all results returned in order
        assert_eq!(results.len(), 5);
        for (i, (name, result)) in results.iter().enumerate() {
            assert_eq!(name, &format!("queue{}", i + 1));
            assert!(result.is_ok());
        }
        
        // Verify call count equals number of queues
        assert_eq!(monitor.get_call_count(), 5);
    }

    // ========================================================================
    // ERROR HANDLING TESTS
    // ========================================================================

    #[tokio::test]
    async fn test_failing_broker_get_queue_metrics() {
        // Test broker that always fails
        let mut monitor = FailingBrokerMonitor::new("Connection timeout");
        
        let result = monitor.get_queue_metrics("any.queue").await;
        
        assert!(result.is_err());
        match result {
            Err(MonitoringError::BrokerAPIError(msg)) => {
                assert_eq!(msg, "Connection timeout");
            }
            _ => panic!("Expected BrokerAPIError"),
        }
    }

    #[tokio::test]
    async fn test_failing_broker_health_check() {
        // Test health check on failing broker
        let mut monitor = FailingBrokerMonitor::new("Service unavailable");
        
        let result = monitor.health_check().await;
        
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), false);
    }

    #[tokio::test]
    async fn test_error_message_propagation() {
        // Test that error messages are properly propagated
        let error_messages = vec![
            "Network error",
            "Authentication failed",
            "Queue does not exist",
            "Permission denied",
            "Rate limit exceeded",
        ];

        for error_msg in error_messages {
            let mut monitor = FailingBrokerMonitor::new(error_msg);
            let result = monitor.get_queue_metrics("test").await;
            
            assert!(result.is_err());
            match result {
                Err(MonitoringError::BrokerAPIError(msg)) => {
                    assert_eq!(msg, error_msg);
                }
                _ => panic!("Expected BrokerAPIError with message: {}", error_msg),
            }
        }
    }

    // ========================================================================
    // CONCURRENCY TESTS
    // ========================================================================

    #[tokio::test]
    async fn test_concurrent_health_checks() {
        // Test multiple concurrent health checks
        let monitor = MockBrokerMonitor::with_health_status(true);
        
        let mut handles = vec![];
        for _ in 0..10 {
            let mut mon = monitor.clone();
            let handle = tokio::spawn(async move {
                mon.health_check().await
            });
            handles.push(handle);
        }

        // All health checks should succeed
        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
            assert_eq!(result.unwrap(), true);
        }
    }

    #[tokio::test]
    async fn test_concurrent_queue_metrics_retrieval() {
        // Test concurrent retrieval of different queue metrics
        let monitor = MockBrokerMonitor::new();
        
        // Add multiple queues
        for i in 1..=10 {
            let queue_name = format!("queue{}", i);
            monitor.add_queue_metrics(&queue_name, create_sample_metrics(&queue_name, i * 10, i));
        }

        let mut handles = vec![];
        for i in 1..=10 {
            let mut mon = monitor.clone();
            let queue_name = format!("queue{}", i);
            let handle = tokio::spawn(async move {
                mon.get_queue_metrics(&queue_name).await
            });
            handles.push(handle);
        }

        // All retrievals should succeed
        for (i, handle) in handles.into_iter().enumerate() {
            let result = handle.await.unwrap();
            assert!(result.is_ok(), "Queue {} should succeed", i + 1);
        }
    }

    #[tokio::test]
    async fn test_delayed_broker_timing() {
        // Test that delayed broker actually delays
        use std::time::Instant;
        
        let metrics = create_sample_metrics("delayed.queue", 100, 5);
        let mut monitor = DelayedBrokerMonitor::new(100, metrics);
        
        let start = Instant::now();
        let result = monitor.get_queue_metrics("delayed.queue").await;
        let elapsed = start.elapsed();
        
        assert!(result.is_ok());
        assert!(elapsed.as_millis() >= 100, "Should delay at least 100ms");
    }

    // ========================================================================
    // TRAIT BOUNDS TESTS
    // ========================================================================

    #[tokio::test]
    async fn test_send_bound() {
        // Test that BrokerMonitor can be sent across threads
        let monitor = MockBrokerMonitor::new();
        monitor.add_queue_metrics("test.queue", create_sample_metrics("test.queue", 100, 5));
        
        let handle = tokio::spawn(async move {
            let mut mon = monitor;
            mon.get_queue_metrics("test.queue").await
        });

        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sync_bound() {
        // Test that BrokerMonitor can be shared across threads
        let monitor = Arc::new(tokio::sync::Mutex::new(MockBrokerMonitor::new()));
        monitor.lock().await.add_queue_metrics("shared.queue", create_sample_metrics("shared.queue", 50, 2));
        
        let mut handles = vec![];
        for _ in 0..5 {
            let mon = monitor.clone();
            let handle = tokio::spawn(async move {
                let mut guard = mon.lock().await;
                guard.get_queue_metrics("shared.queue").await
            });
            handles.push(handle);
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }
    }

    // ========================================================================
    // INTEGRATION-STYLE TESTS
    // ========================================================================

    #[tokio::test]
    async fn test_typical_monitoring_workflow() {
        // Test a typical monitoring workflow
        let mut monitor = MockBrokerMonitor::new();
        
        // 1. Health check first
        let health = monitor.health_check().await.unwrap();
        assert!(health, "Broker should be healthy");
        
        // 2. Get broker type
        let broker_type = monitor.broker_type();
        assert_eq!(broker_type, "MockBroker");
        
        // 3. Add and retrieve queue metrics
        monitor.add_queue_metrics("app.queue", create_sample_metrics("app.queue", 200, 10));
        let metrics = monitor.get_queue_metrics("app.queue").await.unwrap();
        assert_eq!(metrics.queue_size, 200);
        assert_eq!(metrics.consumer_count, 10);
    }

    #[tokio::test]
    async fn test_monitoring_multiple_queues_workflow() {
        // Test workflow for monitoring multiple queues
        let mut monitor = MockBrokerMonitor::new();
        
        // Setup multiple queues with different characteristics
        let queue_configs = vec![
            ("high_traffic.queue", 1000, 20),
            ("medium_traffic.queue", 500, 10),
            ("low_traffic.queue", 50, 2),
            ("idle.queue", 0, 0),
        ];
        
        for (name, size, consumers) in &queue_configs {
            monitor.add_queue_metrics(name, create_sample_metrics(name, *size, *consumers));
        }
        
        // Retrieve all metrics
        let queue_names: Vec<String> = queue_configs.iter().map(|(n, _, _)| n.to_string()).collect();
        let results = monitor.get_multiple_queue_metrics(&queue_names).await;
        
        assert_eq!(results.len(), queue_configs.len());
        for ((expected_name, expected_size, expected_consumers), (actual_name, result)) in 
            queue_configs.iter().zip(results.iter()) {
            assert_eq!(actual_name, expected_name);
            assert!(result.is_ok());
            
            let metrics = result.as_ref().unwrap();
            assert_eq!(metrics.queue_size, *expected_size);
            assert_eq!(metrics.consumer_count, *expected_consumers);
        }
    }

    #[tokio::test]
    async fn test_error_recovery_workflow() {
        // Test workflow that handles errors gracefully
        let mut monitor = MockBrokerMonitor::new();
        
        // Add only one queue
        monitor.add_queue_metrics("existing.queue", create_sample_metrics("existing.queue", 100, 5));
        
        // Try to get metrics for multiple queues (some don't exist)
        let queue_names = vec![
            "existing.queue".to_string(),
            "missing1.queue".to_string(),
            "missing2.queue".to_string(),
        ];
        
        let results = monitor.get_multiple_queue_metrics(&queue_names).await;
        
        // Should have results for all queries
        assert_eq!(results.len(), 3);
        
        // First should succeed
        assert!(results[0].1.is_ok());
        
        // Others should fail but not crash
        assert!(results[1].1.is_err());
        assert!(results[2].1.is_err());
        
        // Should still be able to perform other operations
        let health = monitor.health_check().await;
        assert!(health.is_ok());
    }

    // ========================================================================
    // PERFORMANCE-RELATED TESTS
    // ========================================================================

    #[tokio::test]
    async fn test_large_number_of_queues() {
        // Test handling of large number of queues
        let mut monitor = MockBrokerMonitor::new();
        
        let queue_count = 100;
        for i in 0..queue_count {
            let queue_name = format!("queue{}", i);
            monitor.add_queue_metrics(&queue_name, create_sample_metrics(&queue_name, i, 1));
        }
        
        let queue_names: Vec<String> = (0..queue_count).map(|i| format!("queue{}", i)).collect();
        let results = monitor.get_multiple_queue_metrics(&queue_names).await;
        
        assert_eq!(results.len(), queue_count as usize);
        for (_, result) in &results {
            assert!(result.is_ok());
        }
    }

    #[tokio::test]
    async fn test_rapid_successive_calls() {
        // Test rapid successive calls to the same method
        let mut monitor = MockBrokerMonitor::new();
        monitor.add_queue_metrics("rapid.queue", create_sample_metrics("rapid.queue", 100, 5));
        
        for _ in 0..50 {
            let result = monitor.get_queue_metrics("rapid.queue").await;
            assert!(result.is_ok());
        }
        
        assert_eq!(monitor.get_call_count(), 50);
    }
}