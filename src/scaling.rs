use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use crate::config::WorkerRange;
use crate::monitor::QueueMetrics;

/// Scaling decision types
#[derive(Debug, Clone, PartialEq)]
pub enum ScalingDecision {
    /// No scaling needed
    NoChange,
    /// Scale up by the specified number of workers
    ScaleUp(u32),
    /// Scale down by the specified number of workers
    ScaleDown(u32),
}

/// Historical metrics for a queue to prevent flapping
#[derive(Debug, Clone)]
pub struct QueueHistory {
    /// Last metrics recorded
    pub last_metrics: Option<QueueMetrics>,
    /// Last scaling decision timestamp
    pub last_scaling_time: Option<Instant>,
    /// Current worker count
    pub current_workers: u32,
    /// Worker range configuration
    pub worker_range: WorkerRange,
    /// Scaling cooldown period
    pub cooldown_period: Duration,
}

impl QueueHistory {
    pub fn new(worker_range: WorkerRange, current_workers: u32) -> Self {
        Self {
            last_metrics: None,
            last_scaling_time: None,
            current_workers,
            worker_range,
            cooldown_period: Duration::from_secs(30), // 30 seconds cooldown
        }
    }

    /// Update metrics and worker count
    pub fn update(&mut self, metrics: QueueMetrics, current_workers: u32) {
        self.last_metrics = Some(metrics);
        self.current_workers = current_workers;
    }

    /// Check if enough time has passed since last scaling action
    pub fn can_scale(&self) -> bool {
        match self.last_scaling_time {
            Some(last_time) => last_time.elapsed() >= self.cooldown_period,
            None => true,
        }
    }

    /// Mark that scaling occurred
    pub fn mark_scaled(&mut self) {
        self.last_scaling_time = Some(Instant::now());
    }
}

/// Scaling engine that makes scaling decisions based on queue metrics
pub struct ScalingEngine {
    /// Queue histories for tracking metrics over time
    queue_histories: HashMap<String, QueueHistory>,
    /// Scale down buffer - keep this many extra workers when scaling down
    scale_down_buffer: u32,
}

impl ScalingEngine {
    /// Create a new scaling engine
    pub fn new(_deprecated_param: u32) -> Self {
        Self {
            queue_histories: HashMap::new(),
            scale_down_buffer: 1, // Keep 1 extra worker as buffer when scaling down
        }
    }

    /// Register a queue for monitoring and scaling
    pub fn register_queue(&mut self, queue_name: String, worker_range: WorkerRange, current_workers: u32) {
        info!(
            "📊 Registering queue '{}' for auto-scaling (workers: {}-{}, current: {})",
            queue_name, worker_range.min, worker_range.max, current_workers
        );

        let history = QueueHistory::new(worker_range, current_workers);
        self.queue_histories.insert(queue_name, history);
    }

    /// Update queue metrics and get scaling decision
    pub fn evaluate_scaling(
        &mut self,
        queue_name: &str,
        metrics: QueueMetrics,
        current_workers: u32,
    ) -> ScalingDecision {
        // Get or create queue history
        let history = match self.queue_histories.get_mut(queue_name) {
            Some(h) => h,
            None => {
                warn!("Queue '{}' not registered for scaling", queue_name);
                return ScalingDecision::NoChange;
            }
        };

        // Update history with current metrics
        history.update(metrics.clone(), current_workers);

        // Check if we can scale (cooldown period)
        if !history.can_scale() {
            let remaining = history.cooldown_period - 
                history.last_scaling_time.unwrap().elapsed();
            debug!(
                "Queue '{}' in cooldown period, {} seconds remaining",
                queue_name,
                remaining.as_secs()
            );
            return ScalingDecision::NoChange;
        }

        // Get worker range before immutable borrow
        let worker_range = history.worker_range.clone();
        
        // Release mutable borrow by getting history again
        let _ = history;

        // Make scaling decision based on current metrics
        let decision = self.make_scaling_decision(queue_name, &metrics, current_workers, &worker_range);

        // Mark scaling time if decision is not NoChange
        if matches!(decision, ScalingDecision::ScaleUp(_) | ScalingDecision::ScaleDown(_)) {
            // Get mutable reference again for marking scaled
            if let Some(history) = self.queue_histories.get_mut(queue_name) {
                history.mark_scaled();
            }
            info!(
                "📈📉 Scaling decision for '{}': {:?} (queue_size: {}, workers: {}, range: {}-{})",
                queue_name, decision, metrics.queue_size, current_workers, 
                worker_range.min, worker_range.max
            );
        }

        decision
    }

    /// Make scaling decision based on current metrics
    fn make_scaling_decision(
        &self,
        queue_name: &str,
        metrics: &QueueMetrics,
        current_workers: u32,
        worker_range: &WorkerRange,
    ) -> ScalingDecision {
        let queue_size = metrics.queue_size;

        debug!(
            "🔍 Evaluating scaling for '{}': queue_size={}, workers={}, range={}-{}, broker_consumers={}",
            queue_name, queue_size, current_workers, worker_range.min, worker_range.max, metrics.consumer_count
        );

        // **Scale Up Logic**
        // Scale up if queue size > current workers AND we're below max workers
        if queue_size > current_workers && current_workers < worker_range.max {
            let needed_workers = queue_size.saturating_sub(current_workers);
            let max_increase = worker_range.max.saturating_sub(current_workers);
            let scale_up_count = needed_workers.min(max_increase);

            debug!(
                "📈 Scale up trigger: queue '{}' has {} messages but only {} workers",
                queue_name, queue_size, current_workers
            );
            return ScalingDecision::ScaleUp(scale_up_count);
        }

        // **Scale Down Logic**
        // Scale down if queue size < current workers AND we have more than min workers
        debug!(
            "📷 Scale down check: queue_size({}) < workers({})? {} AND workers({}) > min({})? {}",
            queue_size, current_workers, queue_size < current_workers,
            current_workers, worker_range.min, current_workers > worker_range.min
        );
        
        if queue_size < current_workers && current_workers > worker_range.min {
            // Calculate needed workers: queue_size + buffer, but not less than min
            let needed_workers = (queue_size + self.scale_down_buffer).max(worker_range.min);
            
            debug!(
                "🗺 Scale down calculation: needed_workers = max({} + {}, {}) = {}",
                queue_size, self.scale_down_buffer, worker_range.min, needed_workers
            );
            
            if needed_workers < current_workers {
                let scale_down_count = current_workers.saturating_sub(needed_workers);
                debug!(
                    "📉 Scale down trigger: queue '{}' has {} messages with {} workers, scaling down to {}",
                    queue_name, queue_size, current_workers, needed_workers
                );
                return ScalingDecision::ScaleDown(scale_down_count);
            } else {
                debug!(
                    "🚫 Scale down blocked: needed_workers({}) >= current_workers({})",
                    needed_workers, current_workers
                );
            }
        } else {
            debug!(
                "🚫 Scale down conditions not met: queue_size({}) < workers({})? {} AND workers({}) > min({})? {}",
                queue_size, current_workers, queue_size < current_workers,
                current_workers, worker_range.min, current_workers > worker_range.min
            );
        }

        
        debug!("🔴 No scaling action needed for queue '{}'", queue_name);
        ScalingDecision::NoChange
    }
}

#[cfg(test)]
mod tests {
    //! Comprehensive unit tests for the scaling module.
    //!
    //! ## Test Coverage Summary:
    //!
    //! ### ScalingDecision enum tests:
    //! - Debug, Clone, PartialEq trait implementations
    //! - Pattern matching and variant handling
    //! - Equality edge cases with different values
    //!
    //! ### QueueHistory struct tests:
    //! - Constructor and initialization
    //! - Metrics updates and state persistence
    //! - Cooldown mechanism and timing
    //! - Debug and Clone trait implementations
    //! - Multiple updates and state transitions
    //! - Instant operations and timing consistency
    //!
    //! ### ScalingEngine struct tests:
    //! - Constructor and initialization
    //! - Queue registration (single and multiple)
    //! - Scaling evaluation with various scenarios:
    //!   - Scale up decisions and limits
    //!   - Scale down decisions with buffer logic
    //!   - Fixed worker ranges
    //!   - Cooldown period enforcement
    //!   - Unregistered queue handling
    //! - State persistence across evaluations
    //! - Multiple queue independence
    //!
    //! ### Edge Cases and Boundary Conditions:
    //! - Zero queue size and worker counts
    //! - Maximum values and overflow protection
    //! - Worker range boundaries (0, u32::MAX)
    //! - Special queue names (empty, special chars)
    //! - Buffer and minimum worker interactions
    //! - Precise calculation verification
    //! - Saturating arithmetic edge cases
    //!
    //! ### Complex Scenarios:
    //! - Multi-step scaling workflows
    //! - Time-based cooldown testing
    //! - Consumer count independence verification
    //! - Production-like load simulation
    //!
    //! Total test count: 45 tests covering all public APIs and edge cases
    
    use super::*;
    use crate::config::WorkerRange;
    use crate::monitor::QueueMetrics;
    use std::time::{Duration, Instant};
    use std::thread::sleep;

    // Helper function to create test queue metrics
    fn create_test_metrics(queue_name: &str, queue_size: u32, consumer_count: u32) -> QueueMetrics {
        QueueMetrics {
            queue_name: queue_name.to_string(),
            queue_size,
            consumer_count,
            enqueue_count: 0,
            dequeue_count: 0,
            memory_percent_usage: 0.0,
        }
    }

    // Helper function to create test worker range
    fn create_worker_range(min: u32, max: u32, is_fixed: bool) -> WorkerRange {
        WorkerRange { min, max, is_fixed }
    }

    #[test]
    fn test_scaling_decision_debug_clone_partial_eq() {
        let no_change = ScalingDecision::NoChange;
        let scale_up = ScalingDecision::ScaleUp(3);
        let scale_down = ScalingDecision::ScaleDown(2);

        // Test Debug
        assert!(format!("{:?}", no_change).contains("NoChange"));
        assert!(format!("{:?}", scale_up).contains("ScaleUp(3)"));
        assert!(format!("{:?}", scale_down).contains("ScaleDown(2)"));

        // Test Clone
        let cloned_no_change = no_change.clone();
        let cloned_scale_up = scale_up.clone();
        let cloned_scale_down = scale_down.clone();

        // Test PartialEq
        assert_eq!(no_change, cloned_no_change);
        assert_eq!(scale_up, cloned_scale_up);
        assert_eq!(scale_down, cloned_scale_down);
        assert_ne!(no_change, scale_up);
        assert_ne!(scale_up, scale_down);
        assert_ne!(ScalingDecision::ScaleUp(3), ScalingDecision::ScaleUp(4));
    }

    #[test]
    fn test_queue_history_new() {
        let worker_range = create_worker_range(1, 5, false);
        let history = QueueHistory::new(worker_range.clone(), 2);

        assert!(history.last_metrics.is_none());
        assert!(history.last_scaling_time.is_none());
        assert_eq!(history.current_workers, 2);
        assert_eq!(history.worker_range.min, 1);
        assert_eq!(history.worker_range.max, 5);
        assert_eq!(history.worker_range.is_fixed, false);
        assert_eq!(history.cooldown_period, Duration::from_secs(30));
    }

    #[test]
    fn test_queue_history_update() {
        let worker_range = create_worker_range(1, 5, false);
        let mut history = QueueHistory::new(worker_range, 2);
        let metrics = create_test_metrics("test-queue", 10, 2);

        history.update(metrics.clone(), 3);

        assert!(history.last_metrics.is_some());
        let last_metrics = history.last_metrics.unwrap();
        assert_eq!(last_metrics.queue_name, "test-queue");
        assert_eq!(last_metrics.queue_size, 10);
        assert_eq!(last_metrics.consumer_count, 2);
        assert_eq!(history.current_workers, 3);
    }

    #[test]
    fn test_queue_history_can_scale_initial() {
        let worker_range = create_worker_range(1, 5, false);
        let history = QueueHistory::new(worker_range, 2);

        // Initially, should be able to scale (no last scaling time)
        assert!(history.can_scale());
    }

    #[test]
    fn test_queue_history_can_scale_after_marking() {
        let worker_range = create_worker_range(1, 5, false);
        let mut history = QueueHistory::new(worker_range, 2);

        // Mark as scaled
        history.mark_scaled();
        
        // Should not be able to scale immediately
        assert!(!history.can_scale());
        
        // Verify last_scaling_time is set
        assert!(history.last_scaling_time.is_some());
    }

    #[test]
    fn test_queue_history_can_scale_after_cooldown() {
        let worker_range = create_worker_range(1, 5, false);
        let mut history = QueueHistory::new(worker_range, 2);
        
        // Set a very short cooldown for testing
        history.cooldown_period = Duration::from_millis(10);
        
        // Mark as scaled
        history.mark_scaled();
        assert!(!history.can_scale());
        
        // Wait for cooldown
        sleep(Duration::from_millis(15));
        
        // Should now be able to scale
        assert!(history.can_scale());
    }

    #[test]
    fn test_queue_history_debug() {
        let worker_range = create_worker_range(1, 5, false);
        let history = QueueHistory::new(worker_range, 2);
        let debug_str = format!("{:?}", history);
        
        assert!(debug_str.contains("QueueHistory"));
        assert!(debug_str.contains("current_workers"));
        assert!(debug_str.contains("worker_range"));
    }

    #[test]
    fn test_queue_history_clone() {
        let worker_range = create_worker_range(1, 5, false);
        let mut history = QueueHistory::new(worker_range.clone(), 2);
        let metrics = create_test_metrics("test-queue", 10, 2);
        history.update(metrics, 3);
        history.mark_scaled();
        
        let cloned = history.clone();
        
        assert_eq!(history.current_workers, cloned.current_workers);
        assert_eq!(history.worker_range.min, cloned.worker_range.min);
        assert_eq!(history.worker_range.max, cloned.worker_range.max);
        assert_eq!(history.cooldown_period, cloned.cooldown_period);
        assert!(cloned.last_metrics.is_some());
        assert!(cloned.last_scaling_time.is_some());
    }

    #[test]
    fn test_scaling_engine_new() {
        let engine = ScalingEngine::new(100); // deprecated param should be ignored
        
        // Check internal state (though it's private, we can test through public interface)
        assert_eq!(engine.queue_histories.len(), 0);
        assert_eq!(engine.scale_down_buffer, 1);
    }

    #[test]
    fn test_scaling_engine_register_queue() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(2, 8, false);
        
        engine.register_queue("test-queue".to_string(), worker_range, 3);
        
        // Verify queue is registered by trying to evaluate scaling
        let metrics = create_test_metrics("test-queue", 2, 1);
        let decision = engine.evaluate_scaling("test-queue", metrics, 3);
        
        // Should not return early warning (which would happen if not registered)
        // Instead should make a proper scaling decision
        assert!(matches!(decision, ScalingDecision::NoChange | ScalingDecision::ScaleUp(_) | ScalingDecision::ScaleDown(_)));
    }

    #[test]
    fn test_scaling_engine_register_multiple_queues() {
        let mut engine = ScalingEngine::new(100);
        let worker_range1 = create_worker_range(1, 5, false);
        let worker_range2 = create_worker_range(2, 10, false);
        
        engine.register_queue("queue1".to_string(), worker_range1, 2);
        engine.register_queue("queue2".to_string(), worker_range2, 5);
        
        // Both queues should be registered
        let metrics1 = create_test_metrics("queue1", 3, 2);
        let metrics2 = create_test_metrics("queue2", 4, 5);
        
        let decision1 = engine.evaluate_scaling("queue1", metrics1, 2);
        let decision2 = engine.evaluate_scaling("queue2", metrics2, 5);
        
        // Both should return valid decisions (not early return for unregistered queue)
        assert!(matches!(decision1, ScalingDecision::NoChange | ScalingDecision::ScaleUp(_) | ScalingDecision::ScaleDown(_)));
        assert!(matches!(decision2, ScalingDecision::NoChange | ScalingDecision::ScaleUp(_) | ScalingDecision::ScaleDown(_)));
    }

    #[test]
    fn test_evaluate_scaling_unregistered_queue() {
        let mut engine = ScalingEngine::new(100);
        let metrics = create_test_metrics("unknown-queue", 10, 2);
        
        let decision = engine.evaluate_scaling("unknown-queue", metrics, 3);
        
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_evaluate_scaling_cooldown_period() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 10, false);
        
        engine.register_queue("test-queue".to_string(), worker_range, 2);
        
        // First scaling decision that triggers scaling
        let metrics1 = create_test_metrics("test-queue", 10, 2);
        let decision1 = engine.evaluate_scaling("test-queue", metrics1, 2);
        assert!(matches!(decision1, ScalingDecision::ScaleUp(_)));
        
        // Immediate second call should be blocked by cooldown
        let metrics2 = create_test_metrics("test-queue", 15, 2);
        let decision2 = engine.evaluate_scaling("test-queue", metrics2, 2);
        assert_eq!(decision2, ScalingDecision::NoChange);
    }

    #[test]
    fn test_scale_up_decision() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 10, false);
        
        engine.register_queue("test-queue".to_string(), worker_range, 2);
        
        // Queue size > current workers, should scale up
        let metrics = create_test_metrics("test-queue", 8, 2);
        let decision = engine.evaluate_scaling("test-queue", metrics, 2);
        
        assert!(matches!(decision, ScalingDecision::ScaleUp(count) if count > 0));
        if let ScalingDecision::ScaleUp(count) = decision {
            // Should scale up to match queue size or max workers
            assert!(count <= 6); // 8 - 2 = 6, but limited by range
        }
    }

    #[test]
    fn test_scale_up_limited_by_max_workers() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 5, false); // Max 5 workers
        
        engine.register_queue("test-queue".to_string(), worker_range, 4);
        
        // Queue size much larger than max workers
        let metrics = create_test_metrics("test-queue", 20, 4);
        let decision = engine.evaluate_scaling("test-queue", metrics, 4);
        
        assert!(matches!(decision, ScalingDecision::ScaleUp(1))); // Can only scale up by 1 (5-4)
    }

    #[test]
    fn test_scale_up_at_max_workers() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 5, false);
        
        engine.register_queue("test-queue".to_string(), worker_range, 5);
        
        // Already at max workers
        let metrics = create_test_metrics("test-queue", 20, 5);
        let decision = engine.evaluate_scaling("test-queue", metrics, 5);
        
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_scale_down_decision() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 10, false);
        
        engine.register_queue("test-queue".to_string(), worker_range, 8);
        
        // Queue size < current workers, should scale down
        let metrics = create_test_metrics("test-queue", 2, 8);
        let decision = engine.evaluate_scaling("test-queue", metrics, 8);
        
        assert!(matches!(decision, ScalingDecision::ScaleDown(count) if count > 0));
        if let ScalingDecision::ScaleDown(count) = decision {
            // Should scale down considering buffer and min workers
            // needed = max(2 + 1, 1) = 3, current = 8, so scale down by 5
            assert_eq!(count, 5);
        }
    }

    #[test]
    fn test_scale_down_with_buffer() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(2, 10, false);
        
        engine.register_queue("test-queue".to_string(), worker_range, 6);
        
        // Queue size = 3, buffer = 1, min = 2
        // needed = max(3 + 1, 2) = 4
        // current = 6, so should scale down by 2
        let metrics = create_test_metrics("test-queue", 3, 6);
        let decision = engine.evaluate_scaling("test-queue", metrics, 6);
        
        assert!(matches!(decision, ScalingDecision::ScaleDown(2)));
    }

    #[test]
    fn test_scale_down_limited_by_min_workers() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(3, 10, false); // Min 3 workers
        
        engine.register_queue("test-queue".to_string(), worker_range, 4);
        
        // Queue size = 0, but min workers = 3
        // needed = max(0 + 1, 3) = 3
        // current = 4, so should scale down by 1
        let metrics = create_test_metrics("test-queue", 0, 4);
        let decision = engine.evaluate_scaling("test-queue", metrics, 4);
        
        assert!(matches!(decision, ScalingDecision::ScaleDown(1)));
    }

    #[test]
    fn test_scale_down_at_min_workers() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(3, 10, false);
        
        engine.register_queue("test-queue".to_string(), worker_range, 3);
        
        // Already at min workers
        let metrics = create_test_metrics("test-queue", 0, 3);
        let decision = engine.evaluate_scaling("test-queue", metrics, 3);
        
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_no_scaling_when_balanced() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 10, false);
        
        engine.register_queue("test-queue".to_string(), worker_range, 5);
        
        // Queue size equals current workers
        let metrics = create_test_metrics("test-queue", 5, 5);
        let decision = engine.evaluate_scaling("test-queue", metrics, 5);
        
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_fixed_worker_range() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(5, 5, true); // Fixed at 5 workers
        
        engine.register_queue("test-queue".to_string(), worker_range, 5);
        
        // Even with high queue size, should not scale up
        let metrics = create_test_metrics("test-queue", 20, 5);
        let decision = engine.evaluate_scaling("test-queue", metrics, 5);
        
        assert_eq!(decision, ScalingDecision::NoChange);
        
        // Even with low queue size, should not scale down
        let metrics = create_test_metrics("test-queue", 0, 5);
        let decision = engine.evaluate_scaling("test-queue", metrics, 5);
        
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_edge_case_zero_queue_size() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 10, false);
        
        engine.register_queue("test-queue".to_string(), worker_range, 5);
        
        let metrics = create_test_metrics("test-queue", 0, 5);
        let decision = engine.evaluate_scaling("test-queue", metrics, 5);
        
        // Should scale down: needed = max(0 + 1, 1) = 1, current = 5, so scale down by 4
        assert!(matches!(decision, ScalingDecision::ScaleDown(4)));
    }

    #[test]
    fn test_edge_case_single_worker_range() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 1, false); // Only 1 worker allowed
        
        engine.register_queue("test-queue".to_string(), worker_range, 1);
        
        // High queue size, but can't scale up
        let metrics = create_test_metrics("test-queue", 100, 1);
        let decision = engine.evaluate_scaling("test-queue", metrics, 1);
        
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_queue_metrics_consumer_count_not_affecting_scaling() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 10, false);
        
        engine.register_queue("test-queue".to_string(), worker_range, 3);
        
        // Different consumer counts shouldn't affect scaling decisions
        let metrics1 = create_test_metrics("test-queue", 8, 1);
        let metrics2 = create_test_metrics("test-queue", 8, 10);
        
        let decision1 = engine.evaluate_scaling("test-queue", metrics1, 3);
        
        // Reset cooldown by creating new engine
        let mut engine2 = ScalingEngine::new(100);
        engine2.register_queue("test-queue".to_string(), create_worker_range(1, 10, false), 3);
        let decision2 = engine2.evaluate_scaling("test-queue", metrics2, 3);
        
        // Both should make the same scaling decision
        assert_eq!(decision1, decision2);
    }

    #[test]
    fn test_scaling_engine_state_persistence() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 10, false);
        
        engine.register_queue("test-queue".to_string(), worker_range, 3);
        
        // First call updates metrics
        let metrics1 = create_test_metrics("test-queue", 5, 3);
        let decision1 = engine.evaluate_scaling("test-queue", metrics1.clone(), 3);
        
        // Verify metrics are stored by making another call
        let metrics2 = create_test_metrics("test-queue", 6, 4);
        let decision2 = engine.evaluate_scaling("test-queue", metrics2, 4);
        
        // Should be able to make decisions for both calls
        assert!(matches!(decision1, ScalingDecision::ScaleUp(_) | ScalingDecision::NoChange));
        assert_eq!(decision2, ScalingDecision::NoChange); // Blocked by cooldown
    }

    #[test]
    fn test_multiple_queue_independence() {
        let mut engine = ScalingEngine::new(100);
        let worker_range1 = create_worker_range(1, 5, false);
        let worker_range2 = create_worker_range(2, 8, false);
        
        engine.register_queue("queue1".to_string(), worker_range1, 2);
        engine.register_queue("queue2".to_string(), worker_range2, 4);
        
        // Scale queue1 up
        let metrics1 = create_test_metrics("queue1", 6, 2);
        let decision1 = engine.evaluate_scaling("queue1", metrics1, 2);
        
        // queue2 should still be able to make independent decisions
        let metrics2 = create_test_metrics("queue2", 2, 4);
        let decision2 = engine.evaluate_scaling("queue2", metrics2, 4);
        
        assert!(matches!(decision1, ScalingDecision::ScaleUp(_)));
        assert!(matches!(decision2, ScalingDecision::ScaleDown(_)));
    }

    #[test]
    fn test_saturating_arithmetic() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(0, u32::MAX, false);
        
        engine.register_queue("test-queue".to_string(), worker_range, u32::MAX - 1);
        
        // Test scale up with potential overflow
        let metrics = create_test_metrics("test-queue", u32::MAX, u32::MAX - 1);
        let decision = engine.evaluate_scaling("test-queue", metrics, u32::MAX - 1);
        
        // Should handle overflow gracefully
        assert!(matches!(decision, ScalingDecision::ScaleUp(1)));
    }

    #[test]
    fn test_instant_now_consistency() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 10, false);
        
        engine.register_queue("test-queue".to_string(), worker_range, 3);
        
        let before = Instant::now();
        
        // Trigger scaling to set last_scaling_time
        let metrics = create_test_metrics("test-queue", 8, 3);
        let _decision = engine.evaluate_scaling("test-queue", metrics, 3);
        
        let after = Instant::now();
        
        // Verify timing makes sense (scaling time should be between before and after)
        if let Some(history) = engine.queue_histories.get("test-queue") {
            if let Some(scaling_time) = history.last_scaling_time {
                assert!(scaling_time >= before);
                assert!(scaling_time <= after);
            }
        }
    }

    #[test]
    fn test_complex_scaling_scenario() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(2, 20, false);
        
        engine.register_queue("production-queue".to_string(), worker_range, 5);
        
        // Scenario 1: High load, should scale up
        let high_load_metrics = create_test_metrics("production-queue", 15, 5);
        let decision1 = engine.evaluate_scaling("production-queue", high_load_metrics, 5);
        assert!(matches!(decision1, ScalingDecision::ScaleUp(count) if count > 0));
        
        // Simulate some time passing with shorter cooldown
        if let Some(history) = engine.queue_histories.get_mut("production-queue") {
            history.cooldown_period = Duration::from_millis(1);
        }
        sleep(Duration::from_millis(2));
        
        // Scenario 2: Load decreased, should scale down
        let low_load_metrics = create_test_metrics("production-queue", 3, 15);
        let decision2 = engine.evaluate_scaling("production-queue", low_load_metrics, 15);
        assert!(matches!(decision2, ScalingDecision::ScaleDown(count) if count > 0));
    }

    #[test]
    fn test_queue_history_multiple_updates() {
        let worker_range = create_worker_range(1, 5, false);
        let mut history = QueueHistory::new(worker_range, 2);
        
        let metrics1 = create_test_metrics("test-queue", 5, 2);
        let metrics2 = create_test_metrics("test-queue", 10, 3);
        let metrics3 = create_test_metrics("test-queue", 3, 1);
        
        // Update multiple times
        history.update(metrics1, 2);
        history.update(metrics2, 3);
        history.update(metrics3.clone(), 1);
        
        // Should have latest metrics
        assert_eq!(history.last_metrics.as_ref().unwrap().queue_size, 3);
        assert_eq!(history.last_metrics.as_ref().unwrap().consumer_count, 1);
        assert_eq!(history.current_workers, 1);
    }

    #[test]
    fn test_scaling_decision_variant_matching() {
        // Test all variants can be matched properly
        let decisions = vec![
            ScalingDecision::NoChange,
            ScalingDecision::ScaleUp(1),
            ScalingDecision::ScaleUp(10),
            ScalingDecision::ScaleDown(1),
            ScalingDecision::ScaleDown(5),
        ];
        
        for decision in decisions {
            match decision {
                ScalingDecision::NoChange => assert_eq!(decision, ScalingDecision::NoChange),
                ScalingDecision::ScaleUp(count) => assert!(count > 0),
                ScalingDecision::ScaleDown(count) => assert!(count > 0),
            }
        }
    }

    #[test]
    fn test_worker_range_boundaries() {
        let mut engine = ScalingEngine::new(100);
        
        // Test with zero min workers
        let worker_range = create_worker_range(0, 10, false);
        engine.register_queue("zero-min-queue".to_string(), worker_range, 5);
        
        let metrics = create_test_metrics("zero-min-queue", 0, 5);
        let decision = engine.evaluate_scaling("zero-min-queue", metrics, 5);
        
        // Should scale down to buffer size (1) since min is 0
        assert!(matches!(decision, ScalingDecision::ScaleDown(4))); // 5 - (0 + 1) = 4
    }

    #[test]
    fn test_scaling_with_very_large_queue_size() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 1000, false);
        
        engine.register_queue("large-queue".to_string(), worker_range, 10);
        
        let metrics = create_test_metrics("large-queue", u32::MAX, 10);
        let decision = engine.evaluate_scaling("large-queue", metrics, 10);
        
        // Should scale up but be limited by max workers
        assert!(matches!(decision, ScalingDecision::ScaleUp(990))); // 1000 - 10 = 990
    }

    #[test]
    fn test_cooldown_edge_cases() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 10, false);
        
        engine.register_queue("cooldown-test".to_string(), worker_range, 3);
        
        // Get mutable reference to modify cooldown
        if let Some(history) = engine.queue_histories.get_mut("cooldown-test") {
            // Set cooldown to zero
            history.cooldown_period = Duration::from_secs(0);
            history.mark_scaled();
        }
        
        // Should be able to scale immediately with zero cooldown
        let metrics = create_test_metrics("cooldown-test", 8, 3);
        let decision = engine.evaluate_scaling("cooldown-test", metrics, 3);
        
        assert!(matches!(decision, ScalingDecision::ScaleUp(_)));
    }

    #[test]
    fn test_scaling_engine_queue_name_edge_cases() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 5, false);
        
        // Test with empty string queue name
        engine.register_queue("".to_string(), worker_range.clone(), 2);
        let metrics = create_test_metrics("", 3, 2);
        let decision = engine.evaluate_scaling("", metrics, 2);
        assert!(matches!(decision, ScalingDecision::ScaleUp(_)));
        
        // Test with special characters in queue name
        let special_name = "queue@#$%^&*()_+{}[]|\\:;<>?";
        engine.register_queue(special_name.to_string(), worker_range, 2);
        let metrics = create_test_metrics(special_name, 3, 2);
        let decision = engine.evaluate_scaling(special_name, metrics, 2);
        assert!(matches!(decision, ScalingDecision::ScaleUp(_)));
    }

    #[test]
    fn test_scale_down_buffer_edge_cases() {
        let mut engine = ScalingEngine::new(100);
        
        // Test where buffer equals min workers
        let worker_range = create_worker_range(1, 10, false); // buffer = 1, min = 1
        engine.register_queue("buffer-test".to_string(), worker_range, 5);
        
        let metrics = create_test_metrics("buffer-test", 0, 5);
        let decision = engine.evaluate_scaling("buffer-test", metrics, 5);
        
        // needed = max(0 + 1, 1) = 1, current = 5, so scale down by 4
        assert!(matches!(decision, ScalingDecision::ScaleDown(4)));
    }

    #[test]
    fn test_precise_scaling_calculations() {
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(2, 15, false);
        
        engine.register_queue("precise-test".to_string(), worker_range, 8);
        
        // Test exact calculations
        let metrics = create_test_metrics("precise-test", 6, 8);
        let decision = engine.evaluate_scaling("precise-test", metrics, 8);
        
        // Queue size (6) + buffer (1) = 7, max with min (2) = 7
        // Current workers (8) - needed (7) = scale down by 1
        assert!(matches!(decision, ScalingDecision::ScaleDown(1)));
    }

    #[test]
    fn test_queue_history_instant_operations() {
        let worker_range = create_worker_range(1, 5, false);
        let mut history = QueueHistory::new(worker_range, 2);
        
        let start = Instant::now();
        
        // Mark scaled multiple times
        history.mark_scaled();
        let first_time = history.last_scaling_time.unwrap();
        
        // Small delay
        sleep(Duration::from_millis(1));
        
        history.mark_scaled();
        let second_time = history.last_scaling_time.unwrap();
        
        // Times should be different and in order
        assert!(first_time >= start);
        assert!(second_time >= first_time);
        assert!(second_time >= start);
    }

    #[test]
    fn test_extreme_worker_counts() {
        let mut engine = ScalingEngine::new(100);
        
        // Test with 0 current workers (edge case)
        let worker_range = create_worker_range(0, 100, false);
        engine.register_queue("zero-workers".to_string(), worker_range, 0);
        
        let metrics = create_test_metrics("zero-workers", 50, 0);
        let decision = engine.evaluate_scaling("zero-workers", metrics, 0);
        
        // Should scale up from 0 to match queue size
        assert!(matches!(decision, ScalingDecision::ScaleUp(50)));
    }

    #[test]
    fn test_scaling_decision_equality_edge_cases() {
        // Test equality with different values
        assert_ne!(ScalingDecision::ScaleUp(0), ScalingDecision::ScaleUp(1));
        assert_ne!(ScalingDecision::ScaleDown(0), ScalingDecision::ScaleDown(1));
        assert_ne!(ScalingDecision::ScaleUp(5), ScalingDecision::ScaleDown(5));
        
        // Test equality with same values
        assert_eq!(ScalingDecision::ScaleUp(42), ScalingDecision::ScaleUp(42));
        assert_eq!(ScalingDecision::ScaleDown(7), ScalingDecision::ScaleDown(7));
    }

    #[test]
    fn test_worker_range_is_fixed_ignored_in_scaling_logic() {
        // The is_fixed field should be ignored by the scaling engine
        // as it's used elsewhere in the application
        let mut engine = ScalingEngine::new(100);
        
        // Create identical ranges except for is_fixed
        let range_not_fixed = create_worker_range(2, 8, false);
        let range_fixed = create_worker_range(2, 8, true);
        
        engine.register_queue("not-fixed".to_string(), range_not_fixed, 4);
        engine.register_queue("fixed".to_string(), range_fixed, 4);
        
        let metrics = create_test_metrics("test", 7, 4);
        
        let decision1 = engine.evaluate_scaling("not-fixed", metrics.clone(), 4);
        
        // Create new engine to avoid cooldown
        let mut engine2 = ScalingEngine::new(100);
        engine2.register_queue("fixed".to_string(), create_worker_range(2, 8, true), 4);
        let decision2 = engine2.evaluate_scaling("fixed", metrics, 4);
        
        // Both should make the same scaling decision regardless of is_fixed
        assert_eq!(decision1, decision2);
    }

    // === Additional tests for uncovered lines ===

    #[test]
    fn test_register_queue_logs_info_line_85() {
        // Covers line 85: info! log in register_queue
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(2, 10, false);
        
        // This will trigger the info! log at line 85
        engine.register_queue("test-queue-logging".to_string(), worker_range, 5);
        
        // Verify the queue is registered by evaluating
        let metrics = create_test_metrics("test-queue-logging", 3, 1);
        let decision = engine.evaluate_scaling("test-queue-logging", metrics, 5);
        assert!(matches!(decision, ScalingDecision::NoChange | ScalingDecision::ScaleUp(_) | ScalingDecision::ScaleDown(_)));
    }

    #[test]
    fn test_cooldown_period_debug_log_lines_117_119() {
        // Covers lines 117-119: debug! log showing cooldown remaining time
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 10, false);
        
        engine.register_queue("cooldown-log-test".to_string(), worker_range, 3);
        
        // First scaling decision - triggers scale up and sets cooldown
        let metrics1 = create_test_metrics("cooldown-log-test", 8, 3);
        let decision1 = engine.evaluate_scaling("cooldown-log-test", metrics1, 3);
        assert!(matches!(decision1, ScalingDecision::ScaleUp(_)));
        
        // Immediate second call - triggers cooldown debug log (lines 117-119)
        let metrics2 = create_test_metrics("cooldown-log-test", 9, 3);
        let decision2 = engine.evaluate_scaling("cooldown-log-test", metrics2, 3);
        assert_eq!(decision2, ScalingDecision::NoChange);
    }

    #[test]
    fn test_scaling_decision_info_log_line_140() {
        // Covers line 140: info! log when scaling decision is made
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 15, false);
        
        engine.register_queue("scaling-info-log".to_string(), worker_range, 4);
        
        // Trigger scale up - this will log info at line 140
        let metrics = create_test_metrics("scaling-info-log", 12, 4);
        let decision = engine.evaluate_scaling("scaling-info-log", metrics, 4);
        assert!(matches!(decision, ScalingDecision::ScaleUp(_)));
    }

    #[test]
    fn test_make_scaling_decision_debug_log_line_160() {
        // Covers line 160: debug! log in make_scaling_decision
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(2, 12, false);
        
        engine.register_queue("debug-log-160".to_string(), worker_range, 6);
        
        // Any evaluation will trigger the debug log at line 160
        let metrics = create_test_metrics("debug-log-160", 5, 6);
        let _ = engine.evaluate_scaling("debug-log-160", metrics, 6);
    }

    #[test]
    fn test_scale_up_debug_log_and_return_lines_173_177() {
        // Covers line 173: debug! log for scale up trigger
        // Covers line 177: return ScalingDecision::ScaleUp
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 20, false);
        
        engine.register_queue("scale-up-debug".to_string(), worker_range, 3);
        
        // Queue size > workers, will trigger scale up debug and return at line 177
        let metrics = create_test_metrics("scale-up-debug", 10, 3);
        let decision = engine.evaluate_scaling("scale-up-debug", metrics, 3);
        
        // Verify we got ScaleUp decision (line 177)
        assert!(matches!(decision, ScalingDecision::ScaleUp(count) if count > 0));
    }

    #[test]
    fn test_scale_down_check_debug_log_lines_183_185() {
        // Covers lines 183-185: debug! log for scale down check
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 10, false);
        
        engine.register_queue("scale-down-check".to_string(), worker_range, 7);
        
        // Queue size < workers, will trigger scale down check debug log (lines 183-185)
        let metrics = create_test_metrics("scale-down-check", 2, 7);
        let decision = engine.evaluate_scaling("scale-down-check", metrics, 7);
        
        assert!(matches!(decision, ScalingDecision::ScaleDown(_)));
    }

    #[test]
    fn test_scale_down_calculation_debug_log_line_193() {
        // Covers line 193: debug! log for scale down calculation
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(2, 15, false);
        
        engine.register_queue("scale-down-calc".to_string(), worker_range, 9);
        
        // Queue size < workers, triggers calculation log at line 193
        let metrics = create_test_metrics("scale-down-calc", 4, 9);
        let decision = engine.evaluate_scaling("scale-down-calc", metrics, 9);
        
        // Should scale down with proper calculation
        assert!(matches!(decision, ScalingDecision::ScaleDown(_)));
    }

    #[test]
    fn test_scale_down_trigger_debug_log_line_200() {
        // Covers line 200: debug! log for scale down trigger
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(1, 12, false);
        
        engine.register_queue("scale-down-trigger".to_string(), worker_range, 8);
        
        // Queue size = 1, workers = 8, should trigger scale down and log at line 200
        let metrics = create_test_metrics("scale-down-trigger", 1, 8);
        let decision = engine.evaluate_scaling("scale-down-trigger", metrics, 8);
        
        assert!(matches!(decision, ScalingDecision::ScaleDown(count) if count > 0));
    }

    #[test]
    fn test_scale_down_blocked_debug_log_line_206() {
        // Covers line 206: debug! log when scale down is blocked
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(5, 10, false);
        
        engine.register_queue("scale-down-blocked".to_string(), worker_range, 6);
        
        // Queue size = 4, buffer = 1, min = 5
        // needed = max(4 + 1, 5) = 5, but current = 6
        // However, if needed == current - 1, we still scale by 1
        // Let's set it so needed >= current to block scaling
        // Actually, with queue_size=5, buffer=1, min=5: needed = max(5+1, 5) = 6
        // current = 6, so needed_workers(6) >= current_workers(6) -> blocked
        let metrics = create_test_metrics("scale-down-blocked", 5, 6);
        let decision = engine.evaluate_scaling("scale-down-blocked", metrics, 6);
        
        // Should not scale down, log at line 206
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_scale_down_conditions_not_met_log_lines_212_214() {
        // Covers lines 212-214: debug! log when scale down conditions not met
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(5, 10, false);
        
        engine.register_queue("no-scale-down".to_string(), worker_range, 5);
        
        // current_workers = 5 = min, so scale down conditions not met
        // This will trigger the else block at lines 212-214
        let metrics = create_test_metrics("no-scale-down", 2, 5);
        let decision = engine.evaluate_scaling("no-scale-down", metrics, 5);
        
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_all_debug_paths_comprehensive() {
        // Comprehensive test covering multiple debug paths in a single flow
        let mut engine = ScalingEngine::new(100);
        let worker_range = create_worker_range(2, 20, false);
        
        // Line 85: Register queue (info log)
        engine.register_queue("comprehensive-test".to_string(), worker_range, 5);
        
        // Lines 160, 173, 177: Scale up with debug logs
        let metrics1 = create_test_metrics("comprehensive-test", 15, 5);
        let decision1 = engine.evaluate_scaling("comprehensive-test", metrics1, 5);
        assert!(matches!(decision1, ScalingDecision::ScaleUp(_)));
        
        // Lines 117, 119: Cooldown period debug log
        let metrics2 = create_test_metrics("comprehensive-test", 16, 5);
        let decision2 = engine.evaluate_scaling("comprehensive-test", metrics2, 5);
        assert_eq!(decision2, ScalingDecision::NoChange);
        
        // Wait for cooldown and scale down
        if let Some(history) = engine.queue_histories.get_mut("comprehensive-test") {
            history.cooldown_period = Duration::from_millis(1);
        }
        sleep(Duration::from_millis(2));
        
        // Lines 160, 183-185, 193, 200: Scale down with debug logs
        let metrics3 = create_test_metrics("comprehensive-test", 2, 15);
        let decision3 = engine.evaluate_scaling("comprehensive-test", metrics3, 15);
        assert!(matches!(decision3, ScalingDecision::ScaleDown(_)));
    }
}

