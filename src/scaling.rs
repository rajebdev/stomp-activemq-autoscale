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
    /// Queue name
    pub queue_name: String,
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
    pub fn new(queue_name: String, worker_range: WorkerRange, current_workers: u32) -> Self {
        Self {
            queue_name,
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

        let history = QueueHistory::new(queue_name.clone(), worker_range, current_workers);
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
            "🔍 Evaluating scaling for '{}': queue_size={}, workers={}, range={}-{}, activemq_consumers={}",
            queue_name, queue_size, current_workers, worker_range.min, worker_range.max, metrics.consumer_count
        );

        // **Scale Up Logic**
        // Scale up if queue size > current workers AND we're below max workers
        if queue_size > current_workers && current_workers < worker_range.max {
            let needed_workers = queue_size.saturating_sub(current_workers);
            let max_increase = worker_range.max.saturating_sub(current_workers);
            let scale_up_count = needed_workers.min(max_increase);

            if scale_up_count > 0 {
                debug!(
                    "📈 Scale up trigger: queue '{}' has {} messages but only {} workers",
                    queue_name, queue_size, current_workers
                );
                return ScalingDecision::ScaleUp(scale_up_count);
            }
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

    /// Get current queue history for debugging
    pub fn get_queue_history(&self, queue_name: &str) -> Option<&QueueHistory> {
        self.queue_histories.get(queue_name)
    }

    /// Get all registered queues
    pub fn get_registered_queues(&self) -> Vec<&str> {
        self.queue_histories.keys().map(|s| s.as_str()).collect()
    }

    /// Get current scale down buffer
    pub fn get_scale_down_buffer(&self) -> u32 {
        self.scale_down_buffer
    }

    /// Remove a queue from monitoring (e.g., when shutting down)
    pub fn unregister_queue(&mut self, queue_name: &str) -> bool {
        match self.queue_histories.remove(queue_name) {
            Some(_) => {
                info!("📊 Unregistered queue '{}' from auto-scaling", queue_name);
                true
            }
            None => false,
        }
    }

    /// Get metrics summary for all registered queues
    pub fn get_metrics_summary(&self) -> HashMap<String, (u32, u32, WorkerRange)> {
        let mut summary = HashMap::new();
        
        for (queue_name, history) in &self.queue_histories {
            let queue_size = history.last_metrics.as_ref()
                .map(|m| m.queue_size)
                .unwrap_or(0);
            
            summary.insert(
                queue_name.clone(),
                (queue_size, history.current_workers, history.worker_range.clone()),
            );
        }
        
        summary
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_metrics(queue_name: &str, queue_size: u32) -> QueueMetrics {
        QueueMetrics {
            queue_name: queue_name.to_string(),
            queue_size,
            consumer_count: 0,
            enqueue_count: 0,
            dequeue_count: 0,
            memory_percent_usage: 0.0,
        }
    }

    #[test]
    fn test_scaling_engine_creation() {
        let engine = ScalingEngine::new(0);
        assert_eq!(engine.get_scale_down_buffer(), 1);
        assert_eq!(engine.get_registered_queues().len(), 0);
    }

    #[test]
    fn test_queue_registration() {
        let mut engine = ScalingEngine::new(4);
        let worker_range = WorkerRange { min: 1, max: 4, is_fixed: false };
        
        engine.register_queue("test".to_string(), worker_range, 1);
        
        let queues = engine.get_registered_queues();
        assert_eq!(queues.len(), 1);
        assert!(queues.contains(&"test"));
    }

    #[test]
    fn test_scale_up_decision() {
        let mut engine = ScalingEngine::new(4);
        let worker_range = WorkerRange { min: 1, max: 4, is_fixed: false };
        
        engine.register_queue("test".to_string(), worker_range, 1);
        
        // Queue has 5 messages but only 1 worker -> should scale up
        let metrics = create_test_metrics("test", 5);
        let decision = engine.evaluate_scaling("test", metrics, 1);
        
        match decision {
            ScalingDecision::ScaleUp(count) => assert!(count > 0),
            _ => panic!("Expected scale up decision"),
        }
    }

    #[test]
    fn test_scale_down_decision() {
        let mut engine = ScalingEngine::new(0); // Parameter ignored now
        let worker_range = WorkerRange { min: 1, max: 4, is_fixed: false };
        
        engine.register_queue("test".to_string(), worker_range, 3);
        
        // Queue has 1 message with 3 workers
        // needed_workers = max(1 + 1, 1) = 2, so should scale down by 3-2=1
        let metrics = create_test_metrics("test", 1);
        let decision = engine.evaluate_scaling("test", metrics, 3);
        
        match decision {
            ScalingDecision::ScaleDown(count) => {
                assert_eq!(count, 1, "Should scale down by 1 worker");
            },
            _ => panic!("Expected scale down decision, got: {:?}", decision),
        }
    }

    #[test]
    fn test_no_change_decision() {
        let mut engine = ScalingEngine::new(4);
        let worker_range = WorkerRange { min: 1, max: 4, is_fixed: false };
        
        engine.register_queue("test".to_string(), worker_range, 2);
        
        // Queue has 2 messages with 2 workers -> no change needed
        let metrics = create_test_metrics("test", 2);
        let decision = engine.evaluate_scaling("test", metrics, 2);
        
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_respect_worker_limits() {
        let mut engine = ScalingEngine::new(4);
        let worker_range = WorkerRange { min: 1, max: 2, is_fixed: false };
        
        engine.register_queue("test".to_string(), worker_range, 2);
        
        // Queue has 10 messages but already at max workers -> no scale up
        let metrics = create_test_metrics("test", 10);
        let decision = engine.evaluate_scaling("test", metrics, 2);
        
        assert_eq!(decision, ScalingDecision::NoChange);
    }

    #[test]
    fn test_cooldown_period() {
        let mut engine = ScalingEngine::new(4);
        let worker_range = WorkerRange { min: 1, max: 4, is_fixed: false };
        
        engine.register_queue("test".to_string(), worker_range, 1);
        
        // First scaling decision
        let metrics = create_test_metrics("test", 5);
        let decision1 = engine.evaluate_scaling("test", metrics.clone(), 1);
        assert!(matches!(decision1, ScalingDecision::ScaleUp(_)));
        
        // Immediate second decision should be NoChange due to cooldown
        let decision2 = engine.evaluate_scaling("test", metrics, 1);
        assert_eq!(decision2, ScalingDecision::NoChange);
    }
}