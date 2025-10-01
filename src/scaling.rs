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
}

