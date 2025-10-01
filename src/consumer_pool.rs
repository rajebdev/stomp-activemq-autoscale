use anyhow::Result;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::config::{Config, WorkerRange};
use crate::client::StompClient;

/// Type alias for message handler functions
pub type MessageHandler = dyn Fn(String) -> Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
    + Send
    + Sync
    + 'static;

/// Worker information
#[derive(Debug)]
pub struct WorkerInfo {
    /// Unique worker ID
    pub id: String,
    /// Worker task handle
    pub handle: JoinHandle<Result<()>>,
    /// Shutdown sender for this worker
    pub shutdown_tx: broadcast::Sender<()>,
}

/// Consumer pool for managing workers for a specific queue
pub struct ConsumerPool {
    /// Queue name this pool manages
    queue_name: String,
    /// Configuration reference
    config: Config,
    /// Min/max worker range for scaling
    worker_range: WorkerRange,
    /// Currently active workers
    workers: Arc<Mutex<Vec<WorkerInfo>>>,
    /// Message handler for this queue
    handler: Arc<Box<MessageHandler>>,
}

impl ConsumerPool {
    /// Create a new consumer pool for a queue
    pub fn new(
        queue_name: String,
        config: Config,
        worker_range: WorkerRange,
        handler: Box<MessageHandler>,
    ) -> Self {
        info!(
            "🏊 Creating consumer pool for queue '{}' (workers: {}-{})",
            queue_name, worker_range.min, worker_range.max
        );

        Self {
            queue_name,
            config,
            worker_range,
            workers: Arc::new(Mutex::new(Vec::new())),
            handler: Arc::new(handler),
        }
    }

    /// Initialize the pool with minimum number of workers
    pub async fn initialize(&mut self) -> Result<()> {
        info!(
            "🚀 Initializing consumer pool for '{}' with {} workers",
            self.queue_name, self.worker_range.min
        );

        for _ in 0..self.worker_range.min {
            self.spawn_worker().await?;
        }

        Ok(())
    }

    /// Get current number of active workers
    pub async fn get_worker_count(&self) -> usize {
        let workers = self.workers.lock().await;
        workers.len()
    }

    /// Get current worker range
    pub fn get_worker_range(&self) -> &WorkerRange {
        &self.worker_range
    }

    /// Spawn a new worker for this queue
    pub async fn spawn_worker(&self) -> Result<String> {
        let worker_id = format!("{}#{}", self.queue_name, Uuid::new_v4());
        debug!("🔄 Spawning worker: {}", worker_id);

        // Create shutdown channel for this worker
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        // Clone necessary data for the worker
        let config = self.config.clone();
        let queue_name = self.queue_name.clone();
        let handler = self.handler.clone();
        let worker_id_clone = worker_id.clone();

        // Spawn the worker task
        let handle = tokio::spawn(async move {
            Self::worker_task(worker_id_clone, config, queue_name, handler, shutdown_rx).await
        });

        // Create worker info and add to pool
        let worker_info = WorkerInfo {
            id: worker_id.clone(),
            handle,
            shutdown_tx,
        };

        let mut workers = self.workers.lock().await;
        workers.push(worker_info);

        debug!("✅ Worker '{}' spawned successfully", worker_id);
        Ok(worker_id)
    }

    /// Stop and remove a worker (preferably idle ones)
    pub async fn stop_worker(&self) -> Result<bool> {
        let mut workers = self.workers.lock().await;

        if workers.is_empty() {
            return Ok(false);
        }

        // For now, remove the last worker (LIFO)
        // In a more sophisticated implementation, you'd track worker activity
        // and remove the most idle worker
        let worker = workers.pop().unwrap();
        drop(workers); // Release lock early

        debug!("🛑 Stopping worker: {}", worker.id);

        // Send shutdown signal to the worker
        let _ = worker.shutdown_tx.send(());

        // Wait for the worker to finish with timeout
        let timeout_duration = tokio::time::Duration::from_secs(10);
        match tokio::time::timeout(timeout_duration, worker.handle).await {
            Ok(Ok(_)) => {
                debug!("✅ Worker '{}' stopped gracefully", worker.id);
            }
            Ok(Err(e)) => {
                warn!("⚠️ Worker '{}' stopped with error: {}", worker.id, e);
            }
            Err(_) => {
                warn!("⏰ Worker '{}' shutdown timeout", worker.id);
            }
        }

        Ok(true)
    }

    /// Scale up workers (add workers up to max)
    pub async fn scale_up(&self, target_count: u32) -> Result<u32> {
        let current_count = self.get_worker_count().await as u32;
        let max_count = self.worker_range.max;

        if target_count <= current_count {
            return Ok(0);
        }

        let desired_increase = target_count.saturating_sub(current_count);
        let max_increase = max_count.saturating_sub(current_count);
        let actual_increase = desired_increase.min(max_increase);

        if actual_increase == 0 {
            debug!("No scaling up needed for queue '{}'", self.queue_name);
            return Ok(0);
        }

        info!(
            "📈 Scaling up queue '{}': adding {} workers ({} -> {})",
            self.queue_name,
            actual_increase,
            current_count,
            current_count + actual_increase
        );

        for _ in 0..actual_increase {
            if let Err(e) = self.spawn_worker().await {
                error!("Failed to spawn worker for '{}': {}", self.queue_name, e);
                break;
            }
        }

        Ok(actual_increase)
    }

    /// Scale down workers (remove workers down to min)
    pub async fn scale_down(&self, target_count: u32) -> Result<u32> {
        let current_count = self.get_worker_count().await as u32;
        let min_count = self.worker_range.min;

        if target_count >= current_count {
            return Ok(0);
        }

        let desired_decrease = current_count.saturating_sub(target_count);
        let max_decrease = current_count.saturating_sub(min_count);
        let actual_decrease = desired_decrease.min(max_decrease);

        if actual_decrease == 0 {
            debug!("No scaling down needed for queue '{}'", self.queue_name);
            return Ok(0);
        }

        info!(
            "📉 Scaling down queue '{}': removing {} workers ({} -> {})",
            self.queue_name,
            actual_decrease,
            current_count,
            current_count - actual_decrease
        );

        for _ in 0..actual_decrease {
            if !self.stop_worker().await? {
                warn!("No more workers to stop for queue '{}'", self.queue_name);
                break;
            }
        }

        Ok(actual_decrease)
    }

    /// Worker task implementation
    async fn worker_task(
        worker_id: String,
        config: Config,
        queue_name: String,
        handler: Arc<Box<MessageHandler>>,
        mut shutdown_rx: broadcast::Receiver<()>,
    ) -> Result<()> {
        debug!("🔄 Worker '{}' starting for queue '{}'", worker_id, queue_name);

        // Create STOMP service for this worker
        let mut client = StompClient::new(config).await?;

        // Create a handler that logs worker ID
        let worker_handler = {
            let worker_id = worker_id.clone();
            let handler = handler.clone();
            move |msg: String| {
                let worker_id = worker_id.clone();
                let handler = handler.clone();
                Box::pin(async move {
                    debug!("[{}] Processing message", worker_id);
                    let result = handler(msg).await;
                    match &result {
                        Ok(_) => debug!("[{}] Message processed successfully", worker_id),
                        Err(e) => error!("[{}] Message processing failed: {}", worker_id, e),
                    }
                    result
                }) as Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>>
            }
        };

        // Run the worker with shutdown handling
        tokio::select! {
            result = client.receive_queue(&queue_name, worker_handler) => {
                match &result {
                    Ok(_) => debug!("✅ Worker '{}' completed normally", worker_id),
                    Err(e) => error!("❌ Worker '{}' failed: {}", worker_id, e),
                }
                result
            }
            _ = shutdown_rx.recv() => {
                debug!("🛑 Worker '{}' received shutdown signal", worker_id);
                if let Err(e) = client.disconnect().await {
                    warn!("Worker '{}' disconnect error: {}", worker_id, e);
                }
                Ok(())
            }
        }
    }

    /// Stop all workers in the pool
    pub async fn stop_all(&self) -> Result<()> {
        info!("🛑 Stopping all workers for queue '{}'", self.queue_name);

        let workers = {
            let mut workers = self.workers.lock().await;
            std::mem::take(&mut *workers)
        };

        if workers.is_empty() {
            debug!("No workers to stop for queue '{}'", self.queue_name);
            return Ok(());
        }

        debug!("Stopping {} workers for queue '{}'", workers.len(), self.queue_name);

        // Send shutdown signals to all workers
        for worker in &workers {
            let _ = worker.shutdown_tx.send(());
        }

        // Wait for all workers to complete
        let timeout_duration = tokio::time::Duration::from_secs(30);
        for worker in workers {
            match tokio::time::timeout(timeout_duration, worker.handle).await {
                Ok(Ok(_)) => {
                    debug!("Worker '{}' stopped gracefully", worker.id);
                }
                Ok(Err(e)) => {
                    warn!("Worker '{}' stopped with error: {}", worker.id, e);
                }
                Err(_) => {
                    warn!("Worker '{}' shutdown timeout", worker.id);
                }
            }
        }

        info!("✅ All workers stopped for queue '{}'", self.queue_name);
        Ok(())
    }
}


