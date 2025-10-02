use anyhow::Result;
use tokio::task::JoinHandle;
use tokio::sync::broadcast;
use tracing::info;

/// Handle for controlling a running StompListener
pub struct StompListenerHandle {
    pub(crate) shutdown_tx: broadcast::Sender<()>,
    pub(crate) task_handle: JoinHandle<Result<()>>,
}

impl StompListenerHandle {
    /// Create a new StompListenerHandle
    pub(crate) fn new(
        shutdown_tx: broadcast::Sender<()>,
        task_handle: JoinHandle<Result<()>>,
    ) -> Self {
        Self {
            shutdown_tx,
            task_handle,
        }
    }

    /// Gracefully shutdown the listener and wait for completion
    pub async fn shutdown_and_wait(self) -> Result<()> {
        info!("🛑 Initiating graceful shutdown...");
        
        // Send shutdown signal
        let _ = self.shutdown_tx.send(());
        
        // Wait for the task to complete
        match self.task_handle.await {
            Ok(result) => {
                info!("✅ STOMP Listener shutdown complete");
                result
            }
            Err(e) => {
                if e.is_cancelled() {
                    info!("✅ STOMP Listener shutdown complete (cancelled)");
                    Ok(())
                } else {
                    Err(anyhow::anyhow!("Failed to join listener task: {}", e))
                }
            }
        }
    }
    
    /// Send shutdown signal without waiting
    pub fn shutdown(&self) {
        info!("🛑 Sending shutdown signal...");
        let _ = self.shutdown_tx.send(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::Duration;

    #[tokio::test]
    async fn test_shutdown_signal() {
        let (shutdown_tx, mut shutdown_rx) = broadcast::channel::<()>(1);
        let task_handle = tokio::spawn(async move {
            // Simulate some work and wait for shutdown signal
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Received shutdown signal in test task");
                    Ok(())
                }
                _ = tokio::time::sleep(Duration::from_secs(10)) => {
                    Ok(())
                }
            }
        });

        let handle = StompListenerHandle::new(shutdown_tx, task_handle);

        // Send shutdown signal
        handle.shutdown();

        // This should complete quickly since we sent the shutdown signal
        let result = tokio::time::timeout(Duration::from_secs(1), handle.shutdown_and_wait()).await;
        assert!(result.is_ok(), "Shutdown should complete within timeout");
    }

    #[tokio::test]
    async fn test_multiple_shutdown_calls() {
        let (shutdown_tx, _shutdown_rx) = broadcast::channel::<()>(1);
        let task_handle = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(())
        });

        let handle = StompListenerHandle::new(shutdown_tx, task_handle);

        // Call shutdown multiple times - should not panic
        handle.shutdown();
        handle.shutdown();
        handle.shutdown();

        // This test passes if it reaches this point without panicking
        // Multiple shutdown calls are handled gracefully
    }
}