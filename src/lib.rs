// Public API - hanya 3 ini yang bisa diakses user
pub mod config;
pub mod producer;
pub mod listener;

// Internal modules - tidak bisa diakses dari luar
mod client;
mod broker_monitor;
mod monitor;
mod consumer_pool;
mod scaling;
mod autoscaler;
mod runner;
mod utils;
mod listener_handle;

#[cfg(test)]
#[ctor::ctor]
fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .try_init();
}