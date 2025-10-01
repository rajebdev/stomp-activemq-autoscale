# STOMP ActiveMQ Autoscale Library

[![Crates.io](https://img.shields.io/crates/v/stomp-activemq-autoscale)](https://crates.io/crates/stomp-activemq-autoscale)
[![Documentation](https://docs.rs/stomp-activemq-autoscale/badge.svg)](https://docs.rs/stomp-activemq-autoscale)
[![License](https://img.shields.io/crates/l/stomp-activemq-autoscale)](https://github.com/rajebdev/stomp-activemq-autoscale/blob/main/LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange)](https://www.rust-lang.org)

A production-ready **STOMP messaging library** implemented in Rust with intelligent **auto-scaling capabilities**, comprehensive **error handling**, and **automatic reconnection** features. Designed for both **Apache ActiveMQ Classic** and **Apache Artemis** brokers.

## ✨ Key Features

- 🎯 **Auto-Scaling Engine**: Dynamic worker management based on real-time queue metrics
- 🔄 **Multi-Broker Support**: Works seamlessly with both ActiveMQ Classic and Apache Artemis
- 📤 **High-Performance Producer**: Efficient connection reuse and JMS standard headers
- 📥 **Flexible Message Listeners**: Queue and topic support with custom handlers
- 🔄 **Reliability & Resilience**: Auto-reconnection with exponential backoff
- 🏗️ **Production Ready**: Comprehensive monitoring and graceful shutdown
- ⚡ **Async/Await Native**: Built on Tokio for high-performance async operations
- 🛡️ **Type Safety**: Leverages Rust's type system for compile-time guarantees

## 🎯 How Auto-Scaling Works

1. **Monitor**: Query broker every 5 seconds for queue depth metrics
2. **Decide**: Compare queue depth with current worker count using configurable thresholds
3. **Scale**: Dynamically spawn/stop workers while maintaining connection pools
4. **Cool Down**: 30-second cooldown prevents rapid scaling oscillations

## 🚀 Quick Start

### Installation

**Add to your `Cargo.toml`:**
```toml
[dependencies]
stomp-activemq-autoscale = "0.1.0"
tokio = { version = "1.0", features = ["full"] }
anyhow = "1.0"
```

**Or using cargo:**
```bash
cargo add stomp-activemq-autoscale tokio anyhow
```

### Simple Producer Example

```rust
use stomp_activemq_autoscale::{
    config::Config,
    stomp_producer::StompProducer,
};
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Load configuration
    let config = Config::load("config.yaml")?;
    
    // Create producer with connection reuse
    let mut producer = StompProducer::new(config).await?;
    
    // Send messages
    producer.send_queue("orders", "New order #12345").await?;
    producer.send_topic("notifications", "Order processed").await?;
    
    Ok(())
}
```

### Simple Consumer Example

```rust
use stomp_activemq_autoscale::{
    config::Config,
    stomp_listener::StompListener,
};
use anyhow::Result;

async fn handle_order(message: String) -> Result<()> {
    println!("📦 Processing order: {}", message);
    // Add your business logic here
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let config = Config::load("config.yaml")?;
    
    let listener = StompListener::new(config)
        .add_queue("orders", handle_order);
    
    // Run with graceful shutdown
    let shutdown_handle = listener.run_background();
    
    // Wait for Ctrl+C
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("🛑 Shutting down gracefully...");
            shutdown_handle.shutdown_and_wait().await?;
        }
    }
    
    Ok(())
}
```

## 📚 Documentation

**📖 Complete documentation is available in the [GitHub Wiki](../../wiki)**

### Quick Navigation

| **Getting Started** | **Advanced Topics** | **Development** |
|-------------------|-------------------|---------------|
| [🏠 **Home**](../../wiki/Home) | [✨ Best Practices](../../wiki/Best-Practices) | [🧪 Testing Guide](../../wiki/Testing-Guide) |
| [📦 Installation Guide](../../wiki/Installation-Guide) | [🔗 Multi-Broker Support](../../wiki/Multi-Broker-Support) | [🔧 Troubleshooting](../../wiki/Troubleshooting) |
| [⚙️ Configuration Reference](../../wiki/Configuration-Reference) | [📊 Performance Optimization](../../wiki/Best-Practices#performance-optimization) | [🤝 Contributing Guide](../../wiki/Contributing-Guide) |
| [📚 API Documentation](../../wiki/API-Documentation) | [🔒 Production Deployment](../../wiki/Best-Practices#production-configuration) | [📋 Issue Templates](../../issues/new/choose) |
| [💡 Examples & Usage](../../wiki/Examples-and-Usage) | [📈 Monitoring & Metrics](../../wiki/Best-Practices#monitoring-observability) | [💬 Discussions](../../discussions) |

## ⚙️ Configuration Example

**Create `config.yaml`:**

```yaml
service:
  name: "my-stomp-service"
  version: "1.0.0"
  description: "Production STOMP messaging service"

broker:
  type: "activemq"          # or "artemis"
  host: "localhost"
  username: "admin"
  password: "admin"
  stomp_port: 61613
  web_port: 8161
  heartbeat_secs: 30
  broker_name: "localhost"

destinations:
  queues:
    orders: "/queue/orders"
    priority_orders: "/queue/priority.orders"
    payments: "/queue/payments"
  topics:
    notifications: "/topic/notifications"
    events: "/topic/events"

scaling:
  enabled: true
  interval_secs: 5
  workers:
    orders: "1-4"           # Auto-scale 1-4 workers
    priority_orders: "2-6"  # Scale 2-6 workers for priority
    payments: "2"           # Fixed 2 workers

retry:
  max_attempts: -1          # Infinite retries
  initial_delay_ms: 1000
  max_delay_ms: 30000
  backoff_multiplier: 2.0

logging:
  level: "info"
```

**📖 For complete configuration options, see [Configuration Reference](../../wiki/Configuration-Reference)**

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Application   │    │  STOMP Library  │    │  Message Broker │
│                 │    │                 │    │  (ActiveMQ/     │
│  ┌───────────┐  │    │  ┌───────────┐  │    │   Artemis)      │
│  │ Producer  │──┼────┼─►│  Client   │──┼────┼─► Queues/Topics │
│  └───────────┘  │    │  │  Pool     │  │    │                 │
│                 │    │  └───────────┘  │    │                 │
│  ┌───────────┐  │    │                 │    │                 │
│  │ Listener  │◄─┼────┼─►┌───────────┐  │    │                 │
│  │ Handlers  │  │    │  │Auto-Scale │◄─┼────┼─► Queue Metrics │
│  └───────────┘  │    │  │  Engine   │  │    │                 │
└─────────────────┘    │  └───────────┘  │    └─────────────────┘
                       └─────────────────┘
```

## 🎭 Supported Brokers

| Broker | Version | Features | Status |
|--------|---------|----------|---------|
| **Apache ActiveMQ Classic** | 5.15+ | Full support with auto-scaling | ✅ Production Ready |
| **Apache Artemis** | 2.17+ | Full support with clustering | ✅ Production Ready |

## 🔧 Prerequisites

- **Rust**: 1.70.0 or later
- **Message Broker**: ActiveMQ Classic or Apache Artemis
- **Operating System**: Linux, macOS, or Windows
- **Docker** (optional): For quick broker setup

## 🎯 Use Cases

- **🛒 E-commerce**: Order processing with auto-scaling based on demand
- **💳 Payment Processing**: Reliable financial transaction handling
- **📊 Data Pipelines**: High-throughput message processing
- **🔔 Notification Systems**: Topic-based event distribution
- **🏭 Industrial IoT**: Device telemetry and command processing
- **📈 Event Streaming**: Real-time event processing and analytics

## 🤝 Contributing

We welcome contributions! Please see our [**Contributing Guide**](../../wiki/Contributing-Guide) for details.

**Quick Start for Contributors:**

1. 🍴 Fork the repository
2. 🌿 Create a feature branch (`git checkout -b feature/amazing-feature`)
3. ✨ Make your changes with tests
4. 🧪 Run `cargo test` and `cargo clippy`
5. 📝 Commit using conventional commits (`git commit -m 'feat: add amazing feature'`)
6. 🚀 Push and create a Pull Request

**Getting Help:**
- 💬 [Discussions](../../discussions) - Ask questions, share ideas
- 🐛 [Issues](../../issues) - Report bugs, request features
- 📚 [Wiki](../../wiki) - Comprehensive documentation
- 👥 [Contributors](../../graphs/contributors) - Meet the team

## 📊 Project Status

| Aspect | Status |
|--------|--------|
| **Stability** | 🟡 Beta - Ready for testing |
| **API** | 🟡 Stabilizing - Minor changes expected |
| **Documentation** | 🟢 Complete - Comprehensive wiki |
| **Testing** | 🟢 Good - Unit, integration & E2E tests |
| **Performance** | 🟢 Optimized - Production ready |
| **Security** | 🟡 Good - Regular security audits |

## 📄 License

This project is licensed under the **MIT License** - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **Apache Software Foundation** for ActiveMQ and Artemis
- **Rust Community** for excellent async ecosystem
- **Contributors** who make this project possible

---

<div align="center">

**📖 [Complete Documentation](../../wiki) | 🚀 [Getting Started](../../wiki/Installation-Guide) | 💬 [Community](../../discussions)**

*Built with ❤️ in Rust for reliable, scalable messaging*

</div>
