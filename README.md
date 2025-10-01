# STOMP Auto-Scaling Service

A production-ready STOMP messaging service implemented in Rust with intelligent auto-scaling capabilities, comprehensive error handling, and automatic reconnection features. Supports both **ActiveMQ** and **Apache Artemis** brokers.

## ✨ Key Features

- **🎯 Auto-Scaling Engine**: Dynamic worker management based on real-time queue metrics
- **🔄 Multi-Broker Support**: Works with both ActiveMQ and Apache Artemis
- **🔄 Reliability & Resilience**: Auto-reconnection with exponential backoff
- **🏗️ Production Ready**: Modular design with comprehensive monitoring

### How Auto-Scaling Works

1. **Monitor**: Query broker every 5 seconds for queue metrics
2. **Decide**: Compare queue depth with current worker count  
3. **Scale**: Dynamically spawn/stop workers while maintaining connections
4. **Cool Down**: 30-second cooldown prevents rapid scaling changes

## 🚀 Quick Start

### Prerequisites
- Rust 1.70.0 or later
- ActiveMQ or Apache Artemis STOMP broker
- Broker management console (for auto-scaling)

### Installation

```bash
git clone https://github.com/rajebdev/stomp-activemq-autoscale.git
cd stomp-activemq-autoscale
cargo build --release
cargo run
```

### Basic Usage

```rust
use stomp_activemq_autoscale::runner::StompRunner;
use stomp_activemq_autoscale::utils;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    utils::initialize_logging();
    let config = utils::load_configuration("config.yaml")?;
    
    StompRunner::new()
        .with_config(config)
        .add_queue("orders", handle_orders)
        .add_topic("notifications", handle_notifications)
        .run()
        .await
}

async fn handle_orders(message: String) -> anyhow::Result<()> {
    println!("Processing order: {}", message);
    Ok(())
}

async fn handle_notifications(message: String) -> anyhow::Result<()> {
    println!("Notification: {}", message);
    Ok(())
}
```

## 📚 Documentation

**Complete documentation is available in the [Wiki](docs/wiki/)**

### 📖 Documentation Pages

| Topic | Description |
|-------|-------------|
| **[🏠 Home](docs/wiki/Home.md)** | Overview and getting started |
| **[⚙️ Installation](docs/wiki/Installation.md)** | Setup and prerequisites |
| **[🔧 Configuration](docs/wiki/Configuration.md)** | Complete configuration reference |
| **[🔗 Multi-Broker Support](docs/wiki/Multi-Broker-Support.md)** | ActiveMQ and Artemis setup |
| **[📖 API Reference](docs/wiki/API-Reference.md)** | Complete API documentation |
| **[🧪 Testing](docs/wiki/Testing.md)** | Testing guide and examples |

## 💻 Configuration Example

```yaml
# config.yaml
service:
  name: "stomp-service"
  version: "1.0.0"

broker:
  type: "activemq"          # or "artemis"
  host: "localhost"
  username: "admin"
  password: "admin"
  stomp_port: 61613
  web_port: 8161
  broker_name: "localhost"

destinations:
  queues:
    orders: "/queue/orders"
    payments: "/queue/payments"
  topics:
    events: "/topic/events"

scaling:
  enabled: true
  interval_secs: 5
  workers:
    orders: "1-4"           # Auto-scale 1-4 workers
    payments: "2"           # Fixed 2 workers
```

## 🔧 Dependencies

- **[stomp-rs](https://github.com/rajebdev/stomp-rs)** - STOMP 1.2 client (custom fork)
- **[tokio](https://tokio.rs/)** - Async runtime
- **[serde](https://serde.rs/)** - YAML configuration
- **[anyhow](https://docs.rs/anyhow/)** - Error handling
- **[async-trait](https://docs.rs/async-trait/)** - Broker abstraction

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🏷️ Version

**v0.1.0** - Development version with multi-broker support and auto-scaling

---

📖 **For detailed documentation, examples, and guides, visit the [Wiki](docs/wiki/)**