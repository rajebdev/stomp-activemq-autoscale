# STOMP Auto-Scaling Service

A production-ready STOMP messaging service implemented in Rust with intelligent auto-scaling capabilities, comprehensive error handling, and automatic reconnection features.

## ✨ Key Features

### 🎯 Auto-Scaling Engine
- **Dynamic Worker Management**: Automatically scale consumer workers based on real-time queue metrics
- **ActiveMQ Integration**: Real-time monitoring via ActiveMQ's Jolokia REST API
- **Smart Scaling Logic**: Configurable thresholds with cooldown periods to prevent flapping
- **Flexible Configuration**: Set min/max worker limits per queue with fallback to static mode

### 🔄 Reliability & Resilience
- **Auto-Reconnection**: Exponential backoff strategy with configurable retry limits
- **Connection Recovery**: Seamless subscription restoration after network failures
- **Graceful Shutdown**: Clean shutdown handling with proper connection cleanup
- **Error Handling**: Comprehensive error management with structured logging

### 🏗️ Architecture
- **Multi-Subscriber Support**: Load distribution for queues, broadcasting for topics
- **Modular Design**: Clean separation of concerns with async/await patterns
- **YAML Configuration**: Flexible configuration with environment variable overrides
- **Production Ready**: Heartbeat management, timeout handling, and monitoring

### How Auto-Scaling Works

1. **Monitor**: Query ActiveMQ every 5 seconds (configurable) for queue metrics
2. **Decide**: Compare queue depth with current worker count
3. **Scale**: Dynamically spawn/stop workers while maintaining connections
4. **Cool Down**: 30-second cooldown prevents rapid scaling changes

## 🚀 Quick Start

### Prerequisites
- Rust 1.70.0 or later
- ActiveMQ or compatible STOMP broker
- ActiveMQ management console (for auto-scaling)

### Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/rajebdev/stomp-activemq-autoscale.git
   cd stomp-activemq-autoscale
   ```

2. **Configure the service**
   Update `config.yaml` with your ActiveMQ settings:
   ```yaml
   broker:
     host: "localhost"
     port: 61613
     credentials:
       username: "admin"
       password: "admin"
   
   monitoring:
     activemq:
       base_url: "http://localhost:8161"
       broker_name: "localhost"
     scaling:
       interval_secs: 5
       scale_down_threshold: 4
       worker_per_queue:
         default: { min: 1, max: 4 }
         high_priority: { min: 2, max: 8 }
   ```

3. **Build and run**
   ```bash
   cargo build --release
   cargo run
   ```

### Expected Output
```
🚀 Starting Auto-Scaling STOMP Application
🎯 Auto-scaling is ENABLED
📈 Scaling up queue 'default': adding 2 workers (queue_size: 8)
✅ Scaled up queue 'default': added 2 workers
```

## 📖 Documentation

### Configuration Reference

#### Broker Settings
```yaml
broker:
  host: "localhost"
  port: 61613
  credentials:
    username: "admin"
    password: "admin"
  heartbeat:
    client_send_secs: 30
    client_receive_secs: 30
```

#### Auto-Scaling Configuration
```yaml
monitoring:
  activemq:
    base_url: "http://localhost:8161"
    broker_name: "localhost"
  scaling:
    interval_secs: 5                    # Check every 5 seconds
    scale_down_threshold: 4             # Scale down when queue < 4
    worker_per_queue:
      demo: { min: 1, max: 2 }
      api_requests: { min: 2, max: 6 }
```

#### Retry & Reconnection
```yaml
retry:
  max_attempts: 5
  initial_delay_ms: 1000
  max_delay_ms: 30000
  backoff_multiplier: 2.0
```

### API Usage

#### Service Creation
```rust
use stomp_activemq_autoscale::{Config, StompService};

let config = Config::from_file("config.yaml")?;
let mut service = StompService::new(config).await?;
```

#### Sending Messages
```rust
// Send to queue
service.send_queue("orders", "Order payload", HashMap::new()).await?;

// Send to topic with headers
let mut headers = HashMap::new();
headers.insert("priority".to_string(), "high".to_string());
service.send_topic("notifications", "Alert message", headers).await?;
```

#### Message Handling
```rust
// Custom queue handler
service.receive_queue("orders", |msg| {
    Box::pin(async move {
        println!("Processing order: {}", msg);
        // Process order logic here
        Ok(())
    })
}).await?;
```

## 🔧 Production Deployment

### Docker
```dockerfile
FROM rust:1.70-slim as builder
WORKDIR /app
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/stomp_activemq_autoscale /usr/local/bin/
COPY config.yaml /app/
WORKDIR /app
CMD ["stomp_activemq_autoscale"]
```

### Environment Variables
```bash
export STOMP_BROKER_HOST=production.broker.com
export STOMP_BROKER_PORT=61613
export STOMP_BROKER_USERNAME=prod_user
export RUST_LOG=info
```

### Systemd Service
```ini
[Unit]
Description=STOMP Auto-Scaling Service
After=network.target

[Service]
Type=exec
User=stomp
ExecStart=/usr/local/bin/stomp_activemq_autoscale
WorkingDirectory=/opt/stomp
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## 📊 Monitoring & Troubleshooting

### Logging
```bash
# Debug mode
RUST_LOG=debug cargo run

# Production logging
RUST_LOG=info cargo run
```

### Health Checks
The service provides health metrics and status information:
- Connection status monitoring
- Worker scaling events
- Message processing rates
- Error rates and recovery

### Common Issues

| Issue | Solution |
|-------|----------|
| Connection refused | Verify broker is running and accessible |
| Authentication failure | Check credentials in config.yaml |
| Auto-scaling not working | Ensure ActiveMQ management console is accessible |
| Messages not processing | Check queue paths and worker configuration |

## 🧪 Testing

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Integration tests (requires running ActiveMQ)
cargo test --test integration
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🏷️ Releases

- **v1.0.0** - Initial release with auto-scaling capabilities
- See [Releases](https://github.com/rajebdev/stomp-activemq-autoscale/releases) for changelog