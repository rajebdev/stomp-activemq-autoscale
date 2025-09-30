# STOMP Auto-Scaling Service

A production-ready STOMP messaging service implemented in Rust with intelligent auto-scaling capabilities, comprehensive error handling, and automatic reconnection features. Supports both **ActiveMQ** and **Apache Artemis** brokers.

## ✨ Key Features

### 🎯 Auto-Scaling Engine
- **Dynamic Worker Management**: Automatically scale consumer workers based on real-time queue metrics
- **Multi-Broker Support**: Works with both ActiveMQ and Apache Artemis
- **Real-time Monitoring**: Monitors via broker's Jolokia REST API
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

1. **Monitor**: Query broker every 5 seconds (configurable) for queue metrics
2. **Decide**: Compare queue depth with current worker count
3. **Scale**: Dynamically spawn/stop workers while maintaining connections
4. **Cool Down**: 30-second cooldown prevents rapid scaling changes

## 🚀 Quick Start

### Prerequisites
- Rust 1.70.0 or later
- ActiveMQ or Apache Artemis STOMP broker
- Broker management console (for auto-scaling)

### Installation & Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/rajebdev/stomp-activemq-autoscale.git
   cd stomp-activemq-autoscale
   ```

2. **Configure the service**
   Create or update `config.yaml` with your broker settings:
   ```yaml
   # Service metadata
   service:
     name: "stomp-service"
     version: "1.0.0"
     description: "Production STOMP messaging service"

   # Broker configuration (supports ActiveMQ and Artemis)
   broker:
     type: "activemq"             # Broker type: "activemq" or "artemis"
     host: "localhost"            # Broker server host
     username: "admin"            # STOMP credentials
     password: "admin"
     stomp_port: 61613           # STOMP messaging port
     web_port: 8161             # Web console port (for monitoring)
     heartbeat_secs: 30         # Connection heartbeat
     broker_name: "localhost"   # Broker name (ActiveMQ: "localhost", Artemis: "broker")

   # Message destinations
   destinations:
     queues:
       default: "/queue/demo"
       api_requests: "/queue/api.requests"
       errors: "/queue/errors"
     topics:
       notifications: "/topic/notifications"
       events: "/topic/events"

   # Auto-scaling configuration
   scaling:
     enabled: true                 # Enable auto-scaling
     interval_secs: 5             # Monitor every 5 seconds
     workers:
       default: "1-4"             # Auto-scale: 1-4 workers
       api_requests: "2-6"        # Auto-scale: 2-6 workers
       errors: "2"                # Fixed: exactly 2 workers
   ```

3. **Build and run**
   ```bash
   cargo build --release
   cargo run
   ```

### Expected Output
```
🚀 Starting STOMP Auto-Scaling Service
📋 Service: stomp-service v1.0.0
🔗 ActiveMQ: localhost:61613 (Web: 8161)
🎯 Auto-scaling: ENABLED
� Queue 'default' workers: 1-4 (auto-scaling)
📊 Queue 'api_requests' workers: 2-6 (auto-scaling)
📊 Queue 'errors' workers: 2 (fixed)
📈 Scaling monitor started (interval: 5s)
✅ All services initialized successfully
```

## 📖 Documentation

### Multi-Broker Support

The service supports both **ActiveMQ** and **Apache Artemis** brokers through a unified configuration:

#### ActiveMQ Configuration
```yaml
broker:
  type: "activemq"
  host: "localhost"
  broker_name: "localhost"       # ActiveMQ uses "localhost"
  # ... other settings
  
destinations:
  queues:
    default: "/queue/demo"       # Standard ActiveMQ queue format
```

#### Apache Artemis Configuration  
```yaml
broker:
  type: "artemis"
  host: "10.0.7.127"
  broker_name: "broker"          # Artemis typically uses "broker"
  # ... other settings
  
destinations:
  queues:
    default: "jns.db.transaction" # Artemis queue format (no /queue/ prefix)
```

### Configuration Reference

#### Broker Settings (Unified Configuration)
```yaml
broker:
  type: "activemq"               # Broker type: "activemq" or "artemis"
  host: "localhost"              # Broker server hostname
  username: "admin"              # STOMP authentication username
  password: "admin"              # STOMP authentication password
  stomp_port: 61613             # STOMP messaging port
  web_port: 8161                # Web console port (for monitoring)
  heartbeat_secs: 30            # Connection heartbeat interval
  broker_name: "localhost"      # Broker name for API queries
```

#### Auto-Scaling Configuration
```yaml
scaling:
  enabled: true                   # Enable/disable auto-scaling
  interval_secs: 5               # Monitor queue metrics every N seconds
  workers:
    # Auto-scaling ranges (format: "min-max")
    default: "1-4"               # Scale between 1-4 workers
    api_requests: "2-6"          # Scale between 2-6 workers
    high_priority: "1-8"         # Scale between 1-8 workers
    
    # Fixed worker count (format: "count")
    errors: "2"                  # Always exactly 2 workers
    logs: "1"                    # Always exactly 1 worker
```

#### Message Destinations
```yaml
destinations:
  queues:                        # Queue destinations (competing consumers)
    default: "/queue/demo"
    api_requests: "/queue/api.requests"
    orders: "/queue/orders"
    errors: "/queue/errors"
  
  topics:                        # Topic destinations (broadcast)
    notifications: "/topic/notifications"
    events: "/topic/events"
    alerts: "/topic/alerts"
```

#### Optional Advanced Settings
```yaml
# Consumer behavior (optional - has defaults)
consumers:
  ack_mode: "client_individual"  # Message acknowledgment mode

# Retry/reconnection settings (optional - has defaults)
retry:
  max_attempts: -1               # Infinite retries (-1) or max attempts
  initial_delay_ms: 1000        # Start with 1 second delay
  max_delay_ms: 30000           # Cap at 30 seconds
  backoff_multiplier: 2.0       # Exponential backoff factor

# Graceful shutdown (optional - has defaults)
shutdown:
  timeout_secs: 30              # Maximum shutdown time
  grace_period_secs: 5          # Grace period for cleanup

# Logging (optional - use RUST_LOG env var instead)
logging:
  level: "info"                 # Log level: trace, debug, info, warn, error
  output: "stdout"              # Output destination
```

### API Usage

### API Usage

#### Basic Service Setup
```rust
use stomp_activemq_autoscale::runner::StompRunner;
use stomp_activemq_autoscale::utils;
use anyhow::Result;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    utils::initialize_logging();
    
    // Load configuration from file
    let config = utils::load_configuration("config.yaml")?;
    
    // Display startup information
    utils::display_startup_info(&config);
    
    // Create and run the service with custom handlers
    StompRunner::new()
        .with_config(config)
        .add_queue("orders", handle_order_message)
        .add_queue("api_requests", handle_api_request)
        .add_topic("notifications", handle_notification)
        .run()
        .await
}
```

#### Custom Message Handlers
```rust
// Order processing handler
async fn handle_order_message(message: String) -> Result<()> {
    println!("🛒 Processing ORDER: {}", message);
    
    // Parse order data
    let order: Order = serde_json::from_str(&message)?;
    
    // Process the order
    process_order(order).await?;
    
    println!("✅ Order processed successfully");
    Ok(())
}

// API request handler with error handling
async fn handle_api_request(message: String) -> Result<()> {
    println!("🌐 Processing API REQUEST: {}", message);
    
    // Simulate API processing time
    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    
    // Your API logic here
    match process_api_request(&message).await {
        Ok(response) => println!("✅ API request completed: {}", response),
        Err(e) => println!("❌ API request failed: {}", e),
    }
    
    Ok(())
}

// Notification handler for topics
async fn handle_notification(message: String) -> Result<()> {
    println!("🔔 Broadcasting NOTIFICATION: {}", message);
    
    // Send notification to multiple channels
    broadcast_to_channels(&message).await?;
    
    Ok(())
}
```

#### Configuration-Only Setup
```rust
// Simple setup using only configuration file
#[tokio::main]
async fn main() -> Result<()> {
    utils::initialize_logging();
    
    let config = utils::load_configuration("config.yaml")?;
    
    // Uses default message handlers for all destinations
    StompRunner::new()
        .with_config(config)
        .run()  // Auto-discovers destinations from config
        .await
}
```

#### Sending Test Messages
```rust
// Send test messages programmatically
async fn send_test_messages(config: &Config) -> Result<()> {
    let mut service = StompService::new(config.clone()).await?;
    
    // Send to queue
    service.send_queue("orders", r#"{"order_id": 12345, "amount": 99.99}"#).await?;
    
    // Send to topic with headers
    let headers = [("priority", "high"), ("type", "alert")]
        .iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
    
    service.send_topic("notifications", "System maintenance in 5 minutes", headers).await?;
    
    Ok(())
}
```

## 🧪 Testing

### Unit Tests
```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test module
cargo test config

# Test with logging enabled
RUST_LOG=debug cargo test
```

### Integration Testing
```bash
# Start ActiveMQ (required for integration tests)
docker run -d --name activemq \
  -p 61613:61613 -p 8161:8161 \
  apache/activemq-artemis

# Run integration tests
cargo test --test integration

# Clean up
docker stop activemq && docker rm activemq
```

### Manual Testing
```bash
# 1. Configure your ActiveMQ settings in config.yaml
# 2. Start the service with debug logging
RUST_LOG=debug cargo run

# 3. In another terminal, send test messages using ActiveMQ web console:
# http://localhost:8161/admin (admin/admin)
```

### Load Testing
```bash
# Enable production optimizations
cargo build --release

# Run with performance logging
RUST_LOG=info ./target/release/stomp_activemq_autoscale

# Monitor scaling behavior with high message volume
```

## 🚀 Deployment

### Environment Variables
Override configuration with environment variables:
```bash
# ActiveMQ connection
export ACTIVEMQ_HOST=production.activemq.com
export ACTIVEMQ_STOMP_PORT=61613
export ACTIVEMQ_USERNAME=prod_user
export ACTIVEMQ_PASSWORD=secure_password

# Logging
export RUST_LOG=info

# Scaling
export SCALING_ENABLED=true
export SCALING_INTERVAL_SECS=10
```

### Docker Deployment
```dockerfile
# Multi-stage build for optimal image size
FROM rust:1.75-slim as builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy source code
WORKDIR /app
COPY . .

# Build release binary
RUN cargo build --release

# Runtime image
FROM debian:bookworm-slim

# Install CA certificates for HTTPS
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 stomp

# Copy binary and config template
COPY --from=builder /app/target/release/stomp_activemq_autoscale /usr/local/bin/
COPY config.yaml /app/config.yaml.template

# Set permissions
RUN chown stomp:stomp /usr/local/bin/stomp_activemq_autoscale
RUN chown -R stomp:stomp /app

USER stomp
WORKDIR /app

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
  CMD pgrep stomp_activemq_autoscale || exit 1

EXPOSE 8080
CMD ["stomp_activemq_autoscale"]
```

### Kubernetes Deployment
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: stomp-autoscaler
  labels:
    app: stomp-autoscaler
spec:
  replicas: 2
  selector:
    matchLabels:
      app: stomp-autoscaler
  template:
    metadata:
      labels:
        app: stomp-autoscaler
    spec:
      containers:
      - name: stomp-autoscaler
        image: stomp-activemq-autoscale:latest
        ports:
        - containerPort: 8080
        env:
        - name: ACTIVEMQ_HOST
          value: "activemq-service"
        - name: ACTIVEMQ_STOMP_PORT
          value: "61613"
        - name: RUST_LOG
          value: "info"
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        livenessProbe:
          exec:
            command:
            - pgrep
            - stomp_activemq_autoscale
          initialDelaySeconds: 30
          periodSeconds: 30
        readinessProbe:
          exec:
            command:
            - pgrep  
            - stomp_activemq_autoscale
          initialDelaySeconds: 10
          periodSeconds: 10
```

### Systemd Service
```ini
[Unit]
Description=STOMP Auto-Scaling Service
After=network.target

[Service]
Type=exec
User=stomp
Group=stomp
ExecStart=/usr/local/bin/stomp_activemq_autoscale
WorkingDirectory=/opt/stomp
Environment=RUST_LOG=info
Environment=CONFIG_PATH=/opt/stomp/config.yaml

# Security settings
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/stomp

# Restart configuration
Restart=on-failure
RestartSec=10
StartLimitInterval=60
StartLimitBurst=3

[Install]
WantedBy=multi-user.target
```

## 📊 Monitoring & Troubleshooting

### Application Monitoring
The service provides comprehensive monitoring and logging:
```bash
# Enable different log levels
export RUST_LOG=debug    # Detailed debug information
export RUST_LOG=info     # General operational info (recommended)
export RUST_LOG=warn     # Warnings and errors only
export RUST_LOG=error    # Errors only

# Component-specific logging
export RUST_LOG=stomp_activemq_autoscale::autoscaler=debug,info
```

### Health Checks
Monitor service health and performance:
- **Connection Status**: Real-time STOMP connection monitoring
- **Worker Scaling Events**: Track auto-scaling decisions and actions
- **Message Processing Rates**: Queue throughput and processing times
- **Error Rates**: Connection failures, message processing errors
- **ActiveMQ Integration**: Monitor queue depths and broker connectivity

### Auto-Scaling Metrics
Watch for these key indicators:
```
📈 Scaling up queue 'api_requests': adding 2 workers (queue_size: 15, current: 2)
✅ Scaled up queue 'api_requests': added 2 workers
📉 Scaling down queue 'default': removing 1 workers (queue_size: 2, current: 4)
✅ Scaled down queue 'default': removed 1 workers
```

### Common Issues & Solutions

| Issue | Symptoms | Solution |
|-------|----------|----------|
| **Connection Refused** | `Connection refused` errors | • Verify ActiveMQ is running<br>• Check host/port in config<br>• Verify firewall settings |
| **Authentication Failed** | `401 Unauthorized` errors | • Check username/password in config<br>• Verify ActiveMQ user permissions<br>• Test credentials via web console |
| **Auto-Scaling Not Working** | Fixed worker count despite queue depth | • Verify ActiveMQ web console is accessible<br>• Check `activemq.web_port` configuration<br>• Ensure monitoring credentials are correct |
| **Messages Not Processing** | Messages stuck in queues | • Check queue paths match ActiveMQ config<br>• Verify message handler functions<br>• Check for processing errors in logs |
| **High Memory Usage** | Service consuming too much RAM | • Reduce worker counts in scaling config<br>• Check for message handler memory leaks<br>• Consider processing smaller message batches |
| **Connection Drops** | Frequent reconnection attempts | • Increase `heartbeat_secs` value<br>• Check network stability<br>• Verify ActiveMQ connection limits |

### Debug Mode
Enable detailed debugging for troubleshooting:
```bash
# Maximum verbosity
RUST_LOG=trace cargo run

# Component-specific debugging
RUST_LOG=stomp_activemq_autoscale::runner=debug cargo run

# Network debugging
RUST_LOG=stomp_activemq_autoscale::service=debug cargo run
```

### Performance Tuning
Optimize for your workload:
```yaml
# High-throughput configuration
scaling:
  enabled: true
  interval_secs: 3      # More frequent monitoring
  workers:
    high_volume: "4-16"  # Higher maximum workers

activemq:
  heartbeat_secs: 60    # Less frequent heartbeats

# Low-latency configuration  
scaling:
  enabled: true
  interval_secs: 1      # Very frequent monitoring
  workers:
    realtime: "8-12"    # Keep workers ready

activemq:
  heartbeat_secs: 10    # Quick failure detection
```

## 🔧 Dependencies

### Core Dependencies
- **[stomp-rs](https://github.com/rajebdev/stomp-rs)** - STOMP 1.2 client library (custom fork)
- **[tokio](https://tokio.rs/)** - Async runtime with full features
- **[anyhow](https://docs.rs/anyhow/)** - Flexible error handling
- **[serde](https://serde.rs/)** - Serialization framework with YAML support
- **[tracing](https://docs.rs/tracing/)** - Structured application logging

### Networking & HTTP
- **[reqwest](https://docs.rs/reqwest/)** - HTTP client for ActiveMQ monitoring API
- **[rustls](https://docs.rs/rustls/)** - TLS implementation for secure connections

### Utilities
- **[uuid](https://docs.rs/uuid/)** - Unique identifier generation
- **[chrono](https://docs.rs/chrono/)** - Date and time handling
- **[futures](https://docs.rs/futures/)** - Additional async utilities

### Development Tools
```toml
[dependencies]
stomp = { git = "https://github.com/rajebdev/stomp-rs", branch = "master" }
tokio = { version = "1.32", features = ["full", "signal"] }
futures = "0.3"
serde = { version = "1.0", features = ["derive"] }
serde_yaml = "0.9"
serde_json = "1.0"
anyhow = "1.0"
thiserror = "1.0"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
uuid = { version = "1.0", features = ["v4"] }
chrono = { version = "0.4", features = ["serde"] }
reqwest = { version = "0.11", features = ["json", "rustls-tls"] }
urlencoding = "2.1"
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