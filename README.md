# STOMP Auto-Scaling Service

A production-ready STOMP service implementation in Rust with **auto-scaling capabilities**, comprehensive configuration, error handling, and auto-reconnection features.

## 🎯 Auto-Scaling Features

This service can automatically scale the number of consumer workers up and down based on real-time queue metrics from ActiveMQ:

- **Dynamic Worker Scaling**: Automatically adjust the number of consumers per queue based on queue depth
- **ActiveMQ Integration**: Real-time monitoring via ActiveMQ's Jolokia REST API
- **Configurable Scaling Rules**: Set min/max worker limits and scaling thresholds per queue
- **Smart Scaling Logic**: Includes cooldown periods and hysteresis to prevent flapping
- **Backward Compatibility**: Falls back to static worker mode when auto-scaling is disabled

### How Auto-Scaling Works

1. **Monitoring**: Every 5 seconds (configurable), the service queries ActiveMQ for queue metrics
2. **Decision Engine**: Compares queue depth with current worker count:
   - **Scale Up**: If `queue_size > current_workers` and `current_workers < max_workers`
   - **Scale Down**: If `queue_size < scale_down_threshold` (default: 4) and `current_workers > min_workers`
3. **Execution**: Dynamically spawns or stops consumer workers while maintaining STOMP connections
4. **Cooldown**: Prevents rapid scaling changes with a 30-second cooldown period

## Architecture

The application follows a modular architecture with the following components:

### Core Modules

- **`main.rs`** - Application entry point with async runtime and graceful shutdown
- **`config.rs`** - Configuration management using YAML files
- **`service.rs`** - Core STOMP service with send/receive operations
- **`handler.rs`** - Message handlers for processing topic and queue messages

### Features

✅ **Auto-Reconnection with Exponential Backoff** 🚀 **NEW**
- Automatic detection and recovery from connection failures
- Exponential backoff strategy prevents overwhelming the broker
- Configurable retry limits and delay settings
- Distinguishes between temporary and permanent errors
- Seamless subscription restoration after reconnection
- Graceful shutdown handling during reconnection attempts

✅ **Multi-Subscriber Architecture** ⭐
- Configure multiple concurrent subscribers per destination
- Load distribution for queues (competing consumers)
- Broadcasting for topics (all subscribers receive messages)
- Individual connection management per subscriber
- Subscriber-specific logging and monitoring

✅ **Complete STOMP Integration**
- Send messages to topics and queues
- Receive messages with custom handlers
- Individual message acknowledgment
- Graceful connection management

✅ **Configuration Management**
- YAML-based configuration with auto-scaling and static worker settings
- Environment variable overrides
- Broker connection settings
- Destination management

✅ **Robust Message Handling**
- Async/await patterns throughout
- Proper error handling and propagation
- Structured logging with tracing
- Message processing metrics

✅ **Production Ready**
- Coordinated graceful shutdown for all subscribers
- Connection retry logic
- Heartbeat configuration
- Timeout handling

## Quick Start

### Prerequisites

- Rust 1.70.0 or later
- A running STOMP broker (ActiveMQ, RabbitMQ, etc.)

### Configuration

Update `config.yaml` with your broker details and **auto-scaling settings**:

```yaml
service:
  name: "stomp-autoscaler"
  version: "1.0.0"
  description: "Auto-scaling STOMP messaging service"

broker:
  host: "10.0.7.127"  # Your STOMP broker IP
  port: 31333         # Your STOMP broker port
  credentials:
    username: "admin"
    password: "admin"
  heartbeat:
    client_send_secs: 30
    client_receive_secs: 30

# 🎯 NEW: Auto-scaling monitoring configuration
monitoring:
  # ActiveMQ management console settings
  activemq:
    base_url: "http://10.0.7.127:8161"  # ActiveMQ management console URL
    broker_name: "localhost"             # Broker name (usually "localhost")
    credentials:                          # Optional credentials for management console
      username: "admin"
      password: "admin"
  
  # Auto-scaling configuration
  scaling:
    interval_secs: 5                    # Check queue metrics every 5 seconds
    scale_down_threshold: 4             # Scale down when queue count < 4
    
    # Queue-specific worker scaling rules: "queue_name: min-max"
    worker_per_queue:
      demo:
        min: 1  # Minimum 1 worker always running
        max: 2  # Maximum 2 workers under high load
      default:
        min: 1  # Minimum 1 worker always running
        max: 4  # Maximum 4 workers under high load
      api_requests:
        min: 2  # Always have 2 workers for API requests
        max: 6  # Scale up to 6 workers if needed
      errors:
        min: 1  # Single error handler minimum
        max: 2  # Max 2 error handlers

# Legacy static worker configuration (used when monitoring is disabled)
consumers:
  ack_mode: "client_individual"
  workers_per_queue:
    default: 2
    api_requests: 4
    errors: 1
  # Note: Topics always use 1 worker (no configuration needed)

retry:
  max_attempts: -1         # Infinite retries for production
  initial_delay_ms: 1000
  max_delay_ms: 30000
  backoff_multiplier: 2.0
```

### Running the Application

```bash
# Build and run with auto-scaling (requires ActiveMQ management console)
cargo run

# Or build first, then run
cargo build --release
./target/release/stomp_app_with_auto_scalling
```

### ✨ Quick Start with Auto-Scaling

1. **Ensure ActiveMQ is running** with management console enabled on port 8161
2. **Update config.yaml** with your broker details and enable monitoring section
3. **Configure scaling ranges** for your queues (e.g., demo: 1-2, default: 1-4)
4. **Run the application** - it will automatically start with minimum workers and scale based on queue depth

**Expected Output:**
```
🚀 Starting Auto-Scaling STOMP Application: stomp-autoscaler
🎯 Auto-scaling is ENABLED
🏊 Creating consumer pool for 'demo' (workers: 1-2)
🏊 Creating consumer pool for 'default' (workers: 1-4)
✅ All consumer pools initialized
🎯 Auto-scaler initialized with 2 queues
🔄 Starting auto-scaling monitor (interval: 5 seconds)
📈 Scaling up queue 'default': adding 2 workers (queue_size: 8)
✅ Scaled up queue 'default': added 2 workers
📉 Scaling down queue 'demo': removing 1 workers (queue_size: 1)
✅ Scaled down queue 'demo': removed 1 workers
```

### Example Output

```
🚀 Starting Multi-Subscriber STOMP Application: stomp-service
📋 Version: 1.0.0
🔗 Broker: 10.0.7.127:31333
⚙️ Auto-reconnect: 5 attempts, 1s-30s delays, 2.0x backoff
📊 Queue 'default' configured with 2 workers
📊 Queue 'api_requests' configured with 4 workers
📊 Queue 'errors' configured with 1 workers
📊 Topic 'notifications' configured with 2 workers
📊 Topic 'events' configured with 1 workers
🔧 Spawning 10 total subscribers...
[default][default#1] Starting subscriber with auto-reconnection...
[default][default#2] Starting subscriber with auto-reconnection...
[api_requests][api_requests#1] Starting subscriber with auto-reconnection...
[notifications][notifications#1] Starting subscriber with auto-reconnection...
[notifications][notifications#2] Starting subscriber with auto-reconnection...
📤 Sending test messages to multiple subscribers...
✅ Test messages sent successfully
🔄 Multi-subscriber system running with auto-reconnect... Press Ctrl+C to shutdown gracefully
```

## Multi-Subscriber Behavior

### Queue Workers (Load Distribution)

With multiple subscribers on a **queue**, messages are **distributed** among workers:

```
Queue: api_requests (4 workers)
├─ api_requests#1 ← Message A
├─ api_requests#2 ← Message B  
├─ api_requests#3 ← Message C
└─ api_requests#4 ← Message D
```

**Use Cases:**
- High-throughput message processing
- Load balancing across multiple workers
- Parallel processing of independent tasks

### Topic Workers (Broadcasting)

With multiple subscribers on a **topic**, **all workers** receive each message:

```
Topic: notifications (2 workers)
├─ notifications#1 ← Message X (copy 1)
└─ notifications#2 ← Message X (copy 2)
```

**Use Cases:**
- Event broadcasting to multiple handlers
- Redundant processing for reliability
- Different processing logic per subscriber

### Subscriber Logging

Each subscriber logs with a unique identifier for easy tracking:

```
[api_requests][api_requests#2] Processing queue message: {"order_id": 12345}
[api_requests][api_requests#2] ✅ Message processed successfully in 45ms
[notifications][notifications#1] Processing topic message: Event broadcast
[notifications][notifications#2] Processing topic message: Event broadcast
```

### Connection Resilience 🚀 **NEW**

The system automatically handles connection failures and network issues:

```
[api_requests][api_requests#1] 🔌 Session disconnected: NetworkError
[api_requests][api_requests#1] 🔴 Connection marked as unhealthy
[api_requests][api_requests#1] Connection unhealthy, attempting reconnection...
[api_requests][api_requests#1] 🔄 Starting reconnection process...
[api_requests][api_requests#1] 🔁 Reconnection attempt 1 of 5 in 1000ms
[api_requests][api_requests#1] ⚠️ Reconnection attempt 1 failed (retryable): Connection refused
[api_requests][api_requests#1] 🔁 Reconnection attempt 2 of 5 in 2000ms
[api_requests][api_requests#1] ✅ Successfully reconnected to STOMP broker
[api_requests][api_requests#1] 🔄 Resubscribing to 1 destinations
[api_requests][api_requests#1] Reconnection successful, resuming subscription
[api_requests][api_requests#1] 📥 Starting queue subscription...
```

📚 **Detailed Guide**: See [AUTO_RECONNECT_GUIDE.md](AUTO_RECONNECT_GUIDE.md) for comprehensive documentation

## API Specification

### Service Methods

The `StompService` provides the following methods as specified:

```rust
// Constructor
pub async fn new(config: Config) -> Result<Self>

// Send message to topic
pub async fn send_topic(&mut self, topic_name: &str, payload: &str, headers: HashMap<String, String>) -> Result<()>

// Send message to queue
pub async fn send_queue(&mut self, queue_name: &str, payload: &str, headers: HashMap<String, String>) -> Result<()>

// Receive messages from topic with handler
pub async fn receive_topic<F>(&mut self, topic_name: &str, handler: F) -> Result<()>

// Receive messages from queue with handler
pub async fn receive_queue<F>(&mut self, queue_name: &str, handler: F) -> Result<()>
```

### Handler Methods

The `MessageHandlers` provides default handlers:

```rust
// Constructor
pub fn new() -> Self

// Topic message handler
pub async fn topic_handler(msg: String) -> Result<()>

// Queue message handler
pub async fn queue_handler(msg: String) -> Result<()>
```

## Usage Examples

### Sending Messages

```rust
let mut service = StompService::new(config).await?;

// Send to topic with headers
let mut headers = HashMap::new();
headers.insert("priority".to_string(), "high".to_string());
service.send_topic("notifications", "Hello Topic!", headers).await?;

// Send to queue
service.send_queue("default", "Hello Queue!", HashMap::new()).await?;
```

### Receiving Messages

```rust
// Listen to topic with custom handler
service.receive_topic("notifications", |msg| {
    Box::pin(async move {
        println!("Topic message: {}", msg);
        Ok(())
    })
}).await?;

// Listen to queue with default handler
service.receive_queue("default", |msg| {
    Box::pin(MessageHandlers::queue_handler(msg))
}).await?;
```

## Configuration Reference

### Broker Settings

```yaml
broker:
  host: "localhost"        # STOMP broker hostname
  port: 61613             # STOMP port (default: 61613)
  credentials:            # Optional authentication
    username: "admin"
    password: "admin"
  heartbeat:              # Connection heartbeat (milliseconds)
    client_send_secs: 10000
    client_receive_secs: 10000
  retry:                  # Auto-reconnection settings (🚀 NEW)
    max_attempts: 5              # Maximum reconnection attempts
    initial_delay_ms: 1000       # Initial delay in milliseconds
    max_delay_ms: 30000          # Maximum delay cap in milliseconds
    backoff_multiplier: 2.0      # Exponential backoff multiplier
  headers: {}             # Custom connection headers
```

### Multi-Subscriber Settings ⭐

```yaml
consumers:
  ack_mode: "client_individual"  # Acknowledgment mode
  
  # Queue workers (competing consumers)
  workers_per_queue:
    high_volume: 8      # 8 workers for load balancing
    medium_load: 4      # 4 workers for moderate load
    low_priority: 1     # Single worker for low-priority tasks
    
  # Note: Topics always use exactly 1 worker each (no configuration needed)
  # This ensures proper broadcast behavior where all subscribers receive messages

# Auto-Reconnection Settings 🚀 **NEW**
retry:
  max_attempts: 5            # Maximum reconnection attempts
  initial_delay_ms: 1000     # Initial delay (1 second)
  max_delay_ms: 30000        # Maximum delay (30 seconds)  
  backoff_multiplier: 2.0    # Exponential growth factor
```

### Destinations

```yaml
destinations:
  queues:
    default:
      path: "/queue/demo"
      headers: {}
    api_requests:
      path: "/queue/api.requests"
      headers:
        persistent: "true"
  
  topics:
    notifications:
      path: "/topic/notifications"
      headers: {}
    events:
      path: "/topic/events"
      headers:
        persistent: "false"
```

## Testing

Run the unit tests:

```bash
cargo test
```

Test with a real STOMP broker:

```bash
# Make sure ActiveMQ or another STOMP broker is running
# Update config.yaml with correct broker details
cargo run
```

## Dependencies

- **stomp** - STOMP 1.2 client library
- **tokio** - Async runtime
- **anyhow** - Error handling
- **serde/serde_yaml** - Configuration parsing
- **tracing** - Structured logging
- **futures** - Async utilities
- **uuid** - Unique ID generation
- **chrono** - Date/time handling

## Production Deployment

### Environment Variables

Override configuration with environment variables:

```bash
export STOMP_BROKER_HOST=production.broker.com
export STOMP_BROKER_PORT=61613
export STOMP_BROKER_USERNAME=prod_user
export STOMP_LOGGING_LEVEL=warn
```

### Docker

Create a `Dockerfile`:

```dockerfile
FROM rust:1.70-slim as builder
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
COPY --from=builder /target/release/stomp_app /usr/local/bin/
COPY config.yaml /app/
WORKDIR /app
CMD ["stomp_app"]
```

### Systemd Service

```ini
[Unit]
Description=STOMP Service
After=network.target

[Service]
Type=exec
User=stomp
ExecStart=/usr/local/bin/stomp_app
WorkingDirectory=/opt/stomp
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Troubleshooting

### Common Issues

1. **Connection refused**
   - Check broker is running and accessible
   - Verify host/port in config.yaml
   - Check firewall settings

2. **Authentication failures**
   - Verify credentials in config.yaml
   - Check broker user permissions

3. **Message not received**
   - Verify destination paths match broker configuration
   - Check queue vs topic usage
   - Ensure subscriber is connected before sending

### Debug Logging

Enable debug logging:

```bash
RUST_LOG=debug cargo run
```

Or set in config.yaml:
```yaml
logging:
  level: "debug"
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.