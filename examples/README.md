# STOMP ActiveMQ Examples

This directory contains examples demonstrating how to use the STOMP ActiveMQ autoscale library.

## Basic Examples

### stomp-basic-examples

Demonstrates basic usage of `StompProducer` and `StompListener` with both simple message sending and advanced header functionality.

#### Features Demonstrated:

1. **Simple Message Sending (without headers)**:
   ```rust
   stomp_producer.send_queue("my-queue", "Hello World").await?;
   stomp_producer.send_topic("my-topic", "Hello World").await?;
   ```

2. **Advanced Message Sending (with JMS standard headers)**:
   ```rust
   let mut headers = HashMap::new();
   headers.insert("JMSPriority".to_string(), "5".to_string()); // 0-9 priority scale
   headers.insert("JMSDeliveryMode".to_string(), "PERSISTENT".to_string());
   headers.insert("JMSCorrelationID".to_string(), "api-12345".to_string());
   headers.insert("JMSType".to_string(), "api_request".to_string());
   
   stomp_producer.send_queue_with_headers("my-queue", 
       r#"{"api": "user_info", "id": 12345}"#, 
       headers).await?;
   ```

3. **Message Listening**:
   ```rust
   let stomp_listener = StompListener::new(config)
       .add_queue("my-queue", handle_queue_message)
       .add_topic("my-topic", handle_topic_message);
   
   let listener_handle = stomp_listener.run_background();
   ```

4. **Simple Graceful Shutdown**:
   ```rust
   // Setup graceful shutdown with signal handling
   tokio::select! {
       _ = tokio::signal::ctrl_c() => {
           // Simple abort shutdown
           listener_handle.abort();
           // Wait for cleanup
           tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
       }
   }
   ```

#### JMS Standard Headers:

- **JMSPriority**: Message priority (0-9 scale, where 9 is highest priority)
- **JMSDeliveryMode**: `PERSISTENT` or `NON_PERSISTENT` 
- **JMSCorrelationID**: Track request-response patterns
- **JMSType**: Message type identifier
- **JMSTimestamp**: Message timestamp (milliseconds since epoch)
- **Content-Type**: Specify message format (`application/json`, `text/plain`, etc.)

#### Simple Shutdown Features:

- **Signal Handling**: Responds to Ctrl+C (SIGINT) for clean shutdown
- **Task Abort**: Uses tokio task abort for immediate stop
- **Cleanup Time**: Allows time for final message processing before exit
- **Built-in Shutdown**: The underlying StompRunner handles complex shutdown logic automatically

#### Running the Example:

```bash
cd examples/stomp-basic-examples
cargo run
```

Make sure you have ActiveMQ running and configured properly in `config.yaml` before running the examples.

## Configuration

Examples use the same `config.yaml` from the parent directory. Ensure your ActiveMQ broker is running and accessible at the configured host and port.

## Dependencies

The examples include all necessary dependencies:
- `stomp-activemq-autoscale` (main library)
- `tokio` (async runtime)
- `tracing` (logging)
- `chrono` (timestamp handling)
- `anyhow` (error handling)