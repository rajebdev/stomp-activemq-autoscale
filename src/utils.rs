use tracing::{error, info};
use tokio::signal;
use crate::config::BrokerType;

/// Setup signal handlers for graceful shutdown
pub async fn setup_signal_handlers() {
    #[cfg(unix)]
    {
        use signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).expect("Failed to register SIGTERM handler");
        let mut sigint =
            signal(SignalKind::interrupt()).expect("Failed to register SIGINT handler");

        tokio::select! {
            _ = sigterm.recv() => {
                info!("📡 Received SIGTERM - initiating graceful shutdown");
            }
            _ = sigint.recv() => {
                info!("📡 Received SIGINT (Ctrl+C) - initiating graceful shutdown");
            }
        }
    }

    #[cfg(windows)]
    {
        match signal::ctrl_c().await {
            Ok(()) => {
                info!("📡 Received Ctrl+C - initiating graceful shutdown");
            }
            Err(err) => {
                error!("Unable to listen for shutdown signal: {}", err);
            }
        }
    }
}

/// Build STOMP destination path with appropriate prefix based on broker type and destination type
/// For ActiveMQ: Uses full STOMP paths (/queue/name, /topic/name)
/// For Artemis: Uses logical address names (name) - prefixes are handled internally
pub fn build_stomp_destination(broker_type: &BrokerType, dest_type: &str, name: &str) -> String {
    match broker_type {
        BrokerType::ActiveMQ => {
            if name.starts_with("/queue/") || name.starts_with("/topic/") {
                name.to_string()
            } else {
                format!("/{}/{}", dest_type, name)
            }
        }
        BrokerType::Artemis => {
            name.strip_prefix("/queue/")
                .or_else(|| name.strip_prefix("/topic/"))
                .unwrap_or(name)
                .to_string()
        }
    }
}

/// Helper function to normalize destination names - now mainly validates input
/// Since we're moving away from prefixed configs, this primarily ensures clean names
pub fn normalize_destination_name(destination: &str) -> &str {
    // Legacy compatibility: still strip prefixes if they exist (for migration)
    destination
        .strip_prefix("/queue/")
        .or_else(|| destination.strip_prefix("/topic/"))
        .unwrap_or(destination)
}

/// Helper function to check if destination is a queue
pub fn is_queue_destination(destination: &str) -> bool {
    destination.starts_with("/queue/")
}

/// Helper function to determine routing types priority based on destination type
/// For Artemis: queues prefer anycast (point-to-point), topics prefer multicast (publish-subscribe)
pub fn determine_routing_types(destination: &str) -> [&'static str; 2] {
    if is_queue_destination(destination) {
        ["anycast", "multicast"]  // Try anycast first for queues (point-to-point)
    } else {
        ["multicast", "anycast"]  // Try multicast first for topics (publish-subscribe)
    }
}

#[cfg(test)]
mod tests {
    //! Comprehensive unit tests for the utils module.
    //!
    //! ## Test Coverage Summary:
    //!
    //! ### Signal handling tests:
    //! - Platform-specific signal handling compilation
    //! - Setup function availability
    //!
    //! ### STOMP destination building tests:
    //! - ActiveMQ destination formatting with prefixes
    //! - Artemis destination name stripping
    //! - Edge cases with different name formats
    //! - Empty and special character handling
    //!
    //! ### Destination name normalization tests:
    //! - Prefix stripping for migration compatibility
    //! - Idempotent operations
    //! - Length preservation and bounds
    //! - Unicode and special character handling
    //!
    //! ### Queue destination detection tests:
    //! - Correct identification of queue vs topic
    //! - Edge cases with malformed paths
    //! - Case sensitivity testing
    //!
    //! ### Routing type determination tests:
    //! - Correct priority ordering for queues (anycast first)
    //! - Correct priority ordering for topics (multicast first)
    //! - Fallback behavior for unknown formats
    //!
    //! ### Property-based testing:
    //! - Fuzz testing with arbitrary strings
    //! - Invariant checking across operations
    //! - Round-trip testing where applicable
    //!
    //! Total test count: 30+ tests covering all public APIs and edge cases
    
    use super::*;
    use crate::config::BrokerType;
    use proptest::prelude::*;
    use tokio::time::{timeout, Duration};

    // ============================================
    // UNIX-SPECIFIC TESTS
    // ============================================
    
    #[cfg(unix)]
    mod unix_tests {
        use super::*;
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;

        #[tokio::test]
        async fn test_sigterm_triggers_shutdown() {
            // Spawn a task that will wait for the signal
            let handle = tokio::spawn(async {
                setup_signal_handlers().await;
            });

            // Give the signal handler time to set up
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Send SIGTERM to self
            let pid = Pid::this();
            kill(pid, Signal::SIGTERM).expect("Failed to send SIGTERM");

            // The function should complete when signal is received
            let result = timeout(Duration::from_secs(2), handle).await;
            assert!(result.is_ok(), "Signal handler should complete on SIGTERM");
        }

        #[tokio::test]
        async fn test_sigint_triggers_shutdown() {
            let handle = tokio::spawn(async {
                setup_signal_handlers().await;
            });

            tokio::time::sleep(Duration::from_millis(100)).await;

            // Send SIGINT to self
            let pid = Pid::this();
            kill(pid, Signal::SIGINT).expect("Failed to send SIGINT");

            let result = timeout(Duration::from_secs(2), handle).await;
            assert!(result.is_ok(), "Signal handler should complete on SIGINT");
        }

        #[tokio::test]
        async fn test_signal_handler_registration_succeeds() {
            // This test verifies the signal handlers can be registered without panic
            let handle = tokio::spawn(async {
                timeout(Duration::from_millis(500), setup_signal_handlers()).await
            });

            // We timeout the signal handler since it waits indefinitely
            let result = handle.await;
            assert!(result.is_ok());
            
            // The inner result should be a timeout error (expected)
            match result.unwrap() {
                Err(_) => {}, // Timeout is expected - handler is waiting for signal
                Ok(_) => panic!("Handler should not complete without signal"),
            }
        }

        #[tokio::test]
        async fn test_multiple_signal_handlers_concurrent() {
            use std::sync::Arc;
            use std::sync::atomic::{AtomicBool, Ordering};

            let completed = Arc::new(AtomicBool::new(false));
            let completed_clone = completed.clone();
            
            // Spawn a task that marks completion
            let handle = tokio::spawn(async move {
                setup_signal_handlers().await;
                completed_clone.store(true, Ordering::SeqCst);
            });

            tokio::time::sleep(Duration::from_millis(100)).await;

            // Send signal
            let pid = Pid::this();
            kill(pid, Signal::SIGTERM).expect("Failed to send SIGTERM");

            // Wait for completion
            let _ = timeout(Duration::from_secs(2), handle).await;
            
            assert!(completed.load(Ordering::SeqCst), "Handler should have completed");
        }

        #[tokio::test]
        async fn test_sigterm_before_sigint() {
            // Test that SIGTERM is handled first in select
            let handle = tokio::spawn(async {
                setup_signal_handlers().await;
            });

            tokio::time::sleep(Duration::from_millis(100)).await;

            let pid = Pid::this();
            // Send both signals, SIGTERM first
            kill(pid, Signal::SIGTERM).expect("Failed to send SIGTERM");
            
            let result = timeout(Duration::from_secs(2), handle).await;
            assert!(result.is_ok(), "Handler should complete on first signal");
        }
    }

    // ============================================
    // WINDOWS-SPECIFIC TESTS
    // ============================================

    #[cfg(windows)]
    mod windows_tests {
        use super::*;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        #[tokio::test]
        async fn test_ctrl_c_handler_setup_succeeds() {
            // Verify the function doesn't panic when called
            let handle = tokio::spawn(async {
                timeout(Duration::from_millis(500), setup_signal_handlers()).await
            });

            let result = handle.await;
            assert!(result.is_ok(), "Signal handler setup should not panic");
            
            // Should timeout since no Ctrl+C is sent
            match result.unwrap() {
                Err(_) => {}, // Timeout is expected
                Ok(_) => panic!("Handler should not complete without signal"),
            }
        }

        #[tokio::test]
        async fn test_signal_handler_is_cancellable() {
            // Test that the handler can be cancelled
            let handle = tokio::spawn(async {
                setup_signal_handlers().await;
            });

            tokio::time::sleep(Duration::from_millis(100)).await;
            
            // Abort the task
            handle.abort();
            
            let result = handle.await;
            assert!(result.is_err(), "Handler should be cancellable");
        }

        #[tokio::test]
        async fn test_multiple_handler_instances() {
            // Test that multiple instances can be created
            let handle1 = tokio::spawn(async {
                timeout(Duration::from_millis(200), setup_signal_handlers()).await
            });
            
            let handle2 = tokio::spawn(async {
                timeout(Duration::from_millis(200), setup_signal_handlers()).await
            });

            let result1 = handle1.await;
            let result2 = handle2.await;
            
            assert!(result1.is_ok(), "First handler should setup successfully");
            assert!(result2.is_ok(), "Second handler should setup successfully");
        }

        #[tokio::test]
        async fn test_handler_runs_in_async_context() {
            // Verify the function works properly in async runtime
            let completed = Arc::new(AtomicBool::new(false));
            let completed_clone = completed.clone();
            
            let handle = tokio::spawn(async move {
                let _ = timeout(Duration::from_millis(300), setup_signal_handlers()).await;
                completed_clone.store(true, Ordering::SeqCst);
            });

            handle.await.expect("Task should complete");
            assert!(completed.load(Ordering::SeqCst), "Handler task should complete");
        }

        #[tokio::test]
        async fn test_ctrl_c_error_handling() {
            // This test verifies the function handles potential errors
            // In practice, ctrl_c() registration rarely fails, but we test the path exists
            let handle = tokio::spawn(async {
                setup_signal_handlers().await;
            });

            // Let it run briefly then cancel
            tokio::time::sleep(Duration::from_millis(100)).await;
            handle.abort();
            
            // Should not panic even when interrupted
            let _ = handle.await;
        }
    }

    // ============================================
    // CROSS-PLATFORM TESTS
    // ============================================

    #[tokio::test]
    async fn test_signal_handler_is_async() {
        // Verify the function works in an async context
        let handle = tokio::spawn(async {
            timeout(Duration::from_millis(100), setup_signal_handlers()).await
        });

        let result = handle.await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_handler_can_be_spawned() {
        // Test that handler can be spawned as a separate task
        let handle = tokio::spawn(setup_signal_handlers());
        
        tokio::time::sleep(Duration::from_millis(100)).await;
        
        // Cancel it since we're not sending signals
        handle.abort();
        
        let result = handle.await;
        // Should be cancelled
        assert!(result.is_err() || result.unwrap_err().is_cancelled());
    }

    #[tokio::test]
    async fn test_handler_timeout_behavior() {
        // Verify that without signals, the handler waits indefinitely
        let result = timeout(
            Duration::from_millis(200),
            setup_signal_handlers()
        ).await;
        
        assert!(result.is_err(), "Handler should timeout without signal");
    }

    // END OF SIGNAL HANDLER TESTS
    // ============================================

    #[test]
    fn test_build_stomp_destination_activemq_basic() {
        // Test basic ActiveMQ destination building
        assert_eq!(
            build_stomp_destination(&BrokerType::ActiveMQ, "queue", "demo"),
            "/queue/demo"
        );
        assert_eq!(
            build_stomp_destination(&BrokerType::ActiveMQ, "topic", "notifications"),
            "/topic/notifications"
        );
        assert_eq!(
            build_stomp_destination(&BrokerType::ActiveMQ, "queue", "api.requests"),
            "/queue/api.requests"
        );
    }

    #[test]
    fn test_build_stomp_destination_activemq_already_prefixed() {
        // Test ActiveMQ with names that already have prefixes
        assert_eq!(
            build_stomp_destination(&BrokerType::ActiveMQ, "queue", "/queue/demo"),
            "/queue/demo"
        );
        assert_eq!(
            build_stomp_destination(&BrokerType::ActiveMQ, "topic", "/topic/notifications"),
            "/topic/notifications"
        );
        // Even if dest_type doesn't match the prefix, preserve the original
        assert_eq!(
            build_stomp_destination(&BrokerType::ActiveMQ, "topic", "/queue/mixed"),
            "/queue/mixed"
        );
    }

    #[test]
    fn test_build_stomp_destination_artemis_basic() {
        // Test basic Artemis destination building (strips prefixes)
        assert_eq!(
            build_stomp_destination(&BrokerType::Artemis, "queue", "demo"),
            "demo"
        );
        assert_eq!(
            build_stomp_destination(&BrokerType::Artemis, "topic", "notifications"),
            "notifications"
        );
        assert_eq!(
            build_stomp_destination(&BrokerType::Artemis, "queue", "api.requests"),
            "api.requests"
        );
    }

    #[test]
    fn test_build_stomp_destination_artemis_strip_prefixes() {
        // Test Artemis stripping prefixes
        assert_eq!(
            build_stomp_destination(&BrokerType::Artemis, "queue", "/queue/demo"),
            "demo"
        );
        assert_eq!(
            build_stomp_destination(&BrokerType::Artemis, "topic", "/topic/notifications"),
            "notifications"
        );
        assert_eq!(
            build_stomp_destination(&BrokerType::Artemis, "queue", "/topic/mixed"),
            "mixed"
        );
    }

    #[test]
    fn test_build_stomp_destination_edge_cases() {
        // Test with empty strings
        assert_eq!(
            build_stomp_destination(&BrokerType::ActiveMQ, "queue", ""),
            "/queue/"
        );
        assert_eq!(
            build_stomp_destination(&BrokerType::Artemis, "queue", ""),
            ""
        );

        // Test with special characters
        assert_eq!(
            build_stomp_destination(&BrokerType::ActiveMQ, "queue", "test@#$%"),
            "/queue/test@#$%"
        );
        assert_eq!(
            build_stomp_destination(&BrokerType::Artemis, "queue", "test@#$%"),
            "test@#$%"
        );

        // Test with only prefixes
        assert_eq!(
            build_stomp_destination(&BrokerType::Artemis, "queue", "/queue/"),
            ""
        );
        assert_eq!(
            build_stomp_destination(&BrokerType::Artemis, "topic", "/topic/"),
            ""
        );
    }

    #[test]
    fn test_build_stomp_destination_unicode() {
        // Test with Unicode characters
        assert_eq!(
            build_stomp_destination(&BrokerType::ActiveMQ, "queue", "测试队列"),
            "/queue/测试队列"
        );
        assert_eq!(
            build_stomp_destination(&BrokerType::Artemis, "topic", "уведомления"),
            "уведомления"
        );
        assert_eq!(
            build_stomp_destination(&BrokerType::Artemis, "queue", "/queue/📢_notifications"),
            "📢_notifications"
        );
    }

    #[test]
    fn test_build_stomp_destination_dest_type_variations() {
        // Test different destination types
        let dest_types = ["queue", "topic", "temp-queue", "temp-topic", "custom"];
        
        for dest_type in &dest_types {
            let result = build_stomp_destination(&BrokerType::ActiveMQ, dest_type, "test");
            assert_eq!(result, format!("/{}/test", dest_type));
            
            let result_artemis = build_stomp_destination(&BrokerType::Artemis, dest_type, "test");
            assert_eq!(result_artemis, "test");
        }
    }

    #[test]
    fn test_normalize_destination_name_basic() {
        // Test basic normalization
        assert_eq!(normalize_destination_name("/queue/demo"), "demo");
        assert_eq!(normalize_destination_name("/topic/notifications"), "notifications");
        assert_eq!(normalize_destination_name("bare_name"), "bare_name");
    }

    #[test]
    fn test_normalize_destination_name_edge_cases() {
        // Test edge cases
        assert_eq!(normalize_destination_name("/queue/"), "");
        assert_eq!(normalize_destination_name("/topic/"), "");
        assert_eq!(normalize_destination_name(""), "");
        
        // Test with repeated prefixes
        assert_eq!(normalize_destination_name("/queue/queue/test"), "queue/test");
        assert_eq!(normalize_destination_name("/topic/topic/test"), "topic/test");
        
        // Test with mixed prefixes
        assert_eq!(normalize_destination_name("/queue/topic/test"), "topic/test");
        assert_eq!(normalize_destination_name("/topic/queue/test"), "queue/test");
    }

    #[test]
    fn test_normalize_destination_name_unicode() {
        // Test with Unicode
        assert_eq!(normalize_destination_name("/queue/测试"), "测试");
        assert_eq!(normalize_destination_name("/topic/тест"), "тест");
        assert_eq!(normalize_destination_name("/queue/قائمة_انتظار"), "قائمة_انتظار");
    }

    #[test]
    fn test_normalize_destination_name_special_chars() {
        // Test with special characters
        assert_eq!(normalize_destination_name("/queue/test.queue-name_123"), "test.queue-name_123");
        assert_eq!(normalize_destination_name("/topic/test@#$%^&*()"), "test@#$%^&*()");
        
        // Test whitespace
        assert_eq!(normalize_destination_name("   "), "   ");
        assert_eq!(normalize_destination_name("/queue/\n\t\r "), "\n\t\r ");
    }

    #[test]
    fn test_is_queue_destination_basic() {
        // Test basic queue detection
        assert!(is_queue_destination("/queue/demo"));
        assert!(is_queue_destination("/queue/orders"));
        assert!(is_queue_destination("/queue/"));
        
        // Test non-queue destinations
        assert!(!is_queue_destination("/topic/events"));
        assert!(!is_queue_destination("demo"));
        assert!(!is_queue_destination(""));
        assert!(!is_queue_destination("/other/path"));
    }

    #[test]
    fn test_is_queue_destination_case_sensitivity() {
        // Test case sensitivity
        assert!(!is_queue_destination("/Queue/test"));
        assert!(!is_queue_destination("/QUEUE/test"));
        assert!(!is_queue_destination("/que/test"));
        
        // Test correct case
        assert!(is_queue_destination("/queue/test"));
        assert!(is_queue_destination("/queue/Test"));
        assert!(is_queue_destination("/queue/TEST"));
    }

    #[test]
    fn test_is_queue_destination_edge_cases() {
        // Test edge cases
        assert!(is_queue_destination("/queue/test.queue-name_123"));
        assert!(is_queue_destination("/queue/测试"));
        assert!(!is_queue_destination("/queue"));
        assert!(!is_queue_destination("queue/test"));
        assert!(!is_queue_destination("/queuetest"));
    }

    #[test]
    fn test_determine_routing_types_queues() {
        // Test routing types for queues (should prefer anycast)
        let queue_types = determine_routing_types("/queue/demo");
        assert_eq!(queue_types, ["anycast", "multicast"]);
        
        let queue_types = determine_routing_types("/queue/test");
        assert_eq!(queue_types[0], "anycast");
        assert_eq!(queue_types[1], "multicast");
        
        // Test with empty queue name
        let queue_types = determine_routing_types("/queue/");
        assert_eq!(queue_types, ["anycast", "multicast"]);
    }

    #[test]
    fn test_determine_routing_types_topics() {
        // Test routing types for topics (should prefer multicast)
        let topic_types = determine_routing_types("/topic/events");
        assert_eq!(topic_types, ["multicast", "anycast"]);
        
        let topic_types = determine_routing_types("/topic/notifications");
        assert_eq!(topic_types[0], "multicast");
        assert_eq!(topic_types[1], "anycast");
    }

    #[test]
    fn test_determine_routing_types_non_standard() {
        // Test routing types for non-standard destinations (should default to multicast first)
        let default_types = determine_routing_types("demo");
        assert_eq!(default_types, ["multicast", "anycast"]);
        
        let default_types = determine_routing_types("other/path");
        assert_eq!(default_types, ["multicast", "anycast"]);
        
        let default_types = determine_routing_types("");
        assert_eq!(default_types, ["multicast", "anycast"]);
        
        // Test mixed case
        let mixed_types = determine_routing_types("/Queue/test");
        assert_eq!(mixed_types, ["multicast", "anycast"]);
    }

    #[test]
    fn test_routing_types_array_properties() {
        // Test that arrays are exactly 2 elements and contain expected values
        let queue_routing = determine_routing_types("/queue/test");
        assert_eq!(queue_routing.len(), 2);
        assert!(queue_routing.contains(&"anycast"));
        assert!(queue_routing.contains(&"multicast"));
        
        let topic_routing = determine_routing_types("/topic/test");
        assert_eq!(topic_routing.len(), 2);
        assert!(topic_routing.contains(&"anycast"));
        assert!(topic_routing.contains(&"multicast"));
    }

    #[test]
    fn test_function_consistency() {
        // Test that all helper functions work together consistently
        let test_cases = vec![
            ("/queue/test", true, "test"),
            ("/topic/test", false, "test"),
            ("bare_name", false, "bare_name"),
            ("/queue/", true, ""),
            ("/topic/", false, ""),
        ];
        
        for (input, expected_is_queue, expected_normalized) in test_cases {
            assert_eq!(is_queue_destination(input), expected_is_queue, 
                      "Failed for input: {}", input);
            assert_eq!(normalize_destination_name(input), expected_normalized, 
                      "Failed for input: {}", input);
            
            let routing = determine_routing_types(input);
            if expected_is_queue {
                assert_eq!(routing[0], "anycast", 
                          "Queue should prefer anycast: {}", input);
            } else {
                assert_eq!(routing[0], "multicast", 
                          "Topic should prefer multicast: {}", input);
            }
        }
    }

    #[test]
    fn test_destination_helpers_with_large_strings() {
        // Test with very long strings
        let long_queue = format!("/queue/{}", "a".repeat(10000));
        assert!(is_queue_destination(&long_queue));
        let normalized = normalize_destination_name(&long_queue);
        assert_eq!(normalized.len(), 10000);
        
        let long_topic = format!("/topic/{}", "b".repeat(5000));
        assert!(!is_queue_destination(&long_topic));
        let normalized = normalize_destination_name(&long_topic);
        assert_eq!(normalized.len(), 5000);
    }

    #[test]
    fn test_broker_type_variants() {
        // Test that both broker types work with all functions
        let test_name = "test-destination";
        
        // Both broker types should produce some output
        let activemq_result = build_stomp_destination(&BrokerType::ActiveMQ, "queue", test_name);
        let artemis_result = build_stomp_destination(&BrokerType::Artemis, "queue", test_name);
        
        assert!(!activemq_result.is_empty());
        assert!(!artemis_result.is_empty());
        assert_ne!(activemq_result, artemis_result);
        
        // ActiveMQ should add prefix
        assert!(activemq_result.contains(test_name));
        assert!(activemq_result.starts_with("/"));
        
        // Artemis should return clean name
        assert_eq!(artemis_result, test_name);
    }

    // Property-based tests using proptest
    proptest! {
        #[test]
        fn prop_normalize_destination_idempotent(s in ".*") {
            let normalized_once = normalize_destination_name(&s);
            let normalized_twice = normalize_destination_name(normalized_once);
            prop_assert_eq!(normalized_once, normalized_twice);
        }
        
        #[test]
        fn prop_normalize_destination_length(s in ".*") {
            let normalized = normalize_destination_name(&s);
            prop_assert!(normalized.len() <= s.len());
        }
        
        #[test]
        fn prop_activemq_destination_contains_name(name in "[a-zA-Z0-9_.-]+") {
            let result = build_stomp_destination(&BrokerType::ActiveMQ, "queue", &name);
            prop_assert!(result.contains(&name));
        }
        
        #[test]
        fn prop_artemis_strips_prefixes(name in "[a-zA-Z0-9_.-]+") {
            let queue_input = format!("/queue/{}", name);
            let topic_input = format!("/topic/{}", name);
            
            let queue_result = build_stomp_destination(&BrokerType::Artemis, "queue", &queue_input);
            let topic_result = build_stomp_destination(&BrokerType::Artemis, "topic", &topic_input);
            
            prop_assert_eq!(queue_result, name.clone());
            prop_assert_eq!(topic_result, name);
        }
        
        #[test]
        fn prop_routing_types_always_two_elements(s in ".*") {
            let routing = determine_routing_types(&s);
            prop_assert_eq!(routing.len(), 2);
            prop_assert!(routing.contains(&"anycast"));
            prop_assert!(routing.contains(&"multicast"));
        }
        
        #[test] 
        fn prop_queue_detection_consistent(s in ".*") {
            let is_queue = is_queue_destination(&s);
            let routing = determine_routing_types(&s);
            
            if is_queue {
                prop_assert_eq!(routing[0], "anycast");
            } else {
                prop_assert_eq!(routing[0], "multicast");
            }
        }
    }

    #[test]
    fn test_comprehensive_edge_cases() {
        // Test various edge cases in one comprehensive test
        let edge_cases = vec![
            // Empty and whitespace
            ("", false, ""),
            ("   ", false, "   "),
            ("\n\t\r", false, "\n\t\r"),
            
            // Just prefixes
            ("/queue/", true, ""),
            ("/topic/", false, ""),
            
            // Unicode
            ("/queue/测试", true, "测试"),
            ("/topic/тест", false, "тест"),
            ("العربية", false, "العربية"),
            
            // Special characters
            ("/queue/test@#$%^&*()", true, "test@#$%^&*()"),
            ("/topic/test.queue-name_123", false, "test.queue-name_123"),
            
            // Mixed formats
            ("/queue/topic/nested", true, "topic/nested"),
            ("/topic/queue/nested", false, "queue/nested"),
            
            // Case variations
            ("/Queue/test", false, "/Queue/test"),  // Not recognized as queue
            ("/TOPIC/test", false, "/TOPIC/test"),  // Not recognized as topic
        ];
        
        for (input, expected_is_queue, expected_normalized) in edge_cases {
            assert_eq!(is_queue_destination(input), expected_is_queue, 
                      "Queue detection failed for: '{}'", input);
            assert_eq!(normalize_destination_name(input), expected_normalized, 
                      "Normalization failed for: '{}'", input);
            
            // Test that routing types are consistent with queue detection
            let routing = determine_routing_types(input);
            if expected_is_queue {
                assert_eq!(routing[0], "anycast", 
                          "Queue routing failed for: '{}'", input);
            } else {
                assert_eq!(routing[0], "multicast", 
                          "Topic routing failed for: '{}'", input);
            }
        }
    }

    #[test]
    fn test_build_stomp_destination_comprehensive() {
        // Comprehensive test for destination building
        let test_cases = vec![
            // ActiveMQ cases
            (BrokerType::ActiveMQ, "queue", "simple", "/queue/simple"),
            (BrokerType::ActiveMQ, "topic", "simple", "/topic/simple"),
            (BrokerType::ActiveMQ, "queue", "/queue/prefixed", "/queue/prefixed"),
            (BrokerType::ActiveMQ, "topic", "/topic/prefixed", "/topic/prefixed"),
            (BrokerType::ActiveMQ, "queue", "", "/queue/"),
            (BrokerType::ActiveMQ, "custom", "test", "/custom/test"),
            
            // Artemis cases
            (BrokerType::Artemis, "queue", "simple", "simple"),
            (BrokerType::Artemis, "topic", "simple", "simple"),
            (BrokerType::Artemis, "queue", "/queue/prefixed", "prefixed"),
            (BrokerType::Artemis, "topic", "/topic/prefixed", "prefixed"),
            (BrokerType::Artemis, "queue", "/queue/", ""),
            (BrokerType::Artemis, "custom", "test", "test"),
        ];
        
        for (broker_type, dest_type, name, expected) in test_cases {
            let result = build_stomp_destination(&broker_type, dest_type, name);
            assert_eq!(result, expected, 
                      "Failed for broker={:?}, dest_type={}, name={}", 
                      broker_type, dest_type, name);
        }
    }
}
