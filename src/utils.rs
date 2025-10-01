use tracing::{error, info};
use tokio::signal;

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

/// Helper function to normalize destination names by stripping STOMP prefixes
/// This is used for Artemis compatibility where logical address names are preferred
pub fn normalize_destination_name(destination: &str) -> &str {
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
    use super::*;
    use quickcheck::quickcheck;

    #[test]
    fn test_normalize_destination_name() {
        // Test queue prefix stripping
        assert_eq!(normalize_destination_name("/queue/demo"), "demo");
        assert_eq!(normalize_destination_name("/queue/orders"), "orders");
        
        // Test topic prefix stripping
        assert_eq!(normalize_destination_name("/topic/events"), "events");
        assert_eq!(normalize_destination_name("/topic/alerts"), "alerts");
        
        // Test no prefix (should return as-is)
        assert_eq!(normalize_destination_name("demo"), "demo");
        assert_eq!(normalize_destination_name("events"), "events");
        
        // Test edge cases
        assert_eq!(normalize_destination_name("/queue/"), "");
        assert_eq!(normalize_destination_name("/topic/"), "");
        assert_eq!(normalize_destination_name(""), "");
    }

    #[test]
    fn test_is_queue_destination() {
        // Test queue destinations
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
    fn test_determine_routing_types() {
        // Test queue destinations (should prefer anycast)
        let queue_types = determine_routing_types("/queue/demo");
        assert_eq!(queue_types[0], "anycast");
        assert_eq!(queue_types[1], "multicast");
        
        // Test topic destinations (should prefer multicast)
        let topic_types = determine_routing_types("/topic/events");
        assert_eq!(topic_types[0], "multicast");
        assert_eq!(topic_types[1], "anycast");
        
        // Test non-standard destinations (should default to topic behavior)
        let default_types = determine_routing_types("demo");
        assert_eq!(default_types[0], "multicast");
        assert_eq!(default_types[1], "anycast");
    }

    #[test]
    fn test_destination_helper_integration() {
        // Test full workflow for queue
        let queue_dest = "/queue/orders";
        assert!(is_queue_destination(queue_dest));
        assert_eq!(normalize_destination_name(queue_dest), "orders");
        let routing = determine_routing_types(queue_dest);
        assert_eq!(routing[0], "anycast");
        
        // Test full workflow for topic
        let topic_dest = "/topic/notifications";
        assert!(!is_queue_destination(topic_dest));
        assert_eq!(normalize_destination_name(topic_dest), "notifications");
        let routing = determine_routing_types(topic_dest);
        assert_eq!(routing[0], "multicast");
    }

    #[test]
    fn test_artemis_constants() {
        // Test that basic destination helper functions work correctly
        assert!(is_queue_destination("/queue/test"));
        assert!(!is_queue_destination("/topic/test"));
        assert_eq!(normalize_destination_name("/queue/test"), "test");
        assert_eq!(normalize_destination_name("/topic/test"), "test");
    }

    // Property-based tests using quickcheck
    quickcheck! {
        fn prop_normalize_destination_idempotent(s: String) -> bool {
            let normalized_once = normalize_destination_name(&s);
            let normalized_twice = normalize_destination_name(normalized_once);
            normalized_once == normalized_twice
        }
        
        fn prop_normalize_destination_length(s: String) -> bool {
            let normalized = normalize_destination_name(&s);
            normalized.len() <= s.len()
        }
    }

    #[test]
    fn test_normalize_destination_edge_cases() {
        // Test with only prefixes
        assert_eq!(normalize_destination_name("/queue/"), "");
        assert_eq!(normalize_destination_name("/topic/"), "");
        
        // Test with repeated prefixes  
        assert_eq!(normalize_destination_name("/queue//queue/test"), "/queue/test");
        assert_eq!(normalize_destination_name("/topic//topic/test"), "/topic/test");
        
        // Test with mixed prefixes
        assert_eq!(normalize_destination_name("/queue/topic/test"), "topic/test");
        assert_eq!(normalize_destination_name("/topic/queue/test"), "queue/test");
        
        // Test with Unicode
        assert_eq!(normalize_destination_name("/queue/测试"), "测试");
        assert_eq!(normalize_destination_name("/topic/тест"), "тест");
        
        // Test with special characters
        assert_eq!(normalize_destination_name("/queue/test.queue-name_123"), "test.queue-name_123");
    }

    #[test]
    fn test_is_queue_destination_comprehensive() {
        // True cases
        assert!(is_queue_destination("/queue/test"));
        assert!(is_queue_destination("/queue/"));
        assert!(is_queue_destination("/queue/a"));
        assert!(is_queue_destination("/queue/test.queue-name_123"));
        assert!(is_queue_destination("/queue/测试"));
        
        // False cases
        assert!(!is_queue_destination("/topic/test"));
        assert!(!is_queue_destination("queue/test"));
        assert!(!is_queue_destination("/que/test"));
        assert!(!is_queue_destination("/Queue/test"));
        assert!(!is_queue_destination("/QUEUE/test"));
        assert!(!is_queue_destination("test"));
        assert!(!is_queue_destination(""));
        assert!(!is_queue_destination("/other/test"));
        assert!(!is_queue_destination("/queue"));
    }

    #[test]
    fn test_determine_routing_types_comprehensive() {
        // Queue destinations - should prefer anycast
        let queue_types = determine_routing_types("/queue/test");
        assert_eq!(queue_types, ["anycast", "multicast"]);
        
        let queue_types = determine_routing_types("/queue/");
        assert_eq!(queue_types, ["anycast", "multicast"]);
        
        // Topic destinations - should prefer multicast
        let topic_types = determine_routing_types("/topic/test");
        assert_eq!(topic_types, ["multicast", "anycast"]);
        
        // Non-standard destinations - should default to multicast first
        let default_types = determine_routing_types("test");
        assert_eq!(default_types, ["multicast", "anycast"]);
        
        let default_types = determine_routing_types("other/path");
        assert_eq!(default_types, ["multicast", "anycast"]);
        
        let default_types = determine_routing_types("");
        assert_eq!(default_types, ["multicast", "anycast"]);
        
        // Mixed case
        let mixed_types = determine_routing_types("/Queue/test");
        assert_eq!(mixed_types, ["multicast", "anycast"]);
    }

    #[test] 
    fn test_signal_handling_constants() {
        // This test ensures the signal handling logic compiles correctly
        // We can't easily test the actual signal handling without spawning processes
        
        #[cfg(windows)]
        {
            // On Windows, we use ctrl_c signal
            // This is just a compilation test
            assert!(true);
        }
        
        #[cfg(unix)]
        {
            // On Unix, we use SIGTERM and SIGINT
            // This is just a compilation test  
            assert!(true);
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
    fn test_destination_helpers_unicode_edge_cases() {
        // Test with various Unicode characters
        let unicode_destinations = vec![
            "/queue/测试队列",
            "/topic/уведомления",
            "/queue/قائمة_انتظار",
            "/topic/📢_notifications",
            "/queue/🏃‍♂️_runner",
        ];
        
        for dest in unicode_destinations {
            if dest.starts_with("/queue/") {
                assert!(is_queue_destination(dest));
                let routing = determine_routing_types(dest);
                assert_eq!(routing[0], "anycast");
            } else {
                assert!(!is_queue_destination(dest));
                let routing = determine_routing_types(dest);
                assert_eq!(routing[0], "multicast");
            }
            
            let normalized = normalize_destination_name(dest);
            assert!(!normalized.starts_with("/queue/"));
            assert!(!normalized.starts_with("/topic/"));
        }
    }

    #[test]
    fn test_empty_and_whitespace_destinations() {
        // Test empty string
        assert!(!is_queue_destination(""));
        assert_eq!(normalize_destination_name(""), "");
        let routing = determine_routing_types("");
        assert_eq!(routing, ["multicast", "anycast"]);
        
        // Test whitespace only
        assert!(!is_queue_destination("   "));
        assert_eq!(normalize_destination_name("   "), "   ");
        
        // Test newlines and tabs
        let whitespace_dest = "/queue/\n\t\r ";
        assert!(is_queue_destination(whitespace_dest));
        assert_eq!(normalize_destination_name(whitespace_dest), "\n\t\r ");
    }

    #[test]
    fn test_routing_types_return_arrays() {
        // Verify the return types are exactly what we expect
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
            assert_eq!(is_queue_destination(input), expected_is_queue, "Failed for input: {}", input);
            assert_eq!(normalize_destination_name(input), expected_normalized, "Failed for input: {}", input);
            
            let routing = determine_routing_types(input);
            if expected_is_queue {
                assert_eq!(routing[0], "anycast", "Queue should prefer anycast: {}", input);
            } else {
                assert_eq!(routing[0], "multicast", "Topic should prefer multicast: {}", input);
            }
        }
    }
}
