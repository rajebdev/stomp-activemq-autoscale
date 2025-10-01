# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2025-01-01

### Added
- 🎯 **Auto-scaling engine** with dynamic worker management based on queue metrics
- 🔄 **Multi-broker support** for Apache ActiveMQ Classic and Apache Artemis
- 📤 **High-performance producer** with connection reuse and JMS headers
- 📥 **Flexible message listeners** with queue and topic support
- 🔄 **Auto-reconnection** with exponential backoff retry logic
- 🏗️ **Graceful shutdown** with proper cleanup and signal handling
- ⚡ **Async/await native** implementation built on Tokio
- 🛡️ **Type-safe configuration** with YAML support and validation
- 📊 **Production monitoring** with structured logging and metrics
- 🧪 **Comprehensive testing** with unit, integration, and E2E tests

### Documentation
- 📖 **Complete GitHub wiki** with 11 comprehensive documentation pages
- 🏠 **Home page** with overview and quick start guide
- 📦 **Installation guide** with setup instructions for multiple platforms  
- ⚙️ **Configuration reference** with all available options documented
- 📚 **API documentation** with examples and usage patterns
- 💡 **Examples and usage** with real-world production scenarios
- ✨ **Best practices** guide with Rust idioms and performance tips
- 🔗 **Multi-broker support** comparison between ActiveMQ and Artemis
- 🧪 **Testing guide** with TestContainers and CI/CD setup
- 🔧 **Troubleshooting** guide with common issues and solutions
- 🤝 **Contributing guide** with development workflow and standards

### Examples
- 📝 **Basic STOMP examples** with producer and consumer implementations
- 🏭 **Production-ready examples** with error handling and graceful shutdown
- 🔄 **Auto-scaling examples** demonstrating dynamic worker management
- 💳 **Payment processing example** with real-world business logic
- 🐳 **Docker setup examples** with docker-compose configurations

### Infrastructure
- 🔧 **Cargo workspace** configuration for examples
- 📋 **Professional README** with badges, navigation, and clear documentation
- 🏷️ **Semantic versioning** and release management
- 🧪 **Testing infrastructure** with multiple test categories
- 📊 **Code organization** with modular architecture

### Features by Component

#### Core Library
- `StompProducer` - High-performance message producer with connection pooling
- `StompListener` - Flexible message consumer with auto-scaling capabilities
- `StompListenerHandle` - Graceful shutdown control for running services
- `Config` - Type-safe YAML configuration with validation
- Auto-scaling engine with configurable worker ranges (e.g., "1-4", "2")
- Automatic broker metrics collection via management APIs
- Exponential backoff retry logic with configurable parameters
- JMS headers support for message metadata and routing

#### Broker Support
- **Apache ActiveMQ Classic** 5.15+ with full feature support
- **Apache Artemis** 2.17+ with clustering capabilities
- Automatic broker type detection and API adaptation
- Management console integration for queue metrics
- STOMP 1.2 protocol support with heartbeat management

#### Production Features
- Structured logging with `tracing` crate integration
- Configuration via environment variables and YAML files
- Graceful shutdown with configurable timeout periods
- Connection pooling and reuse for optimal performance
- Dead Letter Queue (DLQ) patterns for error handling
- Health checks and monitoring endpoints ready

[Unreleased]: https://github.com/username/stomp-activemq-autoscale/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/username/stomp-activemq-autoscale/releases/tag/v0.1.0