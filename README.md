# Wire-Speed Events

A high-performance, real-time event streaming pipeline built with Go, Apache Kafka, and Protocol Buffers (Protobuf). This project demonstrates how to handle high-frequency telemetry data with minimal overhead and type-safe serialization.

---

## Overview

wire-speed-events simulates a high-speed data stream where events are:
1. **Produced**: Generated in the application and serialized using Protobuf.
2. **Published**: Sent to a Kafka topic.
3. **Consumed**: Read from Kafka, deserialized, and processed in real-time.
4. **Resilient**: Uses a modular architecture with a clean separation between domain logic and transport layers.

---

## Tech Stack

- **Language**: [Go 1.23+](https://go.dev/)
- **Messaging**: [Apache Kafka](https://kafka.apache.org/) (via `segmentio/kafka-go`)
- **Serialization**: [Protocol Buffers (v3)](https://protobuf.dev/)
- **Infrastructure**: Docker & Docker Compose

---

## Protobuf Setup

To generate Go code from `.proto` files, you need to install the Protocol Buffer compiler (`protoc`) and the Go plugins.

### 1. Install protoc
On macOS (using Homebrew):
```bash
brew install protobuf
```

For other platforms, download the pre-compiled binaries from the [official releases page](https://github.com/protocolbuffers/protobuf/releases).

### 2. Install Go Plugins
Install the plugins for Go code generation and gRPC:
```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

### 3. Update PATH
Ensure your Go bin directory is in your `PATH` so `protoc` can find the plugins:
```bash
export PATH="$PATH:$(go env GOPATH)/bin"
```

---

## Project Structure

```text
├── cmd/
│   └── app/main.go          # Application entry point & simulation logic
├── internal/
│   ├── domain/              # Interfaces and business rules
│   ├── models/              # Domain data structures
│   ├── service/             # Orchestrator & core business logic
│   └── transport/
│       └── kafka/           # Kafka producer, consumer, and config
├── pb/
│   └── event.proto          # Protobuf definition
├── gen/
│   └── pb/                  # Generated Go code from Protobuf
├── Makefile                 # Automation for code generation
└── docker-compose.yaml      # Kafka & Zookeeper environment
```

---

## Getting Started

### 1. Start Infrastructure
Spin up Kafka and Zookeeper:
```bash
docker-compose up -d
```

### 2. Generate Code
Generate the Go files from the Protobuf definition:
```bash
make proto
```

### 3. Run the Simulation
```bash
go run cmd/app/main.go
```

---

## How it Works

### Serializing with Protobuf
Instead of bulky JSON, events are encoded into a compact binary format. 
Example from `internal/transport/kafka/producer.go`:
```go
protoEvent := &events.Event{
    Id:        event.ID,
    Type:      event.Type,
    Payload:   event.Payload,
    Timestamp: event.Timestamp.UnixNano(),
}
payload, _ := proto.Marshal(protoEvent)
```

### High-Speed Consumption
The consumer is optimized for low latency, reading binary segments and unmarshaling them directly back into domain models for processing.

---

## License
MIT License. Feel free to use and modify for your own high-speed pipelines.
