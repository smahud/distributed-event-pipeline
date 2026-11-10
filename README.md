# distributed-event-pipeline

Fault-tolerant streaming ingestion engine handling 50k+ events/sec using Go, Kafka, and ClickHouse.

## Architecture

```
┌──────────┐     ┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│ Producers│────▶│   Kafka     │────▶│  Consumers   │────▶│  ClickHouse │
│ (HTTP/   │     │   (Topics:  │     │  (Go Workers)│     │  (OLAP)     │
│  gRPC)   │     │   raw,      │     │  - Parse     │     │             │
│          │     │   enriched, │     │  - Validate  │     │  - Aggregations
└──────────┘     │   dead-letter)│     │  - Enrich    │     │  - Dashboards
                 └─────────────┘     │  - Dedupe    │     └─────────────┘
                        │            └──────────────┘            │
                        ▼                   │                   ▼
               ┌─────────────┐            ▼             ┌─────────────┐
               │ Dead Letter │     ┌─────────────┐      │  Grafana    │
               │   Queue     │     │   Redis     │      │  Dashboards │
               │  (Retry/DLQ)│     │  (Cache/    │      └─────────────┘
               └─────────────┘     │   State)    │
                                   └─────────────┘
```

## Features

- **Exactly-once Semantics**: Transactional outbox pattern + idempotent consumers
- **Backpressure Handling**: Consumer group lag monitoring, auto-scaling triggers
- **Schema Evolution**: Avro/Protobuf with Schema Registry compatibility checks
- **Multi-tenancy**: Topic-per-tenant isolation with quotas
- **Observability**: OpenTelemetry traces, Prometheus metrics, structured logs
- **Failure Recovery**: Automatic retry with exponential backoff, DLQ for poison pills

## Performance Targets

| Metric | Target |
|--------|--------|
| Throughput | 50,000+ events/sec |
| Latency (p99) | < 500ms end-to-end |
| Availability | 99.99% |
| Data Loss | Zero (with exactly-once) |

## Quick Start

```bash
# Build
make build

# Run locally with docker-compose
docker-compose up -d

# Produce test events
make produce-events COUNT=10000

# Query ClickHouse
clickhouse-client --query "SELECT count() FROM events.raw"
```

## Configuration

```yaml
kafka:
  brokers: ["kafka:9092"]
  topics:
    raw: "events.raw"
    enriched: "events.enriched"
    dlq: "events.dlq"
  consumer_group: "pipeline-workers"

clickhouse:
  dsn: "clickhouse://default:@clickhouse:9000/events"
  batch_size: 10000
  flush_interval: "5s"

worker:
  concurrency: 32
  batch_timeout: "100ms"
  max_retries: 3
```

## Topics

`go` `kafka` `distributed-systems` `clickhouse` `concurrency` `streaming` `event-driven` `observability`