# Flink BackpackTF Forwarder

A high-performance Apache Flink application that processes Team Fortress 2 trading data from BackpackTF. Consists of two independent Flink jobs that run simultaneously — one for real-time listing updates and one for on-demand market data backfill.

## Jobs

| Job | Entry Class | Purpose |
|-----|-------------|---------|
| **WebSocketForwarderJob** | `me.matthew.flink.backpacktfforward.WebSocketForwarderJob` | Real-time listing updates from Kafka |
| **BackfillJob** | `me.matthew.flink.backpacktfforward.BackfillJob` | On-demand market data backfill |

Both jobs are packaged into the same JAR and run as separate `FlinkDeployment` resources in Kubernetes.

## Features

- **Real-time Processing**: Consumes trading data from Kafka and processes listing updates
- **Backfill System**: Multiple specialized handlers for refreshing market data from BackpackTF API
- **API Integration**: BackpackTF and Steam Web API integration with rate limiting
- **Database Persistence**: PostgreSQL storage with upsert and delete operations
- **Monitoring**: Comprehensive metrics via Prometheus integration
- **Fault-tolerant**: Checkpointing — restarts resume from last checkpoint instead of replaying from a fixed timestamp

## Quick Start

### Prerequisites

- Java 17+
- Apache Flink 1.20.2
- PostgreSQL database
- Kafka cluster

### Build

```bash
git clone <repository-url>
cd flink-backpack-tf-forwarder
mvn clean package
```

### WebSocketForwarderJob — Environment Variables

```bash
export KAFKA_BROKERS="localhost:9092"
export KAFKA_TOPIC="backpack-tf-relay-egress-queue-topic"
export KAFKA_CONSUMER_GROUP="flink-backpack-tf-consumer"
export DB_URL="jdbc:postgresql://localhost:5432/testdb"
export DB_USERNAME="testuser"
export DB_PASSWORD="testpass"
```

**Optional Kafka offset control** (only applies to the very first cold start — subsequent restarts resume from the last checkpoint):

| Variable | Description | Default |
|---|---|---|
| `KAFKA_START_TIMESTAMP` | Absolute epoch-ms to start from (takes priority) | — |
| `KAFKA_START_TIMESTAMP_MINUTES` | Start from N minutes ago on cold start | `30` |

```bash
flink run -d target/flink-backpack-tf-forwarder-1.0-SNAPSHOT-shaded.jar
```

### BackfillJob — Environment Variables

```bash
export BACKFILL_KAFKA_TOPIC="backpack-tf-backfill-requests-queue-topic"
export BACKFILL_KAFKA_CONSUMER_GROUP="flink-backfill-consumer"
export BACKPACK_TF_API_TOKEN="your-backpack-tf-api-token"
export STEAM_API_KEY="your-steam-api-key"
export DB_URL="jdbc:postgresql://localhost:5432/testdb"
export DB_USERNAME="testuser"
export DB_PASSWORD="testpass"
```

**Rate limit variables** (defaults scale correctly for parallelism=1):

| Variable | Default | Notes |
|---|---|---|
| `BACKPACK_TF_SNAPSHOT_RATE_LIMIT_SECONDS` | `10` | Multiply by parallelism if > 1 |
| `BACKPACK_TF_GET_LISTING_RATE_LIMIT_SECONDS` | `1` | Multiply by parallelism if > 1 |
| `STEAM_API_RATE_LIMIT_SECONDS` | `10` | Multiply by parallelism if > 1 |

```bash
flink run -d --class me.matthew.flink.backpacktfforward.BackfillJob \
  target/flink-backpack-tf-forwarder-1.0-SNAPSHOT-shaded.jar
```

## Backfill System

The backfill job processes requests from a dedicated Kafka topic one at a time (parallelism=1) so it never affects the main listing update stream. It supports four request types:

| Type | Purpose | API Usage | Speed |
|------|---------|-----------|-------|
| `FULL` | Complete refresh (buy + sell) | High | Slow |
| `BUY_ONLY` | Buy orders only | Low-Medium | Fast |
| `SELL_ONLY` | Sell orders only | Medium-High | Medium |
| `SINGLE_ID` | Individual listing | Minimal | Fastest |

### Example Backfill Request

```json
{
  "data": {
    "request_type": "FULL",
    "item_defindex": 190,
    "item_quality_id": 11
  },
  "timestamp": "2024-01-01T12:00:00.000Z",
  "messageId": "backfill-request-id"
}
```

## Docker Deployment

Both jobs share the same image:

```bash
mvn clean package && \
docker build -t tf2-ingest-flink-job:1.0 . && \
docker tag tf2-ingest-flink-job:1.0 mwesterham/tf2-ingest-flink-job:latest && \
docker push mwesterham/tf2-ingest-flink-job:latest
```

The Kubernetes deployments use `spec.job.entryClass` to select which job to run from the shared JAR.

## Checkpointing and Fault Tolerance

Both jobs checkpoint using the filesystem state backend. Checkpoint data is written to `state.checkpoints.dir` (configured in the k8s deployments as `/opt/flink/ha/checkpoints` on their respective HA PVCs).

On restart after a failure, Flink restores from the last successful checkpoint. Without a valid checkpoint (first cold start), the main job falls back to consuming from `KAFKA_START_TIMESTAMP` or `KAFKA_START_TIMESTAMP_MINUTES` ago. The backfill job resumes from its last committed Kafka offset.

Both k8s deployments use `upgradeMode: last-state`, so operator-managed restarts also restore from the last checkpoint.

## Monitoring

Both jobs expose Prometheus metrics on port 9249. In the k8s cluster, each job has its own `Service` and `ServiceMonitor` resources in the `tf2-auto-bot` namespace.

```bash
# Check processing status
curl http://localhost:9249/metrics | grep kafka_messages_consumed

# Monitor backfill operations
curl http://localhost:9249/metrics | grep backfill_requests

# Check consumer lag
curl http://localhost:9249/metrics | grep records_lag_max
```

## Architecture

```
[Kafka: listing updates] → WebSocketForwarderJob → PostgreSQL
[Kafka: backfill requests] → BackfillJob → BackpackTF API → Steam API → PostgreSQL
```

The two jobs run as completely independent Flink deployments with separate checkpoint stores. This guarantees that slow backfill API calls (which can take 30–120 seconds per item) never block checkpoint barriers in the real-time listing update stream.

## Documentation

Detailed documentation is available in the [`docs/`](docs/) directory:

- **[Setup and Configuration](docs/setup-configuration.md)** - Complete environment setup and configuration options
- **[Backfill System](docs/backfill-system.md)** - Comprehensive guide to backfill handlers and usage patterns
- **[API Integration](docs/api-integration.md)** - BackpackTF and Steam API configuration, authentication, and rate limiting
- **[Monitoring and Metrics](docs/monitoring-metrics.md)** - Available metrics, monitoring commands, and troubleshooting
- **[Development Guide](docs/development-guide.md)** - Local development setup, testing, and debugging

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `mvn test`
5. Submit a pull request
