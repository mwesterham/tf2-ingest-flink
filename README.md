# Flink BackpackTF Forwarder

A high-performance Apache Flink application that processes Team Fortress 2 trading data from BackpackTF, featuring real-time message processing and comprehensive backfill capabilities.

## Features

- **Real-time Processing**: Consumes trading data from Kafka and processes listing updates
- **Backfill System**: Multiple specialized handlers for refreshing market data from BackpackTF API
- **API Integration**: BackpackTF and Steam Web API integration with rate limiting
- **Database Persistence**: PostgreSQL storage with upsert and delete operations
- **Monitoring**: Comprehensive metrics via Prometheus integration
- **Scalable**: Built on Apache Flink for distributed processing
- **Fault-tolerant**: Checkpointing every 60 seconds — restarts resume from last checkpoint instead of replaying from a fixed timestamp

## Quick Start

### Prerequisites

- Java 17+
- Apache Flink 1.20.2
- PostgreSQL database
- Kafka cluster

### Basic Setup

1. **Clone and build:**
   ```bash
   git clone <repository-url>
   cd flink-backpack-tf-forwarder
   mvn clean package
   ```

2. **Set environment variables:**
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

3. **Run the application:**
   ```bash
   flink run -d target/flink-backpack-tf-forwarder-1.0-SNAPSHOT-shaded.jar
   ```

### Enable Backfill (Optional)

Add these environment variables to enable backfill functionality:

```bash
export BACKFILL_KAFKA_TOPIC="backpack-tf-backfill-requests"
export BACKFILL_KAFKA_CONSUMER_GROUP="flink-backfill-consumer"
export BACKPACK_TF_API_TOKEN="your-backpack-tf-api-token"
export STEAM_API_KEY="your-steam-api-key"
```

## Backfill System

The application supports four types of backfill operations:

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

```bash
# Build and deploy
mvn clean package && \
docker build -t tf2-ingest-flink-job:1.0 . && \
docker tag tf2-ingest-flink-job:1.0 mwesterham/tf2-ingest-flink-job:latest && \
docker push mwesterham/tf2-ingest-flink-job:latest
```

## Checkpointing and Fault Tolerance

The job checkpoints every 60 seconds using the filesystem state backend. Checkpoint data is written to `state.checkpoints.dir` (configured in the k8s deployment as `/opt/flink/ha/checkpoints` on the HA PVC).

On restart after a failure, Flink restores from the last successful checkpoint, so at most 60 seconds of messages need to be replayed. Without a valid checkpoint (i.e., the very first cold start), the job falls back to consuming from `KAFKA_START_TIMESTAMP` or `KAFKA_START_TIMESTAMP_MINUTES` ago.

The k8s deployment uses `upgradeMode: last-state`, so operator-managed restarts (image updates, config changes) also restore from the last checkpoint rather than starting fresh.

## Monitoring

Access metrics at `http://localhost:9250/metrics`:

```bash
# Check processing status
curl http://localhost:9250/metrics | grep kafka_messages_consumed

# Monitor backfill operations
curl http://localhost:9250/metrics | grep backfill_requests

# Check consumer lag
curl http://localhost:9250/metrics | grep records_lag_max
```

## Documentation

Detailed documentation is available in the [`docs/`](docs/) directory:

- **[Setup and Configuration](docs/setup-configuration.md)** - Complete environment setup and configuration options
- **[Backfill System](docs/backfill-system.md)** - Comprehensive guide to backfill handlers and usage patterns
- **[API Integration](docs/api-integration.md)** - BackpackTF and Steam API configuration, authentication, and rate limiting
- **[Monitoring and Metrics](docs/monitoring-metrics.md)** - Available metrics, monitoring commands, and troubleshooting
- **[Development Guide](docs/development-guide.md)** - Local development setup, testing, and debugging

## Architecture

```
Kafka Topic → Flink Application → PostgreSQL Database
     ↓              ↓
Backfill Topic → Backfill Handlers → API Clients (BackpackTF + Steam)
```

The application processes two data streams:
1. **Main Stream**: Real-time listing updates from WebSocket-to-Kafka bridge
2. **Backfill Stream**: On-demand market data refresh requests

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `mvn test`
5. Submit a pull request

## License

[Add your license information here]