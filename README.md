# RelayOps — Distributed Webhook Delivery Engine

Webhook delivery looks simple until endpoints become slow, return errors, or go offline. RelayOps is a Go-based starter platform for accepting webhook events, delivering them asynchronously, tracking delivery attempts, and replaying failed events.

The project is designed to explore the building blocks behind reliable event delivery: a REST API, concurrent workers, retry handling, persistent delivery records, and operational metrics.

## What it does

- Accepts webhook events through a REST endpoint.
- Queues deliveries for asynchronous processing.
- Sends HTTP requests to configured destination URLs.
- Tracks delivery status and attempt counts.
- Retries unsuccessful deliveries with a configurable limit and backoff.
- Allows failed deliveries to be replayed.
- Exposes health and Prometheus-style metrics endpoints.

## Technologies

| Technology | Role |
|---|---|
| Go | API server and concurrent delivery workers |
| Kafka | Event-streaming foundation for decoupled ingestion and processing |
| PostgreSQL | Persistent event and delivery records |
| Redis | Optional coordination and retry-state component |
| Docker Compose | Local infrastructure |
| Prometheus | Metrics collection |

> **Project status:** This repository is a portfolio starter. The current implementation includes a runnable local delivery API and worker with in-memory storage, so the core flow can be explored without external services. The Docker Compose file provides the supporting infrastructure for extending the starter toward persistent, broker-backed operation. Do not treat this demo as production-ready.

## Architecture

```text
Client
  |
  v
Go REST API ----> Event / Delivery Store
  |                       |
  v                       v
Delivery Queue        Status Tracking
  |
  v
Concurrent Workers
  |
  v
Destination Webhook
  |
  +---- success ----> delivered
  |
  +---- failure ----> retry with backoff
                          |
                          v
                    failed / replayable
```

## Run locally

### Requirements

- Go 1.22+
- Docker Desktop (optional, for the supporting services)

### Start the API

From the repository root:

```bash
cd backend
go run .
```

The API starts on `http://localhost:8080`.

### Try it

Create a webhook delivery:

```bash
curl -X POST http://localhost:8080/webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "destination_url": "https://httpbin.org/post",
    "event_type": "order.created",
    "payload": {
      "order_id": "ord_123",
      "status": "created"
    }
  }'
```

Check delivery records:

```bash
curl http://localhost:8080/deliveries
```

Check service health:

```bash
curl http://localhost:8080/health
```

Check metrics:

```bash
curl http://localhost:8080/metrics
```

Replay a failed delivery by replacing `DELIVERY_ID` with an ID from the delivery list:

```bash
curl -X POST http://localhost:8080/deliveries/DELIVERY_ID/replay
```

## API endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| `GET` | `/health` | Service health |
| `POST` | `/webhooks` | Enqueue a webhook delivery |
| `GET` | `/deliveries` | List known delivery records |
| `GET` | `/deliveries/{id}` | Inspect a delivery |
| `POST` | `/deliveries/{id}/replay` | Requeue a delivery |
| `GET` | `/metrics` | Basic Prometheus-compatible metrics |

## Configuration

The demo uses environment variables with sensible defaults:

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | HTTP server port |
| `WORKER_COUNT` | `4` | Number of concurrent delivery workers |
| `MAX_ATTEMPTS` | `3` | Maximum delivery attempts |
| `RETRY_BASE_DELAY_MS` | `500` | Base retry delay in milliseconds |
| `REQUEST_TIMEOUT_MS` | `5000` | Outbound HTTP request timeout |

## Local infrastructure

To start the supporting services:

```bash
docker compose up -d
```

This launches PostgreSQL, Redis, Kafka, and Prometheus. The current demo API does not yet persist to or publish through these services; they are included as the local infrastructure foundation for the next implementation stage.

## Roadmap

- [ ] Publish delivery jobs to Kafka and consume them with independent workers.
- [ ] Persist delivery attempts and event state in PostgreSQL.
- [ ] Use Redis for retry scheduling, deduplication, or distributed coordination.
- [ ] Add signed webhook payloads and configurable headers.
- [ ] Add integration tests and fault-injection scenarios.
- [ ] Add dashboards and alerts for latency, failure rate, and retry volume.

## Security and reliability notes

- Only send test payloads to destinations you control.
- Production deployments should validate destination URLs and defend against SSRF.
- Add authentication, authorization, rate limiting, signature verification, and secret management before exposing the service publicly.
- Use bounded queues, idempotency keys, and durable broker acknowledgements before relying on the system for critical events.

## Author

**Gunjan Gaur** · [GitHub](https://github.com/GunjanGaur1)

[Repository](https://github.com/GunjanGaur1/RelayOps-Distributed-Webhook-Delivery-Engine)
