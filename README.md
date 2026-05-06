# phone-simulator

Phone Simulator service for the AI Call Simulator Agent platform. Exposes a REST API the AI QA Agent calls to make voice calls and send SMS; drives the GSM CAP simulator via RabbitMQ; externalises call-duration timers to the High-Performance Scheduler.

## Architecture

```
[AI QA Agent] --REST--> [phone-simulator] --AMQP--> [CAP simulator]
                              |     (queue: call-event-queue)
                              +--POST /v1/timers--> [scheduler]
                              <-- Kafka phonesim.timers.v1 --
                              --POST callback--> [QA webhook]
```

State for active calls is kept in Redis. Spring Boot 3 / Java 21 with virtual threads.

## Quick start

Requires Docker.

```bash
docker compose up -d                 # rabbitmq, redis, redpanda, postgres
./mvnw spring-boot:run               # runs on :8081
```

The High-Performance Scheduler must be running separately at `http://localhost:8080` (or override `phonesim.scheduler.base-url`). It needs the `postgres` and `redpanda` containers brought up by this compose file.

OpenAPI docs: `http://localhost:8081/swagger-ui.html`. Prometheus metrics: `/actuator/prometheus`.

## REST API (`/api/v1`)

| Method | Path | Description |
|---|---|---|
| `POST` | `/calls/voice/mo` | Place voice MO call → `202` + callId |
| `POST` | `/calls/voice/mt` | Place voice MT call → `202` + callId |
| `POST` | `/sms/mo` | Send SMS MO → `202` + callId |
| `POST` | `/sms/mt` | Send SMS MT → `202` + callId |
| `GET` | `/calls/{callId}` | Snapshot of one call |
| `GET` | `/calls?state=ANSWERED` | List calls in a given state |
| `POST` | `/webhooks` | Register a fallback webhook URL |
| `DELETE` | `/webhooks/{id}` | Unregister fallback |

### Sample requests

Voice MO:

```bash
curl -X POST localhost:8081/api/v1/calls/voice/mo \
  -H 'Content-Type: application/json' \
  -d '{
    "callingParty": "994501112233",
    "calledParty":  "994504445566",
    "imsi":         "400040000000001",
    "mscNumber":    "994700000001",
    "vlrAddress":   "994700000002",
    "lac": 1,
    "cellId": 1,
    "durationSeconds": 15,
    "callbackUrl": "http://localhost:9000/hook"
  }'
```

SMS MO:

```bash
curl -X POST localhost:8081/api/v1/sms/mo \
  -H 'Content-Type: application/json' \
  -d '{
    "callingParty": "994501112233",
    "calledParty":  "994504445566",
    "imsi":         "400040000000001",
    "mscNumber":    "994700000001",
    "vlrAddress":   "994700000002",
    "lac": 1, "cellId": 1
  }'
```

Get call snapshot:

```bash
curl localhost:8081/api/v1/calls/<callId>
```

### Webhook event schema

Posted to the per-request `callbackUrl` (or the registered fallback):

```json
{
  "eventId": "<uuid>",
  "callId":  "<uuid>",
  "eventType": "CALL_RELEASED",
  "occurredAt": "2026-05-06T12:00:00Z",
  "state": "RELEASED",
  "data": { ...CallSnapshotResponse... }
}
```

Event types: `CALL_RELEASED`, `CALL_FAILED`, `SMS_DELIVERED`, `SMS_FAILED`.

## Configuration (`application.yml`)

Key properties — all overridable via env vars:

| Property | Env var | Default |
|---|---|---|
| `spring.rabbitmq.host` | `RABBIT_HOST` | `localhost` |
| `spring.rabbitmq.virtual-host` | `RABBIT_VHOST` | `call-simulator-vhost` |
| `spring.rabbitmq.username` | `RABBIT_USER` | `simulatormq` |
| `spring.rabbitmq.password` | `RABBIT_PASSWORD` | `changeit` |
| `spring.data.redis.host` | `REDIS_HOST` | `localhost` |
| `spring.kafka.bootstrap-servers` | `KAFKA_BROKERS` | `localhost:9092` |
| `phonesim.scheduler.base-url` | `SCHEDULER_URL` | `http://localhost:8080` |
| `phonesim.kafka.timer-topic` | — | `phonesim.timers.v1` |
| `phonesim.amqp.queue` | — | `call-event-queue` |
| `phonesim.defaults.voice-mo-service-key` | — | `201` |
| `phonesim.defaults.voice-mo-roaming-service-key` | — | `200` |
| `phonesim.defaults.voice-mt-service-key` | — | `101` |
| `phonesim.defaults.sms-service-key` | — | `205` |

## Building & testing

```bash
./mvnw test            # unit tests (no Docker required)
./mvnw verify          # unit + Testcontainers integration tests (Docker required)
./mvnw spring-boot:run # run locally
./mvnw package         # builds executable jar at target/phone-simulator-*.jar
```

## Project layout

```
src/main/java/com/azerconnect/phonesim/
├── api/             REST controllers + DTOs
├── domain/          Call, CallStatus, CallStateMachine
├── service/         CallService, CallRecordMapper
├── adapter/
│   ├── amqp/        Publisher of CallRecord JSON to RabbitMQ
│   ├── kafka/       FireEvent consumer (timer fires)
│   ├── redis/       CallRepository, WebhookRepository
│   ├── scheduler/   SchedulerClient (REST), health indicator
│   └── webhook/     WebhookDispatcher (retrying outbound POSTs)
└── config/          @ConfigurationProperties + filters + OpenAPI
```

## What's NOT in v1

No auth, no in-call control (mute/hold/DTMF/hangup), no pre/post-call hooks (CONNECT, FurnishCharging, ApplyCharging), no multi-instance state coordination beyond Redis, no rate limiting. See the plan at `~/.claude/plans/we-are-developing-ai-glimmering-alpaca.md` for the Phase 2 list.
