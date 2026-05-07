# phone-simulator

Phone Simulator service for the AI Call Simulator Agent platform. Exposes a REST API the AI QA Agent calls to make voice calls and send SMS; drives the GSM CAP simulator via Kafka; externalises call-duration timers to the High-Performance Scheduler.

## Architecture

> Component + sequence diagrams (TeleQA ↔ phone-simulator ↔ Scheduler ↔ CAP simulator) live in [`docs/architecture.md`](docs/architecture.md).


```
[AI QA Agent] --REST--> [phone-simulator] --Kafka--> [CAP simulator]
                              |     (topic: call-event-queue, key: testId)
                              |
                              |     <----- Kafka cap.answer-events.v1 -----
                              |              (CAP publishes when ANSWER goes to SCP)
                              |
                              +--POST /v1/timers--> [scheduler]
                              <-- Kafka phonesim.timers.v1 --
                              --POST callback--> [QA webhook]
```

State for active calls is kept in Redis, keyed by **`testId`** — the caller-supplied identifier in every request. There is no server-generated callId. Spring Boot 3 / Java 21 with virtual threads.

### Voice call lifecycle (answer-driven)

1. `POST /api/v1/calls/voice/{mo,mt}` → `PENDING → DIALING`.
2. Phone-simulator publishes the `INITIAL` `CallRecord` to the call-event Kafka topic and arms a **no-answer guard timer** with the scheduler (default: 30 s, configurable via `phonesim.call.no-answer-timeout`). State settles on `RINGING`. **The duration timer is NOT yet running.**
3. CAP simulator publishes an `AnswerEvent` to `cap.answer-events.v1` (key = `testId`) once it has acknowledged the answer toward the SCP.
4. Phone-simulator's `AnswerEventConsumer` cancels the no-answer guard, transitions `RINGING → ANSWERED`, **then** enqueues the duration timer (`durationSeconds × 1000` ms).
5. Duration timer fires → phone-simulator publishes a `HangupEvent` (the simulated subscriber pressed "End call") on `phonesim.hangup-events.v1` and transitions to `RELEASED`. CAP receives the HangupEvent and emits the final `ApplyChargingReport` + `O_DISCONNECT` toward the SCP.
6. If the no-answer guard fires before the AnswerEvent arrives, the call is moved to `FAILED` with reason `no_answer_timeout`. No `HangupEvent` is published in this case (the call never reached `ANSWERED`).

> **Phone-simulator does not chunk.** It publishes exactly one `INITIAL` and (for answered calls) exactly one `HangupEvent`. ApplyChargingReport chunking, intermediate billing chunks, and `LAST_CHUNK` accounting toward the SCP are entirely owned by the CAP simulator.

### `AnswerEvent` wire contract (Kafka topic `cap.answer-events.v1`)

```json
{
  "testId": "voice-mo-...",
  "answerType": "O_ANSWER" | "T_ANSWER",
  "answeredAt": "2026-05-06T12:00:00Z",
  "schemaVersion": 1
}
```

Message key on Kafka must be the `testId` so partitioning lines up with the call-event topic. `answeredAt` and `answerType` are informational (logged + emitted as Micrometer tags); ordering is what matters.

### `HangupEvent` wire contract (Kafka topic `phonesim.hangup-events.v1`)

```json
{
  "testId": "voice-mo-...",
  "reason": "USER_HANGUP",
  "hungUpAt": "2026-05-06T12:00:15Z",
  "schemaVersion": 1
}
```

Message key on Kafka MUST be the `testId`. `reason=USER_HANGUP` covers the duration-timer-fired case; future reason codes can be added without breaking consumers.

## Quick start

Requires Docker.

```bash
docker compose up -d                 # redis, redpanda (Kafka), postgres
./mvnw spring-boot:run               # runs on :8081
```

The High-Performance Scheduler must be running separately at `http://localhost:8080` (or override `phonesim.scheduler.base-url`). It needs the `postgres` and `redpanda` containers brought up by this compose file.

OpenAPI docs: `http://localhost:8081/swagger-ui.html`. Prometheus metrics: `/actuator/prometheus`.

## REST API (`/api/v1`)

Every call/SMS request must include a unique `testId` (string, `[A-Za-z0-9_-]{1,64}`). The same `testId` is used as:

- the Redis key (`call:{testId}`)
- the Kafka message key on the call-event topic (so CAP-simulator partitions per test)
- the `partition_key` and embedded correlation id on the scheduler timer fire payload
- the path variable on `GET /api/v1/calls/{testId}`
- the `testId` field on every webhook event

Posting twice with the same `testId` returns `409 Conflict` until the previous record's TTL expires.

| Method | Path | Description |
|---|---|---|
| `POST` | `/calls/voice/mo` | Place voice MO call → `202` + testId |
| `POST` | `/calls/voice/mt` | Place voice MT call → `202` + testId |
| `POST` | `/sms/mo` | Send SMS MO → `202` + testId |
| `POST` | `/sms/mt` | Send SMS MT → `202` + testId |
| `GET` | `/calls/{testId}` | Snapshot of one call |
| `GET` | `/calls?state=ANSWERED` | List calls in a given state |
| `POST` | `/webhooks` | Register a fallback webhook URL |
| `DELETE` | `/webhooks/{id}` | Unregister fallback |

### Sample requests

Voice MO:

```bash
curl -X POST localhost:8081/api/v1/calls/voice/mo \
  -H 'Content-Type: application/json' \
  -d '{
    "testId":         "regression-roaming-mt-001",
    "callingParty":   "994501112233",
    "calledParty":    "994504445566",
    "imsi":           "400040000000001",
    "mscNumber":      "994700000001",
    "vlrAddress":     "994700000002",
    "lac": 1,
    "cellId": 1,
    "durationSeconds": 15,
    "callbackUrl":    "http://localhost:9000/hook"
  }'
```

SMS MO:

```bash
curl -X POST localhost:8081/api/v1/sms/mo \
  -H 'Content-Type: application/json' \
  -d '{
    "testId":         "sms-smoke-001",
    "callingParty":   "994501112233",
    "calledParty":    "994504445566",
    "imsi":           "400040000000001",
    "mscNumber":      "994700000001",
    "vlrAddress":     "994700000002",
    "lac": 1, "cellId": 1
  }'
```

Get call snapshot:

```bash
curl localhost:8081/api/v1/calls/regression-roaming-mt-001
```

### Webhook event schema

Posted to the per-request `callbackUrl` (or the registered fallback):

```json
{
  "eventId":   "<uuid>",
  "testId":    "regression-roaming-mt-001",
  "eventType": "CALL_RELEASED",
  "occurredAt":"2026-05-06T12:00:00Z",
  "state":     "RELEASED",
  "data":      { ...CallSnapshotResponse... }
}
```

Event types: `CALL_RELEASED`, `CALL_FAILED`, `SMS_DELIVERED`, `SMS_FAILED`.

## Configuration (`application.yml`)

Key properties — all overridable via env vars:

| Property | Env var | Default |
|---|---|---|
| `spring.data.redis.host` | `REDIS_HOST` | `localhost` |
| `spring.kafka.bootstrap-servers` | `KAFKA_BROKERS` | `localhost:9092` |
| `phonesim.scheduler.base-url` | `SCHEDULER_URL` | `http://localhost:8080` |
| `phonesim.kafka.timer-topic` | — | `phonesim.timers.v1` |
| `phonesim.kafka.call-event-topic` | `PHONESIM_CALL_EVENT_TOPIC` | `call-event-queue` |
| `phonesim.kafka.answer-event-topic` | `PHONESIM_ANSWER_EVENT_TOPIC` | `cap.answer-events.v1` |
| `phonesim.kafka.hangup-event-topic` | `PHONESIM_HANGUP_EVENT_TOPIC` | `phonesim.hangup-events.v1` |
| `phonesim.call.no-answer-timeout` | — | `30s` |
| `phonesim.defaults.voice-mo-service-key` | — | `201` |
| `phonesim.defaults.voice-mo-roaming-service-key` | — | `200` |
| `phonesim.defaults.voice-mt-service-key` | — | `101` |
| `phonesim.defaults.sms-service-key` | — | `205` |

### Postman collection

Import [`postman/phone-simulator.postman_collection.json`](postman/phone-simulator.postman_collection.json) into Postman or run it via Newman:

```bash
newman run postman/phone-simulator.postman_collection.json --env-var baseUrl=http://localhost:8081
```

The collection covers every endpoint (voice MO/MT, SMS MO/MT, snapshot, list, webhook register/unregister, health/metrics) plus error cases (duplicate `testId` → 409, missing `testId` → 400, unknown `testId` → 404). Each create request auto-saves the returned `testId` to a collection variable so the snapshot/list requests can reuse it.

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
│   ├── kafka/       CallEventPublisher (call-event-queue),
│   │                TimerFireConsumer (phonesim.timers.v1)
│   ├── redis/       CallRepository (keyed by testId), WebhookRepository
│   ├── scheduler/   SchedulerClient (REST), health indicator
│   └── webhook/     WebhookDispatcher (retrying outbound POSTs)
└── config/          @ConfigurationProperties + filters + OpenAPI
```

## What's NOT in v1

No auth, no in-call control (mute/hold/DTMF/hangup), no pre/post-call hooks (CONNECT, FurnishCharging, ApplyCharging), no multi-instance state coordination beyond Redis, no rate limiting. See the plan at `~/.claude/plans/we-are-developing-ai-glimmering-alpaca.md` for the Phase 2 list.
