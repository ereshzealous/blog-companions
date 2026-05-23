# OTel Agent + Gateway on K3d — "WearableHealth" Demo Platform

**Date:** 2026-05-19
**Status:** Approved, in implementation
**Goal:** A polyglot microservices demo on local K3d where OpenTelemetry traces, metrics, and logs flow end-to-end across REST, gRPC, Kafka, Postgres, WebSocket, and async batch — using the OTel **Agent (DaemonSet) + Gateway (Deployment)** pattern, with one **sidecar** example for contrast.

---

## 1. Domain

WearableHealth is a continuous health telemetry platform inspired by Fitbit/Apple Health/Whoop. Simulated wearables push vital signs; the platform derives signals, detects anomalies, pushes alerts to user devices, and produces daily insights.

This domain was chosen because each protocol on the requirements list has a *natural* reason to exist — none feel bolted on.

## 2. Services

| # | Service | Language | Protocol(s) | Responsibility |
|---|---|---|---|---|
| 1 | `device-gateway` | Go | gRPC ingress, Kafka producer | Authenticate devices, validate payloads, publish raw telemetry |
| 2 | `stream-processor` | Python (asyncio) | Kafka in/out, Postgres | Rolling aggregates (HR avg, HRV, motion class); writes 1-minute rollups |
| 3 | `anomaly-detector` | Python | Kafka in/out, Postgres | ML: AFib / fall / abnormal-HR detection — **uses sidecar collector** |
| 4 | `health-api` | Java (Spring Boot) | REST, JDBC/Postgres | User-facing queries: vitals, alerts, workouts, insights |
| 5 | `live-gateway` | Node.js | WebSocket + REST, Kafka in | Real-time alert push to mobile clients |
| 6 | `insight-worker` | Python | CronJob, Postgres | Hourly/nightly batch: sleep score, recovery score, daily summary |

## 3. Access patterns (the traced flows)

### Flow A — Telemetry ingestion
`wearable --gRPC--> device-gateway --Kafka(vitals.raw)--> stream-processor --> Postgres + Kafka(vitals.events)`

### Flow B — Anomaly → user alert (showcase trace, 4 languages)
`stream-processor --Kafka(vitals.events)--> anomaly-detector --> Postgres + Kafka(alerts) --> live-gateway --WebSocket--> mobile`

### Flow C — User query (read-heavy)
`mobile --REST--> health-api --JDBC--> Postgres`

### Flow D — Real-time push (long-lived WS)
`Kafka(alerts) --> live-gateway --WS frame--> connected client` (span linked to the alert trace)

### Flow E — Scheduled batch insights
`CronJob --> insight-worker --Postgres reads--> ML score --> Postgres writes`

## 4. Postgres schema (high level)

| Table | Purpose | Notes |
|---|---|---|
| `users` | account + emergency contact | seeded by loadgen |
| `devices` | wearable registry | `last_seen`, firmware |
| `vitals_rollup_1min` | hot store | 1-min aggregates, TTL 7d |
| `vitals_rollup_1hour` | downsampled | TTL 90d |
| `events` | derived detections | AFib, fall, abnormal HR |
| `alerts` | user-facing alerts | with ack status |
| `workouts` | auto/manual sessions | |
| `insights_daily` | sleep/recovery scores | 1 row/user/day |

DDL lives in `k8s/infra/postgres-init.yaml` (ConfigMap mounted as `/docker-entrypoint-initdb.d`).

## 5. Kafka topics

| Topic | Partitions | Retention | Producer | Consumer |
|---|---|---|---|---|
| `vitals.raw` | 6 | 1h | device-gateway | stream-processor |
| `vitals.events` | 3 | 24h | stream-processor | anomaly-detector |
| `alerts` | 3 | 7d | anomaly-detector | live-gateway |

All producers/consumers inject/extract W3C `traceparent` via Kafka headers.

## 6. Observability plane

### Pattern: Agent (DaemonSet) + Gateway (Deployment)

- **Agent** runs as a DaemonSet on every K3d node — pods send OTLP to `$(NODE_IP):4317` via the downward API.
- **Gateway** runs as a 2-replica Deployment in the `observability` namespace; agents forward to it via OTLP.
- **Sidecar** runs alongside `anomaly-detector` only — demonstrates per-app config (100% sampling on alert spans) and contrasts with the DaemonSet pattern.

### Stack (lightweight)
- **Jaeger all-in-one** (in-memory) — traces UI
- **Prometheus** — scrapes the gateway's `/metrics` (spanmetrics connector emits RED metrics)
- **Grafana** — pre-provisioned dashboards + datasources (Jaeger, Prometheus)

### Resource attributes (added by agent)
- `k8s.cluster.name`, `k8s.node.name`, `k8s.pod.name`, `k8s.namespace.name`, `service.name`, `service.version`

## 7. Kubernetes layout

```
ns: infra            kafka (KRaft, single broker), postgres
ns: observability    otel-gateway (Deployment), otel-agent (DaemonSet),
                     jaeger, prometheus, grafana
ns: wearable         device-gateway, stream-processor, anomaly-detector,
                     health-api, live-gateway, insight-worker (CronJob)
```

K3d cluster: 1 server + 2 agents (3 nodes) so the DaemonSet vs sidecar contrast is meaningful.

## 8. Build & deploy automation

A top-level `Makefile` drives everything:
- `make prereqs` — installs k3d, helm via Homebrew if missing
- `make cluster-up` — creates K3d cluster with port mappings for Grafana/Jaeger
- `make infra-up obs-up` — deploys infra and observability namespaces
- `make build-all` — builds all 6 service Docker images
- `make load-all` — imports images into K3d (`k3d image import`)
- `make deploy-all` — applies app manifests
- `make ui` — port-forwards Grafana (3000), Jaeger (16686), Prometheus (9090)
- `make loadgen` — runs simulated wearables
- `make clean cluster-down` — tear down

## 9. Out of scope (explicit)

- Multi-cluster / federation
- mTLS between services (mentioned but not implemented — gateway accepts cleartext OTLP)
- Real ML models (anomaly-detector uses rule-based stubs + a simple heuristic)
- Persistent Kafka/Postgres (ephemeral volumes; data lost on cluster recreate)
- Authentication / RBAC for app APIs (devices auth via token in metadata, not enforced)
- Helm charts (raw YAML preferred for didactic clarity)

## 10. Testing strategy

- **Per service**: unit tests for business logic (Go: `go test`, Python: `pytest`, Java: JUnit, Node: vitest)
- **Integration**: `loadgen` exercises the full path; verification is "open Jaeger, find a trace spanning all 4 languages"
- **Smoke**: `make smoke` curls `health-api/healthz`, checks Jaeger has spans from each service

## 11. Risks and mitigations

| Risk | Mitigation |
|---|---|
| K3d on macOS networking quirks | Use `--port` mappings on cluster create, document `kubectl port-forward` fallback |
| Kafka memory usage on local cluster | Single-broker KRaft mode, small heap (`-Xmx512m`) |
| Java Spring Boot startup time | Use `eclipse-temurin:21-jre` slim base, pre-warmed health probes |
| OTel context loss across Kafka | Explicit propagator in each language; verify in Jaeger that parent span links across boundary |
| Sidecar vs Agent confusion | README dedicates a section to the comparison with manifest examples |

## 12. Success criteria

1. `make all` runs end-to-end on a fresh macOS dev box (only Docker required pre-installed) and produces a working cluster in < 10 minutes.
2. Opening Jaeger and triggering a synthetic alert via loadgen shows a single trace touching all 4 languages.
3. Grafana shows RED metrics (request rate, error rate, duration) per service via spanmetrics.
4. Killing the OTel gateway pod does NOT lose data — agent buffers (demonstrates the value of the agent layer).
5. The sidecar on `anomaly-detector` captures 100% of alert-classified spans even when the agent is sampling.
