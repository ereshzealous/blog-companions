# WearableHealth — OTel Agent + Gateway on K3d

A polyglot microservices demo showing **OpenTelemetry** end-to-end across **Go, Python, Java, and Node.js** — spanning **gRPC, REST, Kafka, Postgres, WebSocket, and async batch** — deployed entirely on a local **K3d** (K3s in Docker) Kubernetes cluster.

The headline pattern is **OTel Agent (DaemonSet) + Gateway (Deployment)**, with **one sidecar collector** included on the `anomaly-detector` service for contrast.

---

## Domain — WearableHealth

A continuous health telemetry platform inspired by Fitbit/Apple Health/Whoop. Simulated wearables push vitals → derived signals → anomaly detection → real-time alerts → user dashboards + daily insights.

## Architecture at a glance

```
wearable ──gRPC──► device-gateway (Go) ──Kafka(vitals.raw)──► stream-processor (Py)
                                                                  │
                                                  Postgres ◄──────┤
                                                                  ▼
                                                  Kafka(vitals.events)
                                                                  │
                                                                  ▼
                                                      anomaly-detector (Py, sidecar)
                                                                  │
                                                  Postgres ◄──────┤
                                                                  ▼
                                                       Kafka(alerts)
                                                                  │
                                                                  ▼
                                                      live-gateway (Node) ──WS──► mobile

  health-api (Java) ──JDBC──► Postgres                  (user-facing reads)
  insight-worker (Py, CronJob) ──► Postgres             (nightly batch)
```

Every service sends OTLP to its node-local OTel agent (DaemonSet) via `$(NODE_IP):4317` — **except** `anomaly-detector`, which sends to its **sidecar** collector at `127.0.0.1:4317`.

All agents and the sidecar forward to a centralized **OTel Gateway** (2-replica Deployment). The gateway:
- runs **tail sampling** (100% on errors + alert spans, 10% baseline)
- emits **RED metrics** via the `spanmetrics` connector to Prometheus
- exports traces to Jaeger

---

## Prerequisites

Works on **macOS** and **Linux**. **Windows** users run it inside **WSL2** (a
Makefile needs a bash shell, so there's no native-Windows path — WSL2 *is*
Linux as far as this demo is concerned).

- **Docker** running:
  - macOS / Windows → **Docker Desktop**
  - Linux → **Docker Engine** (WSL2: Docker Desktop with the WSL integration)
- ~6 GB free RAM and ~4 CPU for the cluster
- macOS only: **Homebrew** (`make prereqs` installs the CLIs via `brew`)

`make prereqs` **auto-detects your OS** (`uname -s`) and installs `k3d`,
`kubectl`, and `helm` for you — Homebrew on macOS, the tools' official
cross-OS install scripts on Linux/WSL (no apt/dnf/pacman branching, so it
works the same on Ubuntu, Fedora, Arch, etc.). The Linux kubectl install uses
`sudo` to drop the binary into `/usr/local/bin`.

---

## Quickstart

```bash
# 1. Install k3d / helm / kubectl if missing
make prereqs

# 2. Spin up everything: cluster + infra + observability + build + load + deploy
make all

# 3. Open the UIs
open http://localhost:3000        # Grafana (admin/admin)
open http://localhost:16686       # Jaeger
open http://localhost:9090        # Prometheus

# 4. Start the simulated wearables
make simulate
```

If the LoadBalancer port-bindings don't work (e.g. another app on :3000), fall back to:

```bash
make portforward     # port-forwards Grafana / Jaeger / Prometheus
```

---

## Make targets (the important ones)

| Target | What it does |
|---|---|
| `make prereqs` | Install k3d, helm, kubectl via Homebrew |
| `make cluster-up` | Create 3-node K3d cluster + local registry |
| `make infra-up` | Deploy Postgres + Kafka |
| `make obs-up` | Deploy OTel agent + gateway + Jaeger + Prom + Grafana |
| `make build-all` | Build all 7 service Docker images |
| `make load-all` | `k3d image import` all images |
| `make deploy-apps` | Apply app manifests + force rollout restart |
| `make redeploy` | Rebuild → reimport → rollout-restart |
| `make simulate` | Start the simulated wearables (continuous loop) |
| `make simulate-once` | Trigger ONE on-demand upload (`ANOM=true` forces an alert) |
| `make ps` | Show pod status across all namespaces |
| `make logs` | Tail logs across the wearable namespace |
| `make otel-agent-logs` | Tail the DaemonSet agent |
| `make otel-gateway-logs` | Tail the central gateway |
| `make portforward` | Port-forward Grafana/Jaeger/Prometheus |
| `make pf-loadgen` | Port-forward loadgen's `/simulate` endpoint to :8080 |
| `make down` | Delete the cluster |

Run `make help` for the full list.

---

## What to look for in Jaeger

After running `make simulate`, open Jaeger and filter by service. The juicy trace is:

> **Find traces with `alert.severity=high`** — these touch 4 languages in one trace:
> `loadgen (Go) → device-gateway (Go) → stream-processor (Python) → anomaly-detector (Python) → live-gateway (Node)`

You'll see Kafka producer/consumer spans linked by `traceparent` headers, JDBC spans from Postgres reads, and a final `ws.push` span at the end.

---

## Generating load

The `loadgen` service runs in two modes at once:

**Continuous loop** — on startup it mints `DEVICE_COUNT` (default `3`) simulated
wearables, each with random v4 UUIDs for the device and its user, and pushes a
30-reading batch every `INTERVAL` (default `5s`). The startup logs print the
generated identities so you can drop a `user_id` straight into a `health-api`
URL (`/users/{userId}`):

```
device 3f2a… → user 9c11…
```

Every 20th batch per device is intentionally anomalous (heart rate 190+),
tripping the tachycardia rule so the alert pipeline produces an
`alert.severity=high` trace. These loop-driven spans are tagged `trigger=loop`.

**On-demand endpoint** — loadgen also serves `POST /simulate`, which publishes
exactly **one** batch when you ask it to. Spans from it are tagged
`trigger=manual`, and the JSON response includes the `trace_id` so you can jump
straight to that trace in Jaeger.

```bash
# one-shot via Make (forces an anomalous batch by default)
make simulate-once               # ANOM=true
make simulate-once ANOM=false    # normal batch

# or port-forward and curl with your own params
make pf-loadgen                  # → localhost:8080
curl -XPOST "localhost:8080/simulate?anomalous=true"
curl -XPOST "localhost:8080/simulate?device_id=$(uuidgen)&user_id=$(uuidgen)"
```

Without `device_id` the endpoint mints a brand-new random device per call; pass
`device_id` (and optionally `user_id`) to reuse a fixed identity. Response:

```json
{"device_id":"…","user_id":"…","anomalous":true,"accepted":30,"rejected":0,"trace_id":"4bf92f3577b34da6…"}
```

Paste that `trace_id` into Jaeger's *Search by Trace ID*, or filter spans by
`trigger=manual` to see only your on-demand uploads.

---

## DaemonSet vs Sidecar — why both?

| | Agent (DaemonSet) | Sidecar |
|---|---|---|
| Pods/cluster | one per node | one per app pod |
| Resource cost | low, amortized | high at scale |
| Config flexibility | uniform | per-app |
| Failure blast radius | all pods on node | only that pod |
| App reaches it via | `$(NODE_IP):4317` (downward API) | `127.0.0.1:4317` |

In this repo, **all services use the DaemonSet agent** except `anomaly-detector`. The sidecar there isn't strictly needed — it's there to demonstrate the pattern and to enforce **100% sampling on alert spans** (so the gateway's tail sampler never drops them).

Compare the two manifests:
- `k8s/apps/device-gateway.yaml` — DaemonSet pattern (single container, OTLP → `$(NODE_IP)`)
- `k8s/apps/anomaly-detector.yaml` — Sidecar pattern (two containers, OTLP → `127.0.0.1`)

---

## Repository layout

```
.
├── Makefile                    # installs k3d, builds + deploys everything
├── otel-config/                # standalone copies of agent/gateway/sidecar configs
├── k8s/
│   ├── base/                   # namespaces
│   ├── infra/                  # kafka, postgres
│   ├── observability/          # otel agent+gateway, jaeger, prom, grafana
│   └── apps/                   # one manifest per service
└── services/
    ├── device-gateway/         # Go, gRPC ingress, Kafka producer
    ├── stream-processor/       # Python, Kafka in/out, Postgres rollups
    ├── anomaly-detector/       # Python (sidecar pattern), ML stubs
    ├── health-api/             # Java/Spring Boot, REST + JDBC
    ├── live-gateway/           # Node.js, Fastify + WebSocket
    ├── insight-worker/         # Python CronJob, batch scoring
    └── loadgen/                # Go, simulated wearable devices
```

---

## Troubleshooting

**Pods stuck Pending after `make all`** — Docker Desktop probably needs more memory. Bump to 8 GB and retry.

**No traces in Jaeger** — `make otel-agent-logs` and `make otel-gateway-logs`. The most common cause is the agent can't reach the gateway service (`kubectl -n observability get svc otel-gateway`).

**Kafka pod CrashLoopBackOff** — KRaft mode needs the broker hostname to be stable. The StatefulSet uses `kafka-0.kafka.infra.svc.cluster.local` — make sure that's resolvable: `kubectl -n infra run -it --rm dns --image=busybox -- nslookup kafka-0.kafka.infra.svc.cluster.local`.

**`make build-all` fails on `loadgen` or `device-gateway`** — these run `protoc` at image-build time. If you've cached an old layer, run `make build-device-gateway` (or `build-loadgen`) directly to see the error.

**Java image build is slow first time** — Maven downloads ~150 MB of dependencies and the OTel javaagent. Subsequent builds use the layer cache and are fast.

---

## Out of scope

- Multi-cluster federation
- mTLS between services
- Real ML models (we use rule-based stubs)
- Persistent storage (ephemeral volumes — data lost on cluster delete)
- App-level authentication / RBAC
- Helm charts (raw YAML is more didactic for this demo)
