# WearableHealth — Full System Architecture

A complete, component-by-component view of the WearableHealth OpenTelemetry POC: a
polyglot health-telemetry platform on a 3-node **K3d** Kubernetes cluster, with the
**DaemonSet agent + Gateway** collection topology and one deliberate **sidecar** exception.

All diagrams are inline Mermaid — they render in GitHub, the VS Code *Markdown Preview
Mermaid* extension, and any Mermaid viewer.

---

## Component inventory

| Namespace | Component | Tech / Framework | Role |
|---|---|---|---|
| *(external)* | **Wearable device** | — | Pushes vitals over gRPC |
| *(external)* | **Mobile app** | — | REST reads + WebSocket alerts |
| `wearable` | **device-gateway** | Go · gRPC | Ingest, Kafka producer (`vitals.raw`) |
| `wearable` | **stream-processor** | Python · aiokafka | Rollups → Postgres, derive `vitals.events` |
| `wearable` | **anomaly-detector** | Python · aiokafka **(+ sidecar)** | Rule-based detection, produce `alerts` |
| `wearable` | **live-gateway** | Node.js · Fastify | Consume `alerts` → WebSocket push |
| `wearable` | **health-api** | Java · Spring Boot | REST + JDBC reads |
| `wearable` | **insight-worker** | Python · CronJob | Nightly batch scoring |
| `infra` | **Apache Kafka** | KRaft | Event bus: `vitals.raw` → `vitals.events` → `alerts` |
| `infra` | **Postgres** | — | Rollups, events, alerts, insights |
| `observability` | **OTel Agent** | Collector Contrib | **DaemonSet — 1 per node** (node-1/2/3) |
| `observability` | **OTel Sidecar** | Collector Contrib | Runs *inside* the anomaly-detector pod |
| `observability` | **OTel Gateway** | Collector Contrib · 2 replicas | Tail-sampling + spanmetrics |
| `observability` | **Jaeger** | — | Trace storage + UI |
| `observability` | **Prometheus** | — | RED metrics (scrapes gateway `:8889`) |
| `observability` | **Grafana** | — | Unified dashboards |

---

## 1. Full architecture (namespaces, every component)

Solid arrows = **business data** · dotted arrows = **OpenTelemetry telemetry**.

```mermaid
flowchart TB
  W(["Wearable device<br/>(external)"]):::ext
  M(["Mobile app<br/>(external)"]):::ext

  subgraph CL["Kubernetes Cluster — K3d (3 nodes)"]

    subgraph NSI["namespace: infra"]
      K{{"Apache Kafka · event bus<br/>vitals.raw · vitals.events · alerts"}}:::kafka
      PG[("Postgres")]:::db
    end

    subgraph NSW["namespace: wearable"]
      DG["device-gateway<br/>Go · gRPC"]:::go
      SP["stream-processor<br/>Python · aiokafka"]:::py
      AD["anomaly-detector<br/>Python · aiokafka"]:::py
      LG["live-gateway<br/>Node.js · Fastify"]:::node
      HA["health-api<br/>Java · Spring Boot"]:::java
      IW["insight-worker<br/>Python · CronJob"]:::py
    end

    subgraph NSO["namespace: observability"]
      A1["OTel Agent<br/>node-1"]:::otel
      A2["OTel Agent<br/>node-2"]:::otel
      A3["OTel Agent<br/>node-3"]:::otel
      SC["OTel Sidecar<br/>anomaly-detector pod"]:::side
      GW["OTel Gateway<br/>tail-sampling + spanmetrics"]:::gw
      JG["Jaeger · traces"]:::backend
      PR["Prometheus · metrics"]:::backend
      GF["Grafana · dashboards"]:::backend
    end
  end

  %% ---- business data plane ----
  W -->|"gRPC PublishBatch"| DG
  DG -->|"produce vitals.raw"| K
  K -->|"consume vitals.raw"| SP
  SP -->|"produce vitals.events"| K
  K -->|"consume vitals.events"| AD
  AD -->|"produce alerts"| K
  K -->|"consume alerts"| LG
  SP -->|"1-min rollups"| PG
  AD -->|"events / alerts"| PG
  IW -->|"nightly batch"| PG
  HA -->|"JDBC reads"| PG
  LG -->|"WebSocket push"| M
  M -->|"REST GET dashboard"| HA

  %% ---- telemetry plane (OTLP) ----
  DG -. "OTLP" .-> A1
  SP -. "OTLP" .-> A2
  LG -. "OTLP" .-> A3
  HA -. "OTLP" .-> A1
  IW -. "OTLP" .-> A2
  AD -. "OTLP 127.0.0.1" .-> SC
  A1 -. "OTLP" .-> GW
  A2 -. "OTLP" .-> GW
  A3 -. "OTLP" .-> GW
  SC -. "keeps 100% alerts" .-> GW
  GW -. "traces" .-> JG
  GW -. "metrics" .-> PR
  JG -.-> GF
  PR -.-> GF

  classDef ext fill:#FFFFFF,stroke:#90a4ae,color:#37474f,stroke-dasharray:4 3;
  classDef go fill:#D6E4F5,stroke:#1971c2,color:#0B3D91;
  classDef py fill:#ECE7FB,stroke:#7048e8,color:#311B92;
  classDef java fill:#FBE6D4,stroke:#f08c00,color:#7A3B00;
  classDef node fill:#D7F0E3,stroke:#2f9e44,color:#1B4332;
  classDef kafka fill:#FFF3BF,stroke:#f08c00,color:#7A3B00;
  classDef db fill:#ECEFF1,stroke:#546e7a,color:#263238;
  classDef otel fill:#E5DBFF,stroke:#7048e8,color:#311B92;
  classDef side fill:#F8D7DA,stroke:#e03131,color:#7A1F25;
  classDef gw fill:#ffa8a8,stroke:#c92a2a,color:#5c1212;
  classDef backend fill:#D6E4F5,stroke:#1971c2,color:#0B3D91;
```

**Every solid arrow carries `traceparent`** — that is what keeps one trace ID alive
across four languages and across each Kafka producer/consumer boundary, with **zero
correlation code in the apps**.

---

## 2. The DaemonSet collection topology (one agent per node)

A DaemonSet runs **exactly one collector pod per node**. Application pods are scheduled
across the nodes; each pod's SDK exports to *its own node's* agent over the loopback/host
path — never across the network to a central collector. `anomaly-detector` is the lone
exception: it ships an OTel **sidecar** in its pod and exports to `127.0.0.1`.

```mermaid
flowchart TB
  subgraph N1["Node 1"]
    P1["device-gateway pod"]:::go
    P2["health-api pod"]:::java
    AG1["otel-agent<br/>(DaemonSet)"]:::otel
    P1 -. "OTLP $(NODE_IP):4317" .-> AG1
    P2 -. "OTLP $(NODE_IP):4317" .-> AG1
  end

  subgraph N2["Node 2"]
    P3["stream-processor pod"]:::py
    P4["insight-worker pod"]:::py
    AG2["otel-agent<br/>(DaemonSet)"]:::otel
    P3 -. "OTLP $(NODE_IP):4317" .-> AG2
    P4 -. "OTLP $(NODE_IP):4317" .-> AG2
  end

  subgraph N3["Node 3"]
    subgraph POD["anomaly-detector pod"]
      P5["anomaly-detector"]:::py
      SC["otel-sidecar"]:::side
      P5 -. "OTLP 127.0.0.1:4317" .-> SC
    end
    P6["live-gateway pod"]:::node
    AG3["otel-agent<br/>(DaemonSet)"]:::otel
    P6 -. "OTLP $(NODE_IP):4317" .-> AG3
  end

  AG1 -. "OTLP" .-> GW["OTel Gateway<br/>2 replicas · tail-sampling + spanmetrics"]:::gw
  AG2 -. "OTLP" .-> GW
  AG3 -. "OTLP" .-> GW
  SC  -. "OTLP · keeps 100% of alert spans" .-> GW

  GW -->|"traces"| JG["Jaeger"]:::backend
  GW -->|"metrics"| PR["Prometheus"]:::backend
  JG --> GF["Grafana"]:::backend
  PR --> GF

  classDef go fill:#D6E4F5,stroke:#1971c2,color:#0B3D91;
  classDef py fill:#ECE7FB,stroke:#7048e8,color:#311B92;
  classDef java fill:#FBE6D4,stroke:#f08c00,color:#7A3B00;
  classDef node fill:#D7F0E3,stroke:#2f9e44,color:#1B4332;
  classDef otel fill:#E5DBFF,stroke:#7048e8,color:#311B92;
  classDef side fill:#F8D7DA,stroke:#e03131,color:#7A1F25;
  classDef gw fill:#ffa8a8,stroke:#c92a2a,color:#5c1212;
  classDef backend fill:#D6E4F5,stroke:#1971c2,color:#0B3D91;
```

> **Why a sidecar for anomaly-detector?** The gateway tail-samples routine traffic to
> ~10%. The sidecar lets that one service enforce a per-app policy — **keep 100% of alert
> spans** — so a high-severity alert trace is never dropped. Every other service uses the
> cheaper shared node agent. This is the documented exception, not the default.

---

## 3. Telemetry pipeline (what the Gateway does)

```mermaid
flowchart LR
  IN["Spans + metrics + logs<br/>from agents & sidecar"]:::otel --> GW

  subgraph GW["OTel Gateway"]
    direction TB
    ML["memory_limiter"]:::step --> TS["tail_sampling<br/>errors 100% · alerts 100%<br/>slow&gt;1s 100% · baseline 10%"]:::step
    TS --> SM["spanmetrics connector<br/>RED metrics from spans"]:::step
    TS --> BT["batch"]:::step
  end

  BT -->|"traces"| JG["Jaeger"]:::backend
  SM -->|"metrics"| PR["Prometheus :8889"]:::backend
  JG --> GF["Grafana"]:::backend
  PR --> GF

  classDef otel fill:#E5DBFF,stroke:#7048e8,color:#311B92;
  classDef step fill:#FFFFFF,stroke:#7048e8,color:#311B92;
  classDef backend fill:#D6E4F5,stroke:#1971c2,color:#0B3D91;
```

**Apps never name a backend.** Swapping Jaeger for Grafana Tempo is a one-block edit in
the gateway config; no service redeploys. RED metrics are derived from spans by the
`spanmetrics` connector — **no metrics code in any service**.

---

## 4. Traced flows (sequence diagrams)

### Flow 1 · Vitals ingestion (hot path)

```mermaid
sequenceDiagram
    autonumber
    participant W as Wearable
    participant DG as device-gateway (Go)
    participant K1 as Kafka vitals.raw
    participant SP as stream-processor (Py)
    participant PG as Postgres
    participant K2 as Kafka vitals.events
    W->>DG: gRPC PublishBatch(30 readings)
    activate DG
    Note over DG: span device.upload to kafka.produce
    DG->>K1: produce (traceparent in header)
    DG-->>W: PublishAck
    deactivate DG
    K1->>SP: consume (extract traceparent)
    activate SP
    SP->>PG: INSERT vitals_rollup_1min
    SP->>K2: produce derived event (traceparent)
    deactivate SP
```

### Flow 2 · Anomaly alert — 4 languages, 1 trace

```mermaid
sequenceDiagram
    autonumber
    participant SP as stream-processor (Py)
    participant K1 as Kafka vitals.events
    participant AD as anomaly-detector (Py + sidecar)
    participant PG as Postgres
    participant K2 as Kafka alerts
    participant LG as live-gateway (Node)
    participant U as Mobile WS client
    SP->>K1: vitals.events (traceparent)
    K1->>AD: consume
    activate AD
    Note over AD: HR=190 sets alert.severity=high<br/>sidecar keeps 100%
    AD->>PG: INSERT events, alerts
    AD->>K2: produce alert (traceparent)
    deactivate AD
    K2->>LG: consume
    activate LG
    LG->>U: WebSocket frame {type: alert}
    deactivate LG
```

### Flow 3 · Dashboard query (Java auto-instrumented)

```mermaid
sequenceDiagram
    autonumber
    participant U as Mobile client
    participant HA as health-api (Java)
    participant PG as Postgres
    U->>HA: GET /users/{id}/dashboard
    activate HA
    Note over HA: javaagent emits HTTP + JDBC spans, zero code
    par parallel JDBC reads
        HA->>PG: SELECT users
    and
        HA->>PG: SELECT vitals_rollup_1min
    and
        HA->>PG: SELECT open alerts
    end
    HA-->>U: aggregated JSON
    deactivate HA
```

### Flow 4 · Telemetry path (every span)

```mermaid
sequenceDiagram
    autonumber
    participant APP as Any app pod
    participant AG as Node OTel Agent (DaemonSet)
    participant SC as Sidecar (anomaly-detector only)
    participant GW as OTel Gateway
    participant BE as Jaeger / Prometheus
    participant GF as Grafana
    alt default services
        APP->>AG: OTLP to $(NODE_IP):4317
        AG->>GW: OTLP (k8sattributes + batch)
    else anomaly-detector
        APP->>SC: OTLP to 127.0.0.1:4317
        SC->>GW: OTLP (keeps 100% of alerts)
    end
    activate GW
    Note over GW: tail_sampling + spanmetrics
    GW->>BE: traces to Jaeger, metrics to Prometheus
    deactivate GW
    BE->>GF: queried by dashboards
```

---

## 5. Key facts

- **Topology:** 6 services use the node **DaemonSet agent**; `anomaly-detector` uses a
  **sidecar** (the one exception, to keep 100% of alert traces).
- **DaemonSet math:** collectors scale with **nodes** (3 here), not pods — far cheaper
  than one sidecar per pod at scale.
- **Tail sampling (gateway):** errors, alerts, and slow (&gt;1s) traces kept at **100%**;
  routine traffic at **10%**.
- **RED metrics:** derived from spans by the `spanmetrics` connector — no app metric code.
- **Propagation:** `traceparent` flows across gRPC, Kafka, JDBC, and WebSocket — one trace
  ID across Go, Python, Java, and Node.js.

*Tags: opentelemetry, kubernetes, daemonset, sidecar, observability, microservices, mermaid*
