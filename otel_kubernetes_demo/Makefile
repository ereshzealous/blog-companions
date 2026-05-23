# =============================================================================
#  WearableHealth — OTel Agent + Gateway on K3d
# =============================================================================
#  Common targets:
#    make prereqs        Install k3d + helm + kubectl (macOS via brew)
#    make all            FULL setup: cluster + infra + obs + build + load + deploy
#    make simulate       Start the simulated wearables (loadgen)
#    make portforward    Forward Grafana/Jaeger/Prom UIs to localhost
#    make ps             Pod status across all namespaces
#    make down           Delete the cluster (wipes everything)
#    make down-apps      Delete only the wearable app workloads
#    make down-infra     Delete Kafka + Postgres
#    make down-obs       Delete Jaeger/Prom/Grafana/OTel
# =============================================================================

SHELL              := /bin/bash
.SHELLFLAGS        := -eu -o pipefail -c
.DEFAULT_GOAL      := help

# --- Configuration -----------------------------------------------------------
CLUSTER_NAME       ?= wearable
K3S_IMAGE          ?= rancher/k3s:v1.30.4-k3s1
REGISTRY_NAME      ?= wearable-registry
REGISTRY_PORT      ?= 5001
NODES_AGENT        ?= 2

NAMESPACE_INFRA    := infra
NAMESPACE_OBS      := observability
NAMESPACE_APP      := wearable

IMG_TAG            ?= dev
IMG_PREFIX         := wearable

# --- OS / arch detection -----------------------------------------------------
# Auto-detect the host so `make prereqs` works on macOS and Linux (incl. WSL2,
# which is how Windows users run this demo — a Makefile needs a bash shell, so
# there is no native-Windows path). HOST_ARCH normalises to Docker/k8s naming.
OS_NAME            := $(shell uname -s)
HOST_ARCH          := $(shell uname -m | sed -e 's/x86_64/amd64/' -e 's/aarch64/arm64/')

# Per-OS install commands for the three CLIs. macOS keeps Homebrew; Linux/WSL
# use each tool's official cross-OS installer so there is no apt/dnf/pacman
# branching (works identically on Ubuntu, Fedora, Arch, …).
ifeq ($(OS_NAME),Darwin)
  OS_SUPPORTED     := yes
  K3D_INSTALL      := brew install k3d
  HELM_INSTALL     := brew install helm
  KUBECTL_INSTALL  := brew install kubectl
else ifeq ($(OS_NAME),Linux)
  OS_SUPPORTED     := yes
  K3D_INSTALL      := curl -sfL https://raw.githubusercontent.com/k3d-io/k3d/main/install.sh | bash
  HELM_INSTALL     := curl -sfL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
  KUBECTL_INSTALL  := curl -sfLo /tmp/kubectl "https://dl.k8s.io/release/$$(curl -sL https://dl.k8s.io/release/stable.txt)/bin/linux/$(HOST_ARCH)/kubectl" && chmod +x /tmp/kubectl && sudo mv /tmp/kubectl /usr/local/bin/kubectl
else
  OS_SUPPORTED     := no
endif

# buildx must produce a single-platform image matching the k3d nodes
# (arm64 on Apple Silicon, amd64 on Intel/most Linux). Multi-platform manifest
# lists confuse `k3d image import`.
DOCKER_PLATFORM    := linux/$(HOST_ARCH)

SERVICES           := device-gateway stream-processor anomaly-detector health-api live-gateway insight-worker loadgen

GRAFANA_PORT       ?= 3000
JAEGER_PORT        ?= 16686
PROM_PORT          ?= 9090

# Pretty output
B := \033[34m
G := \033[32m
Y := \033[33m
R := \033[31m
X := \033[0m

# =============================================================================
##@ Help
# =============================================================================
.PHONY: help
help: ## Show this help
	@printf "$(B)WearableHealth — OTel Agent + Gateway$(X)\n\n"
	@awk 'BEGIN {FS = ":.*##"} \
	  /^[a-zA-Z0-9_.-]+:.*##/ { printf "  $(G)%-18s$(X) %s\n", $$1, $$2 } \
	  /^##@/                  { printf "\n$(Y)%s$(X)\n", substr($$0, 5) }' \
	  $(MAKEFILE_LIST)

# =============================================================================
##@ Prerequisites
# =============================================================================
.PHONY: prereqs
prereqs: ## Install k3d, helm, kubectl (auto-detects macOS / Linux / WSL)
	@printf "$(B)▶$(X) detected OS: $(OS_NAME) ($(HOST_ARCH))\n"
	@if [ "$(OS_SUPPORTED)" != "yes" ]; then \
	  printf "$(R)✗$(X) unsupported OS '$(OS_NAME)'. Use macOS, Linux, or Windows via WSL2.\n"; exit 1; \
	fi
	@command -v docker >/dev/null || { printf "$(R)✗$(X) Install Docker first (Docker Desktop on macOS / Windows-WSL2, or Docker Engine on Linux).\n"; exit 1; }
	@docker info >/dev/null 2>&1 || { printf "$(R)✗$(X) Docker daemon not running — start Docker and retry.\n"; exit 1; }
	@if [ "$(OS_NAME)" = "Darwin" ]; then \
	  command -v brew >/dev/null || { printf "$(R)✗$(X) Install Homebrew first (https://brew.sh).\n"; exit 1; }; \
	fi
	@command -v k3d     >/dev/null || { printf "$(B)▶$(X) installing k3d\n";     $(K3D_INSTALL); }
	@command -v helm    >/dev/null || { printf "$(B)▶$(X) installing helm\n";    $(HELM_INSTALL); }
	@command -v kubectl >/dev/null || { printf "$(B)▶$(X) installing kubectl\n"; $(KUBECTL_INSTALL); }
	@printf "$(G)✓$(X) prereqs OK\n"

# =============================================================================
##@ One-shot full setup
# =============================================================================
.PHONY: all
all: prereqs cluster-up infra-up obs-up build-all load-all deploy-apps ## Full setup from scratch
	@printf "\n$(G)══════════════════════════════════════════════════════════$(X)\n"
	@printf "$(G) cluster ready$(X)\n"
	@printf "$(G)══════════════════════════════════════════════════════════$(X)\n\n"
	@printf "  Grafana:    http://localhost:$(GRAFANA_PORT)        (admin / admin)\n"
	@printf "  Jaeger:     http://localhost:$(JAEGER_PORT)\n"
	@printf "  Prometheus: http://localhost:$(PROM_PORT)\n\n"
	@printf "  $(Y)make simulate$(X)     start simulated wearables\n"
	@printf "  $(Y)make portforward$(X)  if browser ports aren't bound\n"
	@printf "  $(Y)make ps$(X)           check pod status\n\n"

# =============================================================================
##@ Cluster
# =============================================================================
.PHONY: cluster-up cluster-status registry-up

registry-up:
	@if ! k3d registry list 2>/dev/null | grep -q "$(REGISTRY_NAME)"; then \
	  printf "$(B)▶$(X) creating registry\n"; \
	  k3d registry create $(REGISTRY_NAME) --port $(REGISTRY_PORT); \
	else \
	  printf "$(G)✓$(X) registry already exists\n"; \
	fi

cluster-up: registry-up ## Create K3d cluster (1 server + 2 agents)
	@if k3d cluster list 2>/dev/null | grep -q "^$(CLUSTER_NAME) "; then \
	  printf "$(G)✓$(X) cluster $(CLUSTER_NAME) already exists\n"; \
	else \
	  printf "$(B)▶$(X) creating cluster $(CLUSTER_NAME)\n"; \
	  k3d cluster create $(CLUSTER_NAME) \
	    --image $(K3S_IMAGE) \
	    --agents $(NODES_AGENT) \
	    --registry-use k3d-$(REGISTRY_NAME):$(REGISTRY_PORT) \
	    --port "$(GRAFANA_PORT):$(GRAFANA_PORT)@loadbalancer" \
	    --port "$(JAEGER_PORT):$(JAEGER_PORT)@loadbalancer" \
	    --port "$(PROM_PORT):$(PROM_PORT)@loadbalancer" \
	    --k3s-arg "--disable=traefik@server:0" \
	    --wait; \
	fi
	@kubectl cluster-info | head -3

cluster-status: ## Show cluster + node status
	@k3d cluster list
	@echo
	@kubectl get nodes -o wide

# =============================================================================
##@ Namespaces / Infra / Observability
# =============================================================================
.PHONY: ns infra-up obs-up

ns:
	@kubectl apply -f k8s/base/namespaces.yaml

infra-up: ns ## Deploy Postgres + Kafka
	@printf "$(B)▶$(X) deploying infra (postgres, kafka)\n"
	@kubectl apply -f k8s/infra/
	@printf "$(B)▶$(X) waiting for postgres (up to 180s)\n"
	@kubectl -n $(NAMESPACE_INFRA) rollout status statefulset/postgres --timeout=180s \
	  || { printf "$(R)✗$(X) postgres failed — run: $(Y)kubectl -n $(NAMESPACE_INFRA) describe pod postgres-0$(X)\n"; exit 1; }
	@printf "$(B)▶$(X) waiting for kafka (up to 300s — first run pulls ~700MB image)\n"
	@kubectl -n $(NAMESPACE_INFRA) rollout status statefulset/kafka --timeout=300s \
	  || { printf "$(R)✗$(X) kafka failed — run: $(Y)kubectl -n $(NAMESPACE_INFRA) logs kafka-0$(X)\n"; exit 1; }
	@printf "$(G)✓$(X) infra up\n"

obs-up: ns ## Deploy OTel agent + gateway + Jaeger + Prom + Grafana
	@printf "$(B)▶$(X) deploying observability\n"
	@kubectl apply -f k8s/observability/
	@kubectl -n $(NAMESPACE_OBS) rollout status deployment/otel-gateway --timeout=120s || true
	@kubectl -n $(NAMESPACE_OBS) rollout status deployment/jaeger       --timeout=120s || true
	@kubectl -n $(NAMESPACE_OBS) rollout status deployment/prometheus   --timeout=120s || true
	@kubectl -n $(NAMESPACE_OBS) rollout status deployment/grafana      --timeout=120s || true
	@kubectl -n $(NAMESPACE_OBS) rollout status daemonset/otel-agent     --timeout=120s || true
	@printf "$(G)✓$(X) observability up\n"

# =============================================================================
##@ Build / Load images
# =============================================================================
# NOTE: build-% and load-% are pattern rules — do NOT add them to .PHONY,
# that would create empty target rules that shadow the pattern.
.PHONY: build-all load-all

build-all: $(addprefix build-,$(SERVICES)) ## Build all 7 service Docker images
	@printf "$(G)✓$(X) all images built\n"

build-%:
	@printf "$(B)▶$(X) build $(IMG_PREFIX)/$*:$(IMG_TAG) [$(DOCKER_PLATFORM)]\n"
	@docker buildx build --load --platform $(DOCKER_PLATFORM) \
	  -t $(IMG_PREFIX)/$*:$(IMG_TAG) ./services/$*
	@docker image inspect $(IMG_PREFIX)/$*:$(IMG_TAG) >/dev/null 2>&1 \
	  || { printf "$(R)✗$(X) image not in local docker store after build — check buildx config\n"; exit 1; }

load-all: $(addprefix load-,$(SERVICES)) ## Import all images into K3d
	@printf "$(G)✓$(X) all images loaded into k3d\n"

load-%:
	@printf "$(B)▶$(X) load $(IMG_PREFIX)/$*:$(IMG_TAG) → k3d\n"
	@docker image inspect $(IMG_PREFIX)/$*:$(IMG_TAG) >/dev/null 2>&1 \
	  || { printf "$(R)✗$(X) image missing locally — run: $(Y)make build-$*$(X)\n"; exit 1; }
	@k3d image import $(IMG_PREFIX)/$*:$(IMG_TAG) -c $(CLUSTER_NAME)

# =============================================================================
##@ Deploy / Redeploy app services
# =============================================================================
.PHONY: deploy-apps redeploy

APP_DEPLOYMENTS := device-gateway stream-processor anomaly-detector health-api live-gateway

# Apply every app manifest EXCEPT loadgen — loadgen is on-demand via `make simulate`.
# We FORCE rollout restart so freshly-loaded images with the same tag are picked
# up by kubelet (kubectl apply alone won't restart pods if the manifest is unchanged).
deploy-apps: ns ## Apply app manifests + force rollout restart (excluding loadgen)
	@printf "$(B)▶$(X) applying app manifests\n"
	@for f in k8s/apps/*.yaml; do \
	  case "$$f" in *loadgen.yaml) ;; *) kubectl apply -f "$$f" ;; esac \
	done
	@printf "$(B)▶$(X) forcing rollout restart so new images are picked up\n"
	@for d in $(APP_DEPLOYMENTS); do kubectl -n $(NAMESPACE_APP) rollout restart deploy/$$d; done
	@printf "$(B)▶$(X) waiting for all app deployments to be Ready (up to 240s each)\n"
	@failed=0; for d in $(APP_DEPLOYMENTS); do \
	  printf "$(B)  · $$d$(X)\n"; \
	  kubectl -n $(NAMESPACE_APP) rollout status deploy/$$d --timeout=240s \
	    || { printf "$(R)  ✗ $$d failed — logs: $(Y)kubectl -n $(NAMESPACE_APP) logs deploy/$$d --tail=40$(X)\n"; failed=1; }; \
	done; \
	if [ $$failed -ne 0 ]; then exit 1; fi
	@printf "$(G)✓$(X) apps deployed and Ready\n"

redeploy: build-all load-all ## Rebuild + reimport + restart app pods
	@kubectl -n $(NAMESPACE_APP) rollout restart deployment
	@printf "$(G)✓$(X) rollout restarted\n"

# =============================================================================
##@ Simulate (loadgen)
# =============================================================================
.PHONY: simulate simulate-stop simulate-logs simulate-once simulate-single simulate-batch simulate-fanout simulate-read simulate-insights sampling-all sampling-default loadgen-up

# Ensure loadgen is deployed (it is NOT part of `make all`). Idempotent.
loadgen-up:
	@kubectl -n $(NAMESPACE_APP) get deploy loadgen >/dev/null 2>&1 || { \
	  printf "$(B)▶$(X) loadgen not deployed — deploying it now\n"; \
	  docker image inspect $(IMG_PREFIX)/loadgen:$(IMG_TAG) >/dev/null 2>&1 || $(MAKE) build-loadgen; \
	  k3d image import $(IMG_PREFIX)/loadgen:$(IMG_TAG) -c $(CLUSTER_NAME) >/dev/null 2>&1 || true; \
	  kubectl apply -f k8s/apps/loadgen.yaml; \
	  kubectl -n $(NAMESPACE_APP) rollout status deploy/loadgen --timeout=90s; }

simulate: ## Start the simulated wearables sending vitals
	@printf "$(B)▶$(X) starting loadgen\n"
	@docker image inspect $(IMG_PREFIX)/loadgen:$(IMG_TAG) >/dev/null 2>&1 || $(MAKE) build-loadgen
	@k3d image import $(IMG_PREFIX)/loadgen:$(IMG_TAG) -c $(CLUSTER_NAME) >/dev/null
	@kubectl apply -f k8s/apps/loadgen.yaml
	@kubectl -n $(NAMESPACE_APP) rollout status deploy/loadgen --timeout=60s
	@printf "$(G)✓$(X) loadgen running. tail logs: $(Y)make simulate-logs$(X)\n"

simulate-stop: ## Stop the simulated wearables
	@kubectl delete -f k8s/apps/loadgen.yaml --ignore-not-found=true
	@printf "$(G)✓$(X) loadgen stopped\n"

simulate-logs: ## Tail loadgen logs
	@kubectl -n $(NAMESPACE_APP) logs -f deploy/loadgen

# ANOM=true forces an anomalous batch (trips the alert pipeline). ANOM=false for normal.
ANOM ?= true
simulate-once: loadgen-up ## Trigger ONE on-demand vitals upload via loadgen's /simulate endpoint
	@printf "$(B)▶$(X) POST /simulate?anomalous=$(ANOM)\n"
	@kubectl -n $(NAMESPACE_APP) port-forward svc/loadgen 8080:8080 >/dev/null 2>&1 & \
	  pf=$$!; trap "kill $$pf 2>/dev/null" EXIT; sleep 2; \
	  curl -fsS -XPOST "http://localhost:8080/simulate?anomalous=$(ANOM)"; echo

simulate-single: loadgen-up ## SINGLE upload -> one clean end-to-end trace (ANOM=true|false)
	@printf "$(B)▶$(X) SINGLE upload (anomalous=$(ANOM))\n"
	@kubectl -n $(NAMESPACE_APP) port-forward svc/loadgen 8080:8080 >/dev/null 2>&1 & \
	  pf=$$!; trap "kill $$pf 2>/dev/null" EXIT; sleep 3; \
	  curl -fsS -XPOST "http://localhost:8080/simulate?anomalous=$(ANOM)"; echo; \
	  printf "$(G)✓$(X) open the returned trace_id in Jaeger (one waterfall)\n"

# COUNT parallel uploads -> one getmany() poll grabs several -> 'vitals.raw receive'
# trace with N links + N independent end-to-end traces.
COUNT ?= 6
simulate-batch: loadgen-up ## BATCH: fire COUNT uploads in parallel -> linked traces demo (COUNT=6 ANOM=false)
	@printf "$(B)▶$(X) BATCH: $(COUNT) parallel uploads (anomalous=$(ANOM))\n"
	@kubectl -n $(NAMESPACE_APP) port-forward svc/loadgen 8080:8080 >/dev/null 2>&1 & \
	  pf=$$!; trap "kill $$pf 2>/dev/null" EXIT; sleep 3; \
	  for i in $$(seq 1 $(COUNT)); do \
	    ( curl -fsS -XPOST "http://localhost:8080/simulate?anomalous=$(ANOM)" \
	        | grep -o '"trace_id":"[^"]*"' || echo "  upload failed" ) & \
	  done; wait; \
	  printf "$(G)✓$(X) fired $(COUNT) -> in Jaeger find service=stream-processor op='vitals.raw receive' (links = batch size)\n"

# FAN separate Kafka messages from ONE PublishBatch, ALL under ONE trace id
# (x-fanout-count gRPC metadata). Shows a list-of-messages waterfall in one trace.
FAN ?= 10
simulate-fanout: loadgen-up ## FAN-OUT: one upload -> FAN messages, all under ONE trace id (FAN=10 ANOM=false)
	@printf "$(B)▶$(X) FAN-OUT: one PublishBatch -> $(FAN) messages under one trace (anomalous=$(ANOM))\n"
	@kubectl -n $(NAMESPACE_APP) port-forward svc/loadgen 8080:8080 >/dev/null 2>&1 & \
	  pf=$$!; trap "kill $$pf 2>/dev/null" EXIT; sleep 3; \
	  curl -fsS -XPOST "http://localhost:8080/simulate?anomalous=$(ANOM)&fanout=$(FAN)"; echo; \
	  printf "$(G)✓$(X) open the returned trace_id in Jaeger -> ONE trace, $(FAN) kafka.produce->process branches\n"

simulate-read: loadgen-up ## READ path: seed a user, then health-api GET dashboard/insights + ack (REST in / JDBC out)
	@printf "$(B)▶$(X) seeding a user then reading via health-api\n"
	@u=$$(uuidgen); \
	  kubectl -n $(NAMESPACE_APP) port-forward svc/loadgen 8080:8080 >/dev/null 2>&1 & lpf=$$!; sleep 3; \
	  curl -fsS -XPOST "http://localhost:8080/simulate?anomalous=true&user_id=$$u&device_id=$$(uuidgen)" >/dev/null; \
	  kill $$lpf 2>/dev/null; sleep 8; \
	  kubectl -n $(NAMESPACE_APP) port-forward svc/health-api 8081:8080 >/dev/null 2>&1 & hpf=$$!; sleep 3; \
	  printf "  user=$$u\n"; \
	  printf "  dashboard: "; curl -fsS "http://localhost:8081/users/$$u/dashboard"; echo; \
	  printf "  insights:  "; curl -fsS "http://localhost:8081/users/$$u/insights"; echo; \
	  aid=$$(curl -fsS "http://localhost:8081/users/$$u/alerts" | python3 -c "import sys,json;a=json.load(sys.stdin);print(a[0]['id'] if a else '')"); \
	  printf "  ack=$$aid "; [ -n "$$aid" ] && curl -fsS -XPOST "http://localhost:8081/users/$$u/alerts/$$aid/ack"; echo; \
	  kill $$hpf 2>/dev/null; \
	  printf "$(G)✓$(X) health-api traces emitted (HTTP GET + JDBC SELECT + UPDATE)\n"

simulate-insights: ## BATCH JOB: run insight-worker once (CronJob run-once -> JDBC)
	@printf "$(B)▶$(X) running insight-worker batch\n"
	@kubectl -n $(NAMESPACE_APP) delete job insight-now --ignore-not-found >/dev/null 2>&1
	@kubectl -n $(NAMESPACE_APP) create job --from=cronjob/insight-worker insight-now
	@kubectl -n $(NAMESPACE_APP) wait --for=condition=complete job/insight-now --timeout=90s
	@printf "$(G)✓$(X) insight-worker done (insight.batch trace emitted)\n"

sampling-all: ## Tail-sampling baseline -> 100%% (guarantee every routine trace lands)
	@perl -pi -e 's/sampling_percentage: \d+/sampling_percentage: 100/' k8s/observability/otel-gateway.yaml
	@kubectl apply -f k8s/observability/otel-gateway.yaml >/dev/null
	@kubectl -n $(NAMESPACE_OBS) rollout restart deployment/otel-gateway >/dev/null
	@kubectl -n $(NAMESPACE_OBS) rollout status deployment/otel-gateway --timeout=120s
	@printf "$(G)✓$(X) baseline sampling = 100%% (revert with: make sampling-default)\n"

sampling-default: ## Tail-sampling baseline -> 10%% (documented default)
	@perl -pi -e 's/sampling_percentage: \d+/sampling_percentage: 10/' k8s/observability/otel-gateway.yaml
	@kubectl apply -f k8s/observability/otel-gateway.yaml >/dev/null
	@kubectl -n $(NAMESPACE_OBS) rollout restart deployment/otel-gateway >/dev/null
	@kubectl -n $(NAMESPACE_OBS) rollout status deployment/otel-gateway --timeout=120s
	@printf "$(G)✓$(X) baseline sampling = 10%%\n"

# =============================================================================
##@ Port-forward UIs and APIs
# =============================================================================
.PHONY: portforward pf-grafana pf-jaeger pf-prom pf-health-api pf-live-gateway pf-loadgen

portforward: ## Port-forward Grafana/Jaeger/Prom (parallel)
	@printf "$(B)▶$(X) port-forwarding Grafana :$(GRAFANA_PORT), Jaeger :$(JAEGER_PORT), Prom :$(PROM_PORT)\n"
	@printf "$(Y)Ctrl-C to stop$(X)\n\n"
	@$(MAKE) -j3 --no-print-directory pf-grafana pf-jaeger pf-prom

pf-grafana:
	@kubectl -n $(NAMESPACE_OBS) port-forward svc/grafana    $(GRAFANA_PORT):3000

pf-jaeger:
	@kubectl -n $(NAMESPACE_OBS) port-forward svc/jaeger     $(JAEGER_PORT):16686

pf-prom:
	@kubectl -n $(NAMESPACE_OBS) port-forward svc/prometheus $(PROM_PORT):9090

pf-health-api: ## Port-forward health-api REST to :8080
	@kubectl -n $(NAMESPACE_APP) port-forward svc/health-api 8080:8080

pf-live-gateway: ## Port-forward live-gateway WebSocket to :7000
	@kubectl -n $(NAMESPACE_APP) port-forward svc/live-gateway 7000:7000

pf-loadgen: ## Port-forward loadgen /simulate endpoint to :8080
	@kubectl -n $(NAMESPACE_APP) port-forward svc/loadgen 8080:8080

# =============================================================================
##@ Diagnostics
# =============================================================================
.PHONY: ps logs describe-failed otel-agent-logs otel-gateway-logs

ps: ## Show pod status across all namespaces
	@printf "$(Y)── $(NAMESPACE_INFRA) ──$(X)\n"
	@kubectl -n $(NAMESPACE_INFRA) get pods 2>/dev/null || true
	@printf "\n$(Y)── $(NAMESPACE_OBS) ──$(X)\n"
	@kubectl -n $(NAMESPACE_OBS)   get pods 2>/dev/null || true
	@printf "\n$(Y)── $(NAMESPACE_APP) ──$(X)\n"
	@kubectl -n $(NAMESPACE_APP)   get pods 2>/dev/null || true

logs: ## Tail recent logs across the wearable namespace
	@kubectl -n $(NAMESPACE_APP) get pods -o name | xargs -I {} sh -c \
	  'printf "$(Y)===== {} =====$(X)\n"; kubectl -n $(NAMESPACE_APP) logs --tail=50 {}'

describe-failed: ## Describe any non-Running pods
	@for ns in $(NAMESPACE_INFRA) $(NAMESPACE_OBS) $(NAMESPACE_APP); do \
	  kubectl -n $$ns get pods --field-selector=status.phase!=Running -o name 2>/dev/null | \
	    xargs -I {} sh -c "printf '$(Y)===== '$$ns'/{} =====$(X)\n'; kubectl -n $$ns describe {}"; \
	done

otel-agent-logs: ## Tail OTel agent (DaemonSet) logs
	@kubectl -n $(NAMESPACE_OBS) logs -l app=otel-agent --tail=200 -f

otel-gateway-logs: ## Tail OTel gateway logs
	@kubectl -n $(NAMESPACE_OBS) logs -l app=otel-gateway --tail=200 -f

# =============================================================================
##@ Teardown
# =============================================================================
.PHONY: down down-all down-apps down-infra down-obs down-cluster down-registry

down: down-cluster ## Delete the cluster (everything inside it goes)

down-all: down-cluster down-registry ## Delete cluster AND local registry
	@printf "$(G)✓$(X) everything torn down\n"

down-apps: ## Delete only the app workloads (keeps infra + obs)
	@printf "$(B)▶$(X) deleting apps\n"
	@kubectl delete -f k8s/apps/ --ignore-not-found=true
	@printf "$(G)✓$(X) apps down\n"

down-infra: ## Delete Postgres + Kafka
	@printf "$(B)▶$(X) deleting infra\n"
	@kubectl delete -f k8s/infra/ --ignore-not-found=true
	@printf "$(G)✓$(X) infra down\n"

down-obs: ## Delete Jaeger / Prom / Grafana / OTel
	@printf "$(B)▶$(X) deleting observability\n"
	@kubectl delete -f k8s/observability/ --ignore-not-found=true
	@printf "$(G)✓$(X) observability down\n"

down-cluster: ## Delete the K3d cluster
	@printf "$(B)▶$(X) deleting cluster $(CLUSTER_NAME)\n"
	@k3d cluster delete $(CLUSTER_NAME) 2>/dev/null || true
	@printf "$(G)✓$(X) cluster deleted\n"

down-registry: ## Delete the local k3d registry
	@k3d registry delete $(REGISTRY_NAME) 2>/dev/null || true
	@printf "$(G)✓$(X) registry deleted\n"

clean-images: ## Remove built Docker images locally
	@for s in $(SERVICES); do docker rmi $(IMG_PREFIX)/$$s:$(IMG_TAG) 2>/dev/null || true; done
	@printf "$(G)✓$(X) local images removed\n"
