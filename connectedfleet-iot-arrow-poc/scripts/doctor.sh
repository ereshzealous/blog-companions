#!/usr/bin/env bash
# Read-only environment check. Never starts, stops or modifies containers.
# Exit 1 only when Lab 01 (no Docker) cannot run; Docker findings are warnings
# until the Compose pipeline exists.
set -uo pipefail

RECOMMENDED_DOCKER_MEM_GIB=12
PORTS=(9092 8083 5432 27017 8123 9000)
fail=0

ok()   { printf "  \033[32mOK\033[0m    %-22s %s\n" "$1" "$2"; }
warn() { printf "  \033[33mWARN\033[0m  %-22s %s\n" "$1" "$2"; }
bad()  { printf "  \033[31mFAIL\033[0m  %-22s %s\n" "$1" "$2"; fail=1; }

echo "ConnectedFleet doctor ($(date -u +%Y-%m-%dT%H:%M:%SZ))"
echo "Host"
ok "platform" "$(uname -s) $(uname -m)"
if [[ "$(uname -s)" == "Darwin" ]]; then
  cpus=$(sysctl -n hw.ncpu); mem=$(( $(sysctl -n hw.memsize) / 1073741824 ))
else
  cpus=$(nproc); mem=$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) / 1048576 ))
fi
ok "cpu / memory" "${cpus} CPUs, ${mem} GiB"
free_gib=$(df -Pk . | awk 'NR==2 {printf "%d", $4/1048576}')
if (( free_gib < 20 )); then warn "free disk" "${free_gib} GiB (20+ GiB recommended for images and benchmark data)"; else ok "free disk" "${free_gib} GiB"; fi

echo "Lab 01 requirements (no Docker)"
if command -v uv >/dev/null 2>&1; then
  ok "uv" "$(uv --version)"
  if py=$(uv python find 3.12 2>/dev/null); then ok "python 3.12" "$py"; else bad "python 3.12" "not found; run: uv python install 3.12"; fi
else
  bad "uv" "not installed (https://docs.astral.sh/uv/)"
fi
if command -v git >/dev/null 2>&1; then ok "git" "$(git --version)"; else warn "git" "missing; run manifests will record no commit"; fi
if [[ -f uv.lock ]]; then ok "uv.lock" "present"; else bad "uv.lock" "missing; run: uv lock"; fi

echo "Lab 02 / Lab 03 requirements (Docker Compose)"
if ! command -v docker >/dev/null 2>&1; then
  warn "docker" "CLI not installed; needed from Lab 02 on"
elif ! docker info >/dev/null 2>&1; then
  warn "docker" "daemon not reachable; start Docker Desktop before Lab 02"
else
  ok "docker" "$(docker version --format '{{.Server.Version}}' 2>/dev/null)"
  if docker compose version >/dev/null 2>&1; then ok "compose" "$(docker compose version --short)"; else warn "compose" "docker compose plugin missing"; fi
  dmem=$(( $(docker info --format '{{.MemTotal}}') / 1073741824 ))
  dcpu=$(docker info --format '{{.NCPU}}')
  if (( dmem < RECOMMENDED_DOCKER_MEM_GIB )); then
    warn "docker resources" "${dcpu} CPUs, ${dmem} GiB; raise to ${RECOMMENDED_DOCKER_MEM_GIB}+ GiB for Lab 02/03"
  else
    ok "docker resources" "${dcpu} CPUs, ${dmem} GiB"
  fi
fi
for port in "${PORTS[@]}"; do
  if command -v lsof >/dev/null 2>&1 && lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
    warn "port $port" "in use by $(lsof -nP -iTCP:"$port" -sTCP:LISTEN | awk 'NR==2 {print $1}')"
  else
    ok "port $port" "free"
  fi
done

if (( fail )); then echo "doctor: Lab 01 prerequisites missing"; exit 1; fi
echo "doctor: Lab 01 prerequisites satisfied (warnings above affect Lab 02/03 only)"
