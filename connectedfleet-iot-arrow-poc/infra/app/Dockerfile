# ConnectedFleet app image: normalizer, producers, row/Arrow lane workers, benchmark helpers.
# Same pinned dependencies as the host (uv.lock). Build context = repository root.
FROM ghcr.io/astral-sh/uv:0.11.7 AS uv

FROM python:3.12.13-slim
COPY --from=uv /uv /uvx /usr/local/bin/
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_COMPILE_BYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH=/opt/venv/bin:$PATH
WORKDIR /app

# Dependencies first (cached layer), then the project itself.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
COPY contracts ./contracts
COPY fixtures ./fixtures
COPY infra/clickhouse ./infra/clickhouse
COPY benchmarks/workloads ./benchmarks/workloads
RUN uv sync --frozen --no-dev

RUN useradd --create-home --uid 10001 cf && mkdir -p /metrics /work && chown cf /metrics /work
USER cf
ENTRYPOINT ["python"]
CMD ["-c", "print('connectedfleet app image: pass a module, e.g. -m connectedfleet.normalize.service')"]
