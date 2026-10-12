"""Connection settings from the environment (`.env` is loaded by Compose and by `ops`).

Two network views exist:
  * inside the Compose network (the `app` container): service names, e.g. kafka:19092
  * on the host (lab orchestrators via `uv run`): published ports on localhost
Defaults are the host view; compose.yaml overrides them for containers.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def load_dotenv(path: Path) -> None:
    """Minimal `.env` loader: KEY=VALUE lines, `#` comments; never overrides existing variables."""
    if not path.is_file():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    connect_url: str
    clickhouse_http_url: str
    clickhouse_user: str
    clickhouse_password: str
    postgres_dsn: str
    mongo_uri: str

    @classmethod
    def from_env(cls) -> Settings:
        env = os.environ
        pg_user = env.get("POSTGRES_USER", "connectedfleet")
        pg_password = env.get("POSTGRES_PASSWORD", "change-me-local-only")
        pg_db = env.get("POSTGRES_DB", "connectedfleet")
        pg_host = env.get("POSTGRES_HOST", "localhost")
        pg_port = env.get("POSTGRES_PORT", "5432")
        mongo_user = env.get("MONGO_APP_USER", "cf_app")
        mongo_password = env.get("MONGO_APP_PASSWORD", "change-me-local-only")
        mongo_host = env.get("MONGO_HOST", "localhost:27017")
        # From the host the replica-set member name ("mongodb:27017") is not resolvable,
        # so host-side clients connect directly to the single member.
        mongo_opts = env.get("MONGO_URI_OPTIONS", "directConnection=true&authSource=admin")
        return cls(
            kafka_bootstrap=env.get("KAFKA_BOOTSTRAP", "localhost:9092"),
            connect_url=env.get("CONNECT_URL", "http://localhost:8083"),
            clickhouse_http_url=env.get("CLICKHOUSE_HTTP_URL", "http://localhost:8123"),
            clickhouse_user=env.get("CLICKHOUSE_USER", "connectedfleet"),
            clickhouse_password=env.get("CLICKHOUSE_PASSWORD", "change-me-local-only"),
            postgres_dsn=env.get(
                "POSTGRES_DSN", f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
            ),
            mongo_uri=env.get("MONGO_URI", f"mongodb://{mongo_user}:{mongo_password}@{mongo_host}/?{mongo_opts}"),
        )


POC_ROOT = Path(__file__).resolve().parents[2]
