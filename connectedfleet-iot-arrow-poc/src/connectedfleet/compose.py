"""Small wrapper over `docker compose` for lab orchestration (host side)."""

from __future__ import annotations

import subprocess

from connectedfleet.config import POC_ROOT

PROFILES = ("--profile", "pipeline", "--profile", "lanes", "--profile", "tools")


class ComposeError(Exception):
    pass


def run(*args: str, check: bool = True, timeout: float = 600.0, capture: bool = True) -> subprocess.CompletedProcess:
    cmd = ["docker", "compose", *PROFILES, *args]
    proc = subprocess.run(cmd, cwd=POC_ROOT, capture_output=capture, text=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise ComposeError(f"{' '.join(cmd)} -> exit {proc.returncode}: {(proc.stderr or '')[-800:]}")
    return proc


def up(*services: str) -> None:
    run("up", "-d", *services)


def stop(*services: str) -> None:
    run("stop", *services)


def container_id(service: str) -> str | None:
    out = run("ps", "-q", service, check=False).stdout.strip()
    return out.splitlines()[0] if out else None


def run_once(service: str, *command: str, env: dict[str, str] | None = None, timeout: float = 600.0) -> subprocess.CompletedProcess:
    """`docker compose run --rm` a one-off container; returns the completed process (exit code is not checked)."""
    env_args: list[str] = []
    for key, value in (env or {}).items():
        env_args += ["-e", f"{key}={value}"]
    return run("run", "--rm", *env_args, service, *command, check=False, timeout=timeout)
