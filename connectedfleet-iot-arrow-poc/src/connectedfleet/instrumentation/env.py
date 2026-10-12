"""Capture the facts every run manifest needs: code version, host and library versions."""

from __future__ import annotations

import json
import os
import platform
import resource
import subprocess
import sys
from pathlib import Path

import pyarrow as pa


def git_state(repo: Path) -> dict:
    """POC commit and whether the working tree had uncommitted changes."""

    def _git(*args: str) -> str | None:
        try:
            result = subprocess.run(
                ["git", "-C", str(repo), *args], capture_output=True, text=True, check=True, timeout=10
            )
        except (OSError, subprocess.SubprocessError):
            return None
        return result.stdout.strip()

    sha = _git("rev-parse", "HEAD")
    status = _git("status", "--porcelain")
    return {
        "git_sha": sha,
        "git_dirty": None if status is None else bool(status),
    }



def image_info(image: str) -> dict | None:
    """Id and build time of a local Docker image, so a run records which build of the worker code it used."""
    try:
        out = subprocess.run(["docker", "image", "inspect", image], capture_output=True, text=True, check=True,
                             timeout=20)
        meta = json.loads(out.stdout)[0]
    except (OSError, subprocess.SubprocessError, ValueError, IndexError, KeyError):
        return None
    return {"image": image, "id": meta.get("Id"), "created": meta.get("Created")}

def total_memory_bytes() -> int | None:
    if sys.platform == "darwin":
        try:
            out = subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True, check=True)
            return int(out.stdout.strip())
        except (OSError, subprocess.SubprocessError, ValueError):
            return None
    try:
        with open("/proc/meminfo", encoding="ascii") as meminfo:
            for line in meminfo:
                if line.startswith("MemTotal:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        return None
    return None


def host_info() -> dict:
    return {
        "os": platform.platform(),
        "architecture": platform.machine(),
        "processor": platform.processor() or None,
        "cpu_count": os.cpu_count(),
        "ram_bytes": total_memory_bytes(),
        "in_container": Path("/.dockerenv").exists(),
    }


def versions() -> dict:
    return {
        "python": platform.python_version(),
        "python_implementation": platform.python_implementation(),
        "pyarrow": pa.__version__,
        "arrow_cpp": pa.cpp_build_info.version,
        "arrow_memory_pool": pa.default_memory_pool().backend_name,
    }


def peak_rss_bytes() -> int:
    """Process high-water resident set size (ru_maxrss is bytes on macOS, KiB on Linux)."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak if sys.platform == "darwin" else peak * 1024
