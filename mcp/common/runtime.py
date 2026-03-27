from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePath, PureWindowsPath
from typing import Any


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def ensure_directory(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_directory(path.parent)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def read_json(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    if not path.exists():
        return {} if default is None else default
    return json.loads(path.read_text(encoding="utf-8"))


def sanitize_name(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "-" for ch in value)


def is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def looks_like_windows_absolute_path(value: str) -> bool:
    return len(value) >= 3 and value[1] == ":" and value[0].isalpha() and value[2] in ("\\", "/")


def resolve_host_repo_root(repo_root: Path) -> Path | PurePath:
    raw_value = os.getenv("HOST_REPO_ROOT")
    if not raw_value:
        return repo_root.resolve()
    cleaned = raw_value.strip()
    if looks_like_windows_absolute_path(cleaned):
        return PureWindowsPath(cleaned)
    return Path(cleaned).expanduser().resolve()


def docker_mount_source(path: Path, repo_root: Path) -> Path | PurePath:
    resolved_path = path.resolve()
    resolved_repo_root = repo_root.resolve()
    if is_relative_to(resolved_path, resolved_repo_root):
        return resolve_host_repo_root(resolved_repo_root) / resolved_path.relative_to(resolved_repo_root)
    return resolved_path


def copy_file(source: Path, destination: Path) -> Path:
    ensure_directory(destination.parent)
    shutil.copy2(source, destination)
    return destination


def append_jsonl(path: Path, payload: dict[str, Any]) -> None:
    ensure_directory(path.parent)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


@dataclass(frozen=True)
class MirrorContext:
    repo_root: Path
    environment: str

    @property
    def compartment_name(self) -> str:
        override = os.getenv("OCI_MEDALLION_MIRROR_COMPARTMENT_NAME", "").strip()
        if override:
            return sanitize_name(override)
        return f"compartment-data-medallion-{self.environment}"

    @property
    def compartment_root(self) -> Path:
        return self.repo_root / "workspace" / "oci-mirror" / self.environment / self.compartment_name

    def service_root(self, service_name: str) -> Path:
        return ensure_directory(self.compartment_root / service_name)

    def bucket_root(self, bucket_name: str) -> Path:
        return ensure_directory(self.compartment_root / "buckets" / sanitize_name(bucket_name))

    def report(self, service_name: str, operation: str, payload: dict[str, Any]) -> Path:
        report_path = self.compartment_root / "reports" / f"{utc_timestamp()}-{sanitize_name(service_name)}-{sanitize_name(operation)}.json"
        write_json(report_path, payload)
        return report_path
