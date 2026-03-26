from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
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
