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


RUN_LOG_REDACT_TOKENS = ("password", "secret", "token", "private_key")
RUN_LOG_MAX_ITEMS = 8
RUN_LOG_MAX_TEXT_CHARS = 640


def current_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _truncate_text(value: str, limit: int = RUN_LOG_MAX_TEXT_CHARS) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def _sanitize_run_log_value(value: Any, field_name: str | None = None) -> Any:
    normalized_field = (field_name or "").strip().lower()
    if any(token in normalized_field for token in RUN_LOG_REDACT_TOKENS):
        return "<redacted>"

    if isinstance(value, (PurePath, PureWindowsPath)):
        return str(value)
    if isinstance(value, dict):
        items = list(value.items())
        sanitized: dict[str, Any] = {}
        for index, (child_key, child_value) in enumerate(items):
            if index >= RUN_LOG_MAX_ITEMS:
                sanitized["_truncated_items"] = len(items) - RUN_LOG_MAX_ITEMS
                break
            sanitized[str(child_key)] = _sanitize_run_log_value(child_value, str(child_key))
        return sanitized
    if isinstance(value, (list, tuple, set)):
        sequence = list(value)
        sanitized_items = [_sanitize_run_log_value(item) for item in sequence[:RUN_LOG_MAX_ITEMS]]
        if len(sequence) > RUN_LOG_MAX_ITEMS:
            sanitized_items.append(f"... ({len(sequence) - RUN_LOG_MAX_ITEMS} more)")
        return sanitized_items
    if isinstance(value, str):
        return _truncate_text(value)
    return value


def _default_run_log_fields() -> dict[str, str]:
    defaults: dict[str, str] = {}
    for env_name, field_name in (
        ("OCI_MEDALLION_DEPLOYMENT_ID", "deployment_id"),
        ("OCI_MEDALLION_DEPLOYMENT_PROJECT_ID", "project_id"),
        ("OCI_MEDALLION_DEPLOYMENT_RUN_ID", "run_id"),
        ("OCI_MEDALLION_DEPLOYMENT_ENVIRONMENT", "environment"),
        ("OCI_MEDALLION_DEPLOYMENT_TAG", "tag"),
    ):
        value = os.getenv(env_name, "").strip()
        if value:
            defaults[field_name] = value
    return defaults


def mirror_root(repo_root: Path) -> Path:
    return ensure_directory(repo_root / "workspace" / "oci-mirror")


def mirror_run_log_paths(repo_root: Path, environment: str | None = None, compartment_name: str | None = None) -> tuple[Path, ...]:
    root = mirror_root(repo_root)
    paths: list[Path] = [root / "RUN.log"]
    if environment:
        env_root = ensure_directory(root / sanitize_name(environment))
        paths.append(env_root / "RUN.log")
        if compartment_name:
            compartment_root = ensure_directory(env_root / sanitize_name(compartment_name))
            paths.append(compartment_root / "RUN.log")

    unique: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        key = str(path.resolve()).lower() if path.exists() else str(path).lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return tuple(unique)


def append_run_log(paths: Path | list[Path] | tuple[Path, ...], event: str, fields: dict[str, Any] | None = None) -> None:
    targets = (paths,) if isinstance(paths, Path) else tuple(paths)
    payload = _default_run_log_fields()
    if fields:
        payload.update(fields)

    parts = [current_utc_iso(), event]
    for key, raw_value in payload.items():
        safe_value = _sanitize_run_log_value(raw_value, str(key))
        if safe_value in (None, "", [], {}):
            continue
        parts.append(f"{sanitize_name(str(key))}={json.dumps(safe_value, ensure_ascii=True)}")
    line = " | ".join(parts)

    for path in targets:
        ensure_directory(path.parent)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")


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


def append_context_run_log(context: MirrorContext, event: str, fields: dict[str, Any] | None = None) -> None:
    append_run_log(mirror_run_log_paths(context.repo_root, context.environment, context.compartment_name), event, fields)
