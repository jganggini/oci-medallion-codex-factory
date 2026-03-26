from __future__ import annotations

import configparser
import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any

from .runtime import MirrorContext, ensure_directory, sanitize_name, utc_timestamp, write_json


OCI_IMAGE = "ghcr.io/oracle/oci-cli:latest"
CONTAINER_REPO_ROOT = "/workspace"
CONTAINER_OCI_DIR = "/mnt/oci"
CONTAINER_EXTRA_ROOT = "/mnt/extra"


def _is_relative_to(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _to_posix(root: str, relative: Path) -> str:
    parts = [part for part in relative.parts if part not in ("", ".")]
    return str(PurePosixPath(root, *parts))


def _quoted_shell(value: str) -> str:
    if value == "":
        return "''"
    safe = value.replace("'", "'\"'\"'")
    return f"'{safe}'"


def _resolve_config_artifact(raw_value: str, source_root: Path) -> Path | None:
    cleaned = os.path.expandvars(raw_value.strip().strip("'\""))
    if not cleaned:
        return None

    candidate = Path(cleaned).expanduser()
    if candidate.exists():
        return candidate.resolve()

    if not candidate.is_absolute():
        relative_candidate = (source_root / candidate).resolve()
        if relative_candidate.exists():
            return relative_candidate

    fallback = (source_root / candidate.name).resolve()
    if fallback.exists():
        return fallback
    return None


def _prepare_host_oci_dir(execution: "OciExecutionContext") -> Path:
    source_root = execution.host_oci_dir.resolve()
    if not source_root.exists():
        raise FileNotFoundError(f"No existe la configuracion OCI en {source_root}")

    temp_root = ensure_directory(execution.repo_root / ".tmp_oci_cli")
    prepared_root = Path(tempfile.mkdtemp(prefix="oci-cli-", dir=temp_root))
    shutil.copytree(source_root, prepared_root, dirs_exist_ok=True)

    config_path = prepared_root / "config"
    if not config_path.exists():
        raise FileNotFoundError(f"No existe el archivo OCI config en {config_path}")

    parser = configparser.RawConfigParser()
    parser.optionxform = str
    parser.read(config_path, encoding="utf-8")

    for section in parser.sections():
        for option in ("key_file", "security_token_file"):
            if not parser.has_option(section, option):
                continue

            artifact = _resolve_config_artifact(parser.get(section, option), source_root)
            if artifact is None:
                continue

            target = prepared_root / artifact.name
            if artifact.resolve() != target.resolve():
                shutil.copy2(artifact, target)
            parser.set(section, option, f"{CONTAINER_OCI_DIR}/{target.name}")

    with config_path.open("w", encoding="utf-8") as handle:
        parser.write(handle)

    return prepared_root


@dataclass(frozen=True)
class OciExecutionContext:
    repo_root: Path
    profile: str | None = None
    use_docker: bool = True
    extra_mounts: tuple[Path, ...] = ()

    @property
    def host_oci_dir(self) -> Path:
        return self.repo_root / ".local" / "oci"

    @property
    def host_config_file(self) -> Path:
        return self.host_oci_dir / "config"

    def container_repo_root(self) -> str:
        return CONTAINER_REPO_ROOT

    def container_oci_dir(self) -> str:
        return CONTAINER_OCI_DIR

    def container_extra_dir(self, index: int) -> str:
        return f"{CONTAINER_EXTRA_ROOT}/{index}"

    def normalized_extra_mounts(self) -> tuple[Path, ...]:
        mounts: list[Path] = []
        seen: set[str] = set()
        repo_root = self.repo_root.resolve()
        host_oci_dir = self.host_oci_dir.resolve()

        for item in self.extra_mounts:
            resolved = Path(item).resolve()
            if _is_relative_to(resolved, repo_root) or _is_relative_to(resolved, host_oci_dir):
                continue
            key = str(resolved).lower()
            if key in seen:
                continue
            seen.add(key)
            mounts.append(resolved)
        return tuple(mounts)

    def host_to_container_path(self, path: Path) -> str:
        resolved = path.resolve()
        repo_root = self.repo_root.resolve()
        if _is_relative_to(resolved, repo_root):
            return _to_posix(self.container_repo_root(), resolved.relative_to(repo_root))

        host_oci_dir = self.host_oci_dir.resolve()
        if _is_relative_to(resolved, host_oci_dir):
            return _to_posix(self.container_oci_dir(), resolved.relative_to(host_oci_dir))

        for index, mount_root in enumerate(self.normalized_extra_mounts()):
            if _is_relative_to(resolved, mount_root):
                return _to_posix(self.container_extra_dir(index), resolved.relative_to(mount_root))

        raise ValueError(f"La ruta {resolved} no pertenece al repo ni a los mounts extra configurados")


def build_oci_command(execution: OciExecutionContext, args: list[str], host_oci_dir: Path | None = None) -> list[str]:
    if not execution.use_docker:
        return ["oci", *args]

    oci_dir = (host_oci_dir or execution.host_oci_dir).resolve()
    command = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{execution.repo_root.resolve()}:{execution.container_repo_root()}",
        "-v",
        f"{oci_dir}:{execution.container_oci_dir()}",
        "-w",
        execution.container_repo_root(),
        "-e",
        f"OCI_CLI_CONFIG_FILE={execution.container_oci_dir()}/config",
        "-e",
        "OCI_CLI_SUPPRESS_FILE_PERMISSIONS_WARNING=True",
    ]
    if execution.profile:
        command.extend(["-e", f"OCI_CLI_PROFILE={execution.profile}"])
    for index, mount_root in enumerate(execution.normalized_extra_mounts()):
        command.extend(["-v", f"{mount_root}:{execution.container_extra_dir(index)}"])
    command.extend([OCI_IMAGE, *args])
    return command


def execute_oci(
    execution: OciExecutionContext,
    service_name: str,
    context: MirrorContext,
    operation: str,
    args: list[str],
    apply_mode: str,
) -> dict[str, Any]:
    command = build_oci_command(execution, args)
    payload: dict[str, Any] = {
        "service": service_name,
        "operation": operation,
        "apply_mode": apply_mode,
        "command": command,
        "command_shell": " ".join(_quoted_shell(item) for item in command),
        "created_at_utc": utc_timestamp(),
    }

    plan_dir = ensure_directory(context.service_root(service_name) / "oci-plans")
    plan_path = plan_dir / f"{utc_timestamp()}-{sanitize_name(operation)}.json"
    write_json(plan_path, payload)
    context.report(service_name, f"oci-{operation}-{apply_mode}", payload)

    if apply_mode == "plan":
        payload["plan_path"] = str(plan_path)
        return payload

    env = None
    prepared_oci_dir: Path | None = None
    if execution.use_docker:
        prepared_oci_dir = _prepare_host_oci_dir(execution)
        command = build_oci_command(execution, args, host_oci_dir=prepared_oci_dir)
    else:
        env = os.environ.copy()
        env["OCI_CLI_CONFIG_FILE"] = str(execution.host_config_file)
        env["OCI_CLI_SUPPRESS_FILE_PERMISSIONS_WARNING"] = "True"
        if execution.profile:
            env["OCI_CLI_PROFILE"] = execution.profile

    try:
        result = subprocess.run(command, capture_output=True, text=True, check=False, env=env)
    finally:
        if prepared_oci_dir is not None:
            shutil.rmtree(prepared_oci_dir, ignore_errors=True)

    payload["return_code"] = result.returncode
    payload["stdout"] = result.stdout
    payload["stderr"] = result.stderr
    result_path = plan_dir / f"{utc_timestamp()}-{sanitize_name(operation)}-result.json"
    write_json(result_path, payload)
    if result.returncode != 0:
        raise RuntimeError(json.dumps({"message": "OCI CLI command failed", "result_path": str(result_path)}, ensure_ascii=True))
    payload["result_path"] = str(result_path)
    return payload
