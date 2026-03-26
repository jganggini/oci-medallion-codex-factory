from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .runtime import MirrorContext, ensure_directory, sanitize_name, utc_timestamp, write_json


OCI_IMAGE = "ghcr.io/oracle/oci-cli:latest"


@dataclass(frozen=True)
class OciExecutionContext:
    repo_root: Path
    profile: str | None = None
    use_docker: bool = True

    @property
    def host_oci_dir(self) -> Path:
        return self.repo_root / ".local" / "oci"

    @property
    def host_config_file(self) -> Path:
        return self.host_oci_dir / "config"

    def container_repo_root(self) -> str:
        return "/workspace"

    def container_oci_dir(self) -> str:
        return "/mnt/oci"

    def host_to_container_path(self, path: Path) -> str:
        return str(path.resolve()).replace(str(self.repo_root.resolve()), self.container_repo_root()).replace("\\", "/")


def _quoted_shell(value: str) -> str:
    if value == "":
        return "''"
    safe = value.replace("'", "'\"'\"'")
    return f"'{safe}'"


def build_oci_command(execution: OciExecutionContext, args: list[str]) -> list[str]:
    if not execution.use_docker:
        return ["oci", *args]

    command = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{execution.repo_root}:/workspace",
        "-v",
        f"{execution.host_oci_dir}:/mnt/oci",
        "-w",
        execution.container_repo_root(),
        "-e",
        f"OCI_CLI_CONFIG_FILE={execution.container_oci_dir()}/config",
    ]
    if execution.profile:
        command.extend(["-e", f"OCI_CLI_PROFILE={execution.profile}"])
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

    result = subprocess.run(command, capture_output=True, text=True, check=False)
    payload["return_code"] = result.returncode
    payload["stdout"] = result.stdout
    payload["stderr"] = result.stderr
    result_path = plan_dir / f"{utc_timestamp()}-{sanitize_name(operation)}-result.json"
    write_json(result_path, payload)
    if result.returncode != 0:
        raise RuntimeError(json.dumps({"message": "OCI CLI command failed", "result_path": str(result_path)}, ensure_ascii=True))
    payload["result_path"] = str(result_path)
    return payload
