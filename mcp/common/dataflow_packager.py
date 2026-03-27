from __future__ import annotations

import platform
import shutil
import subprocess
import zipfile
from pathlib import Path
from typing import Any

from .runtime import MirrorContext, append_jsonl, docker_mount_source, ensure_directory, sanitize_name, utc_timestamp, write_json


AMD64_IMAGE = "phx.ocir.io/axmemlgtri2a/dataflow/dependency-packager-linux_x86_64:latest"
ARM64_IMAGE = "phx.ocir.io/axmemlgtri2a/dataflow/dependency-packager-linux_arm64_v8:latest"


def detect_platform() -> str:
    machine = platform.machine().lower()
    if machine in {"arm64", "aarch64"}:
        return "arm64"
    return "amd64"


def default_packager_image(target_platform: str) -> str:
    if target_platform == "arm64":
        return ARM64_IMAGE
    return AMD64_IMAGE


def default_docker_platform(target_platform: str) -> str:
    if target_platform == "arm64":
        return "linux/arm64"
    return "linux/amd64"


def build_packager_command(
    repo_root: Path,
    dependency_root: Path,
    python_version: str,
    image: str,
    target_platform: str,
    validate_only: bool,
    archive_name: str,
) -> list[str]:
    dependency_mount_source = docker_mount_source(dependency_root, repo_root)
    command = [
        "docker",
        "run",
        "--platform",
        default_docker_platform(target_platform),
        "--rm",
        "--user",
        "0:0",
        "-v",
        f"{dependency_mount_source}:/opt/dataflow",
        "--pull",
        "always",
        "-i",
        image,
        "-p",
        python_version,
    ]
    if validate_only:
        command.extend(["--validate", archive_name])
    return command


def build_fallback_archive(dependency_root: Path, archive_path: Path) -> None:
    version_file = dependency_root / "version.txt"
    if not version_file.exists():
        version_file.write_text(
            "\n".join(
                [
                    "dependency_packager=factory-fallback",
                    f"generated_at_utc={utc_timestamp()}",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
    if archive_path.exists():
        archive_path.unlink()
    with zipfile.ZipFile(archive_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for item in sorted(dependency_root.rglob("*")):
            if not item.is_file():
                continue
            if item == archive_path or item.name == ".gitkeep":
                continue
            relative = item.relative_to(dependency_root)
            relative_str = str(relative).replace("\\", "/")
            if relative_str == "version.txt" or relative_str.startswith("python/") or relative_str.startswith("java/"):
                archive.write(item, relative)


def expected_archive_jar_members(dependency_root: Path) -> set[str]:
    return {
        str(item.relative_to(dependency_root)).replace("\\", "/")
        for item in dependency_root.rglob("*.jar")
        if item.is_file()
    }


def archive_contains_required_jars(archive_path: Path, required_members: set[str]) -> bool:
    if not required_members:
        return True
    with zipfile.ZipFile(archive_path, "r") as archive:
        return required_members.issubset(set(archive.namelist()))


def package_dependency_archive(
    context: MirrorContext,
    application_name: str,
    dependency_root: Path,
    python_version: str = "3.11",
    image: str | None = None,
    target_platform: str | None = None,
    archive_name: str = "archive.zip",
    validate_after_build: bool = True,
) -> dict[str, Any]:
    dependency_root = dependency_root.resolve()
    if not dependency_root.exists():
        raise FileNotFoundError(f"No existe el directorio de dependencias: {dependency_root}")
    archive_path = dependency_root / archive_name
    if archive_path.exists():
        archive_path.unlink()

    target_platform = target_platform or detect_platform()
    image = image or default_packager_image(target_platform)

    package_command = build_packager_command(
        dependency_root=dependency_root,
        repo_root=context.repo_root,
        python_version=python_version,
        image=image,
        target_platform=target_platform,
        validate_only=False,
        archive_name=archive_name,
    )
    package_result = subprocess.run(package_command, capture_output=True, text=True, check=False)
    if package_result.returncode != 0:
        raise RuntimeError(package_result.stderr or package_result.stdout)

    archive_mode = "packager"
    if not archive_path.exists():
        build_fallback_archive(dependency_root, archive_path)
        archive_mode = "fallback_local_zip"
    required_jar_members = expected_archive_jar_members(dependency_root)
    if archive_path.exists() and not archive_contains_required_jars(archive_path, required_jar_members):
        build_fallback_archive(dependency_root, archive_path)
        archive_mode = "fallback_local_zip_missing_jars"
    if not archive_path.exists():
        raise FileNotFoundError(f"El packager no genero {archive_name} en {dependency_root}")

    validate_result = None
    validate_command = None
    if validate_after_build:
        validate_command = build_packager_command(
            dependency_root=dependency_root,
            repo_root=context.repo_root,
            python_version=python_version,
            image=image,
            target_platform=target_platform,
            validate_only=True,
            archive_name=archive_name,
        )
        validate_result = subprocess.run(validate_command, capture_output=True, text=True, check=False)
        if validate_result.returncode != 0:
            raise RuntimeError(validate_result.stderr or validate_result.stdout)

    service_root = ensure_directory(context.service_root("data_flow") / sanitize_name(application_name))
    dependency_dir = ensure_directory(service_root / "dependency")
    mirrored_archive = dependency_dir / archive_name
    shutil.copy2(archive_path, mirrored_archive)

    payload: dict[str, Any] = {
        "application_name": application_name,
        "dependency_root": str(dependency_root),
        "archive_name": archive_name,
        "archive_path": str(archive_path),
        "mirrored_archive": str(mirrored_archive),
        "python_version": python_version,
        "image": image,
        "target_platform": target_platform,
        "archive_mode": archive_mode,
        "created_at_utc": utc_timestamp(),
        "package_command": package_command,
        "package_stdout": package_result.stdout,
        "package_stderr": package_result.stderr,
    }
    if validate_command is not None and validate_result is not None:
        payload["validate_command"] = validate_command
        payload["validate_stdout"] = validate_result.stdout
        payload["validate_stderr"] = validate_result.stderr

    manifest_path = dependency_dir / "dependency-archive.manifest.json"
    write_json(manifest_path, payload)
    append_jsonl(service_root / "operations.log.jsonl", {"operation": "package_dependency_archive", **payload})
    context.report("data_flow", "package-dependency-archive", {"application_name": application_name, "archive": str(mirrored_archive), "image": image})
    return {"manifest_path": str(manifest_path), "archive_path": str(mirrored_archive)}


def validate_dependency_archive(
    dependency_root: Path,
    repo_root: Path,
    python_version: str = "3.11",
    image: str | None = None,
    target_platform: str | None = None,
    archive_name: str = "archive.zip",
) -> dict[str, Any]:
    dependency_root = dependency_root.resolve()
    target_platform = target_platform or detect_platform()
    image = image or default_packager_image(target_platform)
    validate_command = build_packager_command(
        dependency_root=dependency_root,
        repo_root=repo_root,
        python_version=python_version,
        image=image,
        target_platform=target_platform,
        validate_only=True,
        archive_name=archive_name,
    )
    validate_result = subprocess.run(validate_command, capture_output=True, text=True, check=False)
    if validate_result.returncode != 0:
        raise RuntimeError(validate_result.stderr or validate_result.stdout)
    return {
        "archive_path": str(dependency_root / archive_name),
        "validate_command": validate_command,
        "validate_stdout": validate_result.stdout,
        "validate_stderr": validate_result.stderr,
    }
