# .codex

This folder is intentionally lightweight.

Purpose:

- store repository-level Codex helper notes or rules
- keep project-scoped Codex MCP registration separate from the user's global config

Important:

- project workflow skills live in `skills/`
- user-global Codex configuration still lives outside the repo
- `setup-dev.ps1` y `setup-dev.sh` sincronizan `.codex/config.template.toml` hacia `.codex/config.toml`
- `.codex/config.toml` es local al proyecto, se mantiene ignorado por Git y debe contener solo los MCP del factory
- el bridge MCP requiere un launcher Python en host (`py`, `python` o `python3`); el resto de runtimes sigue usando Docker
- `.codex/factory_mcp_bridge.py` exposes the runtimes under `mcp/servers/` to Codex without copying personal profiles, auth, or model settings into the repo
- si `/mcp` muestra `No MCP servers configured`, normalmente significa que abriste la carpeta equivocada o que Codex aun no recargo este repo despues de `setup-dev`
