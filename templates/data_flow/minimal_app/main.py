from __future__ import annotations

import json
from pathlib import Path


def main() -> None:
    output = {
        "message": "minimal data flow app scaffold",
        "status": "ok",
    }
    Path("run-output.json").write_text(json.dumps(output, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    print(json.dumps(output, ensure_ascii=True))


if __name__ == "__main__":
    main()
