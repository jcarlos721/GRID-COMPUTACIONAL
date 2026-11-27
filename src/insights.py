# Geração de arquivos de insights de execução do Grid Computacional.

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path


def save_run_insights(
    output_dir: str | Path,
    elapsed_sec: float,
    tasks_by_worker: dict[str, int],
) -> Path:
    """Salva em JSON o tempo total de execução e a contagem de tasks por worker."""
    target_dir = Path(output_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    output_path = target_dir / f"insights_{timestamp}.json"

    payload = {
        "elapsed_sec": round(elapsed_sec, 3),
        "tasks_by_worker": dict(tasks_by_worker),
    }
    output_path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return output_path
