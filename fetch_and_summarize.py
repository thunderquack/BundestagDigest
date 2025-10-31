from __future__ import annotations

import os
import sys
import subprocess
from typing import Dict, Tuple, List

from dotenv import load_dotenv
from tqdm import tqdm


TEXT_DIR = "drucksache_texts"
ANTWORT_DIR = os.path.join(TEXT_DIR, "Antwort")
REPORTS_DIR = os.path.join("reports", "Antwort")


def snapshot_antwort_txts() -> Dict[str, Tuple[int, float]]:
    out: Dict[str, Tuple[int, float]] = {}
    if not os.path.isdir(ANTWORT_DIR):
        return out
    for name in os.listdir(ANTWORT_DIR):
        if not name.lower().endswith(".txt"):
            continue
        p = os.path.join(ANTWORT_DIR, name)
        try:
            st = os.stat(p)
            out[p] = (st.st_size, st.st_mtime)
        except OSError:
            continue
    return out


def detect_new_or_changed(prev: Dict[str, Tuple[int, float]], curr: Dict[str, Tuple[int, float]]) -> List[str]:
    out: List[str] = []
    for p, meta in curr.items():
        if p not in prev or prev[p] != meta:
            out.append(p)
    return sorted(out)


def run_fetch() -> None:
    # Запускаем существующий загрузчик как отдельный процесс — сохраняем логику без изменений
    cmd = [sys.executable, "bundestag_down.py"]
    subprocess.run(cmd, check=True)


def summarize_files(paths: List[str]) -> int:
    if not paths:
        return 0

    if not os.environ.get("OPENAI_API_KEY"):
        print("OPENAI_API_KEY not set. Skipping summarization.")
        return 0

    os.makedirs(REPORTS_DIR, exist_ok=True)
    model_env = os.environ.get("OPENAI_MODEL")

    count = 0
    for p in tqdm(paths, total=len(paths), desc="Summarizing", unit="file"):
        try:
            base = os.path.splitext(os.path.basename(p))[0]
            report_path = os.path.join(REPORTS_DIR, f"{base}-Report.md")

            # Run the summarizer CLI so its own prompt/instructions are used
            cmd = [sys.executable, "openai_summarization.py", p]
            if model_env:
                cmd += ["--model", model_env]

            res = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                env=os.environ.copy(),
            )
            if res.returncode != 0:
                stderr = (res.stderr or "").strip()
                print(f"Summarizer failed for {p}: rc={res.returncode} {stderr}")
                continue
            output = (res.stdout or "").strip()
            if not output:
                print(f"Summarizer produced empty output for {p}")
                continue
            with open(report_path, "w", encoding="utf-8") as f:
                f.write(output)
            count += 1
        except Exception as ex:
            print(f"Failed to summarize {p}: {ex}")
    return count


def main(argv: list[str]) -> int:
    load_dotenv()

    before = snapshot_antwort_txts()
    run_fetch()
    after = snapshot_antwort_txts()
    delta = detect_new_or_changed(before, after)
    print(f"New/changed Antwort files: {len(delta)}")
    made = summarize_files(delta)
    print(f"Reports generated: {made} -> {REPORTS_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
