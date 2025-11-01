from __future__ import annotations

import os
import sys
import subprocess
from typing import Dict, Tuple, List
import time

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


def _unwrap_markdown_fence(text: str) -> str:
    """If text is wrapped in a fenced code block (``` or ```markdown), unwrap it."""
    s = text.strip()
    if s.startswith("```"):
        # remove first line (```[lang]) and last closing fence
        first_nl = s.find("\n")
        if first_nl != -1:
            body_plus = s[first_nl + 1 :]
            last_fence = body_plus.rfind("```")
            if last_fence != -1:
                inner = body_plus[:last_fence]
                return inner.strip()
    return text


def summarize_files(paths: List[str]) -> int:
    if not paths:
        return 0

    if not os.environ.get("OPENAI_API_KEY"):
        print("OPENAI_API_KEY not set. Skipping summarization.")
        return 0

    os.makedirs(REPORTS_DIR, exist_ok=True)
    model = os.environ.get("OPENAI_MODEL") or "gpt-4o-mini"
    try:
        default_max = int(os.environ.get("SUMMARY_MAX_CHARS", "20000"))
    except ValueError:
        default_max = 20000

    count = 0
    # Import once to use its prompt/instructions
    try:
        import openai_summarization as osum  # type: ignore
    except Exception as ex:
        print(f"Cannot import openai_summarization: {ex}. Skipping summarization.")
        return 0

    for p in tqdm(paths, total=len(paths), desc="Summarizing", unit="file"):
        max_chars = default_max
        for attempt in range(4):
            try:
                text = osum.read_text_file(p, max_chars=max_chars)
                md = osum.call_openai_markdown(text, model=model, temperature=0.2)
                md = _unwrap_markdown_fence(md)
                base = os.path.splitext(os.path.basename(p))[0]
                report_path = os.path.join(REPORTS_DIR, f"{base}-Report.md")
                with open(report_path, "w", encoding="utf-8") as f:
                    f.write(md)
                count += 1
                time.sleep(0.6)
                break
            except Exception as ex:
                msg = str(ex).lower()
                too_large = (
                    "rate_limit_exceeded" in msg
                    or "request too large" in msg
                    or "tpm" in msg
                    or "context_length" in msg
                    or "maximum context length" in msg
                )
                if too_large and max_chars > 2500 and attempt < 3:
                    max_chars = max(2500, max_chars // 2)
                    time.sleep(2.0)
                    continue
                print(f"Failed to summarize {p}: {ex}")
                break
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
