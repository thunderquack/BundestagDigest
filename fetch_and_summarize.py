from __future__ import annotations

import os
import sys
import subprocess
import json
from datetime import date
from typing import Dict, Tuple, List, Any
import hashlib
import time

from dotenv import load_dotenv
from tqdm import tqdm


TEXT_DIR = "drucksache_texts"
ANTWORT_DIR = os.path.join(TEXT_DIR, "Antwort")
REPORTS_DIR = os.path.join("reports", "Antwort")
REPORTS_JSON_DIR = os.path.join("reports", "json")
KLEINE_ANFRAGE_DB = os.path.join(REPORTS_JSON_DIR, "kleine-anfrage.json")
KLEINE_ANFRAGE_RECENT = os.path.join(REPORTS_JSON_DIR, "kleine-anfrage-recent.json")


def _sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def snapshot_antwort_txts() -> Dict[str, Tuple[int, str]]:
    out: Dict[str, Tuple[int, str]] = {}
    if not os.path.isdir(ANTWORT_DIR):
        return out
    for name in os.listdir(ANTWORT_DIR):
        if not name.lower().endswith(".txt"):
            continue
        p = os.path.join(ANTWORT_DIR, name)
        try:
            st = os.stat(p)
            # Size helps for quick diffs; hash ensures content-based detection
            file_hash = _sha256_file(p)
            out[p] = (st.st_size, file_hash)
        except OSError:
            continue
    return out


def detect_new_or_changed(prev: Dict[str, Tuple[int, str]], curr: Dict[str, Tuple[int, str]]) -> List[str]:
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


def _md_from_structured(data: Dict[str, Any]) -> str:
    """Render Markdown based on structured JSON fields.
    Expects keys: number, author, date, title, description_ru (preferred).
    Falls back to legacy key `description` if `description_ru` is absent.
    """
    def _v(key: str) -> str:
        val = data.get(key)
        if val is None:
            return "не указано"
        s = str(val).strip()
        return s if s else "не указано"

    lines: List[str] = []
    title_text = _v("title")
    if title_text and title_text != "не указано":
        lines.append(f"# {title_text}")
    lines.append(f"## {_v('author')}")
    lines.append("")
    lines.append(f"__{_v('number')} от {_v('date')}__")
    lines.append("")
    # Prefer Russian description if present; fallback to legacy 'description'
    desc_val = data.get("description_ru")
    if desc_val is None and "description" in data:
        desc_val = data.get("description")
    desc_text = _v("description_ru") if desc_val is not None else _v("description")
    if desc_text and desc_text != "не указано":
        lines.append(desc_text)
    return "\n".join(lines).strip() + "\n"


def summarize_files(paths: List[str]) -> int:
    if not paths:
        return 0

    if not os.environ.get("OPENAI_API_KEY"):
        print("OPENAI_API_KEY not set. Skipping summarization.")
        return 0

    os.makedirs(REPORTS_DIR, exist_ok=True)
    os.makedirs(REPORTS_JSON_DIR, exist_ok=True)

    # Step 1: try to load existing JSON pseudo-DB; if absent — start with empty dict
    db: Dict[str, Any] = {}
    try:
        if os.path.isfile(KLEINE_ANFRAGE_DB):
            with open(KLEINE_ANFRAGE_DB, "r", encoding="utf-8") as fdb:
                db = json.load(fdb) or {}
            if not isinstance(db, dict):
                db = {}
    except Exception:
        # If file is corrupt or unreadable, proceed with a fresh dict
        db = {}
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
                base = os.path.splitext(os.path.basename(p))[0]
                data = osum.call_openai_structured(text, model=model, temperature=0.1)
                md = _md_from_structured(data)
                report_path = os.path.join(REPORTS_DIR, f"{base}-Report.md")
                with open(report_path, "w", encoding="utf-8") as f:
                    f.write(md)
                # Step 2: append structured data into JSON DB keyed by Drucksache number
                # Also add current date field
                key = (str(data.get("number")) if data.get("number") not in (None, "") else base)
                record = dict(data)
                record["saved_date"] = date.today().isoformat()
                db[key] = record
                # Step 3: save DB after each update
                with open(KLEINE_ANFRAGE_DB, "w", encoding="utf-8") as fdb:
                    json.dump(db, fdb, ensure_ascii=False, indent=2)
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


def write_recent_kleineanfrage(limit: int = 25) -> int:
    """Load DB and write last N (by saved_date desc) entries to a separate JSON.
    Returns the number of entries written.
    """
    if not os.path.isfile(KLEINE_ANFRAGE_DB):
        return 0
    try:
        with open(KLEINE_ANFRAGE_DB, "r", encoding="utf-8") as fdb:
            db = json.load(fdb) or {}
        if not isinstance(db, dict):
            return 0
        items = [v for v in db.values() if isinstance(v, dict) and v.get("saved_date")]
        items.sort(key=lambda r: str(r.get("saved_date")), reverse=True)
        recent = items[: max(0, int(limit))]
        os.makedirs(REPORTS_JSON_DIR, exist_ok=True)
        with open(KLEINE_ANFRAGE_RECENT, "w", encoding="utf-8") as fout:
            json.dump(recent, fout, ensure_ascii=False, indent=2)
        return len(recent)
    except Exception:
        return 0


def send_telegram_digest_if_configured() -> int:
    try:
        import send_telegram_digest as tgd  # type: ignore
    except Exception as ex:
        print(f"Cannot import send_telegram_digest: {ex}. Skipping Telegram delivery.")
        return 0

    try:
        return tgd.send_today_digest()
    except Exception as ex:
        print(f"Telegram delivery failed: {ex}")
        return 0


def main(argv: list[str]) -> int:
    load_dotenv()

    before = snapshot_antwort_txts()
    run_fetch()
    after = snapshot_antwort_txts()
    delta = detect_new_or_changed(before, after)
    print(f"New/changed Antwort files: {len(delta)}")
    made = summarize_files(delta)
    print(f"Reports generated: {made} -> {REPORTS_DIR}")
    # After summarization, build a recent list JSON (top 25 by saved_date)
    write_recent_kleineanfrage(25)
    send_telegram_digest_if_configured()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
