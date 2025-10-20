from __future__ import annotations
import os
import sys
import time
import json
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import urlencode, urljoin
import urllib.request
 
from tqdm import tqdm 
from dotenv import load_dotenv

load_dotenv()

PRINT_TO_STDOUT = True         # Печатать краткий дайджест в stdout
WEEK_DAYS = 7                  # Ширина окна по дням (обычно неделя)
UA = "dip-digest-bot/weekly/1.0"
BASE_URL = "https://search.dip.bundestag.de/api/v1/"
SLEEP_SEC = 0.6                # Пауза между запросами к API (сек.)
TEXT_DIR = "drucksache_texts"

# Ensure UTF-8 stdout to safely print German text on Windows consoles
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

# Read API key from environment
def api_key() -> str:
    """Read DIP_API_KEY from environment or raise an error."""
    key = os.environ.get("DIP_API_KEY")
    if not key:
        raise RuntimeError("Не найден DIP_API_KEY в окружении. Загрузите .env или экспортируйте переменную.")
    return key

    # GET JSON helper
def http_get(url: str, headers: dict) -> dict:
    """Perform GET request and decode JSON response."""
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=90) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        return json.loads(resp.read().decode("utf-8"))

# Get single Drucksache text/metadata by id
def fetch_drucksache_text(drucksache_id: str, key: str) -> dict:
    """
    Возвращает JSON с полным текстом документа.
    Структура ответа включает метаданные и поле с текстом документа.
    """
    headers = {"Authorization": f"ApiKey {key}", "Accept": "application/json", "User-Agent": UA}
    url = urljoin(BASE_URL, f"drucksache-text/{drucksache_id}?format=json")
    return http_get(url, headers)

# Save plain text for one entry if available (API or via PDF fallback)
def save_drucksache_text(entry: dict, key: str, out_dir: str) -> dict:
    """
    Скачивает JSON по одному документу и сохраняет .txt только если есть плейнтекст.
    JSON-файл не создаётся. В запись добавляются поля 'local_text_path' или 'text_error'.
    """
    os.makedirs(out_dir, exist_ok=True)
    data = fetch_drucksache_text(str(entry["id"]), key)

    text = None
    if isinstance(data, dict):
        t = data.get("text")
        if isinstance(t, str) and t.strip():
            text = t.strip()

    safe_num = (entry.get("dokumentnummer") or f"id_{entry['id']}").replace("/", "_")
    if text:
        txt_path = os.path.join(out_dir, f"{safe_num}.txt")
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(text)
        entry["local_text_path"] = txt_path
        if not entry.get("pdf_url"):
            if isinstance(data, dict):
                fund = data.get("fundstelle")
                if isinstance(fund, dict):
                    entry["pdf_url"] = fund.get("pdf_url")
    else:
        entry["local_text_path"] = None
        entry["text_error"] = "no text from drucksache-text API"

    return entry

# Fetch answers in date range
def fetch_answers(date_start: date, date_end: date, key: str) -> list[dict]:
    """
    Fetch entries of type 'Antwort' for the given date range.
    Returns a list of document dicts from the DIP API.
    """
    headers = {"Authorization": f"ApiKey {key}", "Accept": "application/json", "User-Agent": UA}
    params = {
        "f.drucksachetyp": "Antwort",
        "format": "json",
        "size": 1000,
        "f.datum.start": date_start.strftime("%Y-%m-%d"),
        "f.datum.end": date_end.strftime("%Y-%m-%d"),
    }
    url = urljoin(BASE_URL, "drucksache") + "?" + urlencode(params)
    data = http_get(url, headers)
    docs = data.get("documents") or []
    next_url = (data.get("links") or {}).get("next")
    while next_url:
        more = http_get(next_url, headers)
        docs.extend(more.get("documents") or [])
        next_url = (more.get("links") or {}).get("next")
    return docs

# Keep only Kleine/Große Anfrage answers and normalize fields
def filter_only_ka_ga(docs: list[dict], key: str) -> list[dict]:
    """
    Filter only Kleine/Große Anfrage answers and normalize fields.
    """
    out: list[dict] = []
    for d in docs:
        # enforce Antwort type, even if server ignored filter
        if (d.get("drucksachetyp") or d.get("typ")) != "Antwort":
            continue
        vbez = d.get("vorgangsbezug") or []
        has_ka_ga = False
        for vb in vbez:
            vt = (vb.get("vorgangstyp") or "").lower()
            if "kleine anfrage" in vt or "große anfrage" in vt or "grosse anfrage" in vt:
                has_ka_ga = True
                break
        if not has_ka_ga:
            continue

        urheber_str = None
        urh = d.get("urheber") or []
        if isinstance(urh, list) and urh:
            first = urh[0]
            urheber_str = first.get("titel") or first.get("bezeichnung") or None

        fund = d.get("fundstelle") or {}
        pdf_url = fund.get("pdf_url")

        out.append({
            "id": d.get("id"),
            "titel": d.get("titel"),
            "dokumentnummer": d.get("dokumentnummer"),
            "drucksachetyp": d.get("drucksachetyp") or d.get("typ"),
            "datum": d.get("datum") or fund.get("datum"),
            "pdf_url": pdf_url,
            "urheber": urheber_str,
        })
    return out

# Build short Markdown digest (no local text links)
def build_md(date_start: date, date_end: date, entries: list[dict]) -> str:
    """
    Build markdown summary (without local text links).
    """
    def group_key(urheber: str | None) -> str:
        if not urheber:
            return "Unbekannt"
        return urheber.strip() or "Unbekannt"

    head = (
        f"# Antworten der Bundesregierung auf Kleine/Große Anfragen\n"
        f"## Zeitraum: {date_start.strftime('%Y-%m-%d')} - {date_end.strftime('%Y-%m-%d')}\n\n"
    )
    if not entries:
        return head + "_Keine Einträge gefunden._\n"

    entries_sorted = sorted(entries, key=lambda e: (group_key(e.get("urheber")), e.get("datum") or "", e.get("dokumentnummer") or ""))
    md: list[str] = [head]
    current = None
    for e in entries_sorted:
        g = group_key(e.get("urheber"))
        if g != current:
            md.append(f"## {g}\n")
            current = g
        line = f"- **{e.get('titel') or 'Ohne Titel'}**"
        if e.get("dokumentnummer"):
            line += f" · BT-Drucksache {e['dokumentnummer']}"
        if e.get("drucksachetyp"):
            line += f" · {e['drucksachetyp']}"
        if e.get("datum"):
            line += f" · {e['datum']}"
        if e.get("pdf_url"):
            line += f" · [PDF]({e['pdf_url']})"
        md.append(line)
    md.append("")
    md.append(f"_Anzahl Einträge: {len(entries)}._\n")
    return "\n".join(md)

def save_texts_for_entries_v2(entries: list[dict], out_dir: str, key: str) -> list[dict]:
    """Идёт по записям и сохраняет .txt только при наличии текста.

    Если текста нет — фиксирует `text_error`. JSON-файлы не создаются.
    """
    enriched: list[dict] = []
    for e in tqdm(entries, total=len(entries), desc="Сохраняю тексты", unit="док"):
        try:
            enriched.append(save_drucksache_text(dict(e), key, out_dir))
        except Exception as ex:
            e = dict(e)
            e["local_text_path"] = None
            e["text_error"] = str(ex)
            enriched.append(e)
        time.sleep(SLEEP_SEC)
    return enriched

# Legacy save_texts_for_entries removed (use save_texts_for_entries_v2)
def build_md_week_with_local_texts(date_start: date, date_end: date, entries: list[dict]) -> str:
    def group_key(urheber: str | None) -> str:
        if not urheber:
            return "Unbekannt"
        return urheber.strip() or "Unbekannt"

    head = (
        f"# Antworten der Bundesregierung auf Kleine/Große Anfragen\n"
        f"## Zeitraum: {date_start.strftime('%Y-%m-%d')} - {date_end.strftime('%Y-%m-%d')}\n\n"
    )
    if not entries:
        return head + "_Ничего не найдено._\n"

    entries_sorted = sorted(entries, key=lambda e: (group_key(e.get("urheber")), e.get("datum") or "", e.get("dokumentnummer") or ""))
    md = [head]
    current = None
    for e in entries_sorted:
        g = group_key(e.get("urheber"))
        if g != current:
            md.append(f"## {g}\n")
            current = g
        line = f"- **{e.get('titel') or 'Без названия'}**"
        if e.get("dokumentnummer"):
            line += f" · BT-Drucksache {e['dokumentnummer']}"
        if e.get("drucksachetyp"):
            line += f" · {e['drucksachetyp']}"
        if e.get("datum"):
            line += f" · {e['datum']}"
        if e.get("pdf_url"):
            line += f" · [PDF]({e['pdf_url']})"
        if e.get("local_text_path"):
            line += f" · [Локальный текст]({e['local_text_path']})"
        elif e.get("text_error"):
            line += f" · Текст не получен: {e['text_error']}"
        md.append(line)
    md.append("")
    md.append(f"_Всего ответов: {len(entries)}._\n")
    return "\n".join(md)


def main() -> None:
    tz = ZoneInfo("Europe/Berlin")
    today = datetime.now(tz).date()
    week_start = today - timedelta(days=WEEK_DAYS - 1)
    key = api_key()

    print(f"Загружаем ответы с {week_start} по {today}...")
    raw = fetch_answers(week_start, today, key)
    filtered = filter_only_ka_ga(raw, key)
    md = build_md(week_start, today, filtered)

    if PRINT_TO_STDOUT:
        print(md)

    print(f"Готово. Ответов: {len(filtered)}")

    entries_with_texts = save_texts_for_entries_v2(filtered, TEXT_DIR, key)

    print(
        f"Готово. Тексты: {sum(1 for e in entries_with_texts if e.get('local_text_path'))} из {len(entries_with_texts)}"
    )


if __name__ == "__main__":
    main()
