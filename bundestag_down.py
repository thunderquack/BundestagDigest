
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

PRINT_TO_STDOUT = True
WEEK_DAYS = 7
UA = "dip-digest-bot/weekly/1.0"
BASE_URL = "https://search.dip.bundestag.de/api/v1/"
SLEEP_SEC = 0.6
TEXT_DIR = "drucksache_texts"

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass


def api_key() -> str:
    """Read DIP_API_KEY from environment or raise an error."""
    key = os.environ.get("DIP_API_KEY")
    if not key:
        raise RuntimeError("DIP_API_KEY is missing. Put it into .env or environment.")
    return key


def http_get(url: str, headers: dict) -> dict:
    """Perform GET request and decode JSON response."""
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=90) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        return json.loads(resp.read().decode("utf-8"))


def fetch_drucksache_text(drucksache_id: str, key: str) -> dict:
    """Load JSON with full text for a Drucksache."""
    headers = {"Authorization": f"ApiKey {key}",
               "Accept": "application/json", "User-Agent": UA}
    url = urljoin(BASE_URL, f"drucksache-text/{drucksache_id}?format=json")
    return http_get(url, headers)


def _safe_dirname(name: str | None) -> str:
    """Return a filesystem-safe directory name based on drucksachetyp."""
    if not name:
        name = "Unbekannt"
    for ch in ("\\", "/", ":", "*", "?", '"', "<", ">", "|"):
        name = name.replace(ch, "_")
    name = name.strip()
    return name or "Unbekannt"


def save_drucksache_text(entry: dict, key: str, out_dir: str) -> dict:
    """Download JSON text and save to .txt if available; augment entry with paths/errors."""
    os.makedirs(out_dir, exist_ok=True)
    data = fetch_drucksache_text(str(entry["id"]), key)

    text = None
    if isinstance(data, dict):
        t = data.get("text")
        if isinstance(t, str) and t.strip():
            text = t.strip()

    def _safe_filename(name: str) -> str:
        for ch in ("\\", "/", ":", "*", "?", '"', "<", ">", "|"):
            name = name.replace(ch, "_")
        return name.strip()

    # Build filename as "YYYY-MM-DD Nummer"
    raw_date = entry.get("datum")
    if not raw_date and isinstance(data, dict):
        fund = data.get("fundstelle") if isinstance(data.get("fundstelle"), dict) else None
        if isinstance(fund, dict):
            raw_date = fund.get("datum")
    date_str = None
    if isinstance(raw_date, str) and raw_date:
        date_str = raw_date[:10]
        try:
            # Validate ISO date substring
            _ = date.fromisoformat(date_str)
        except Exception:
            date_str = None
    if not date_str:
        date_str = "unknown-date"

    nummer = (entry.get("dokumentnummer") or f"id_{entry['id']}").replace("/", "_")
    base_name = _safe_filename(f"{date_str} {nummer}")

    if text:
        txt_path = os.path.join(out_dir, f"{base_name}.txt")
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(text)
        entry["local_text_path"] = txt_path
        if not entry.get("pdf_url") and isinstance(data, dict):
            fund = data.get("fundstelle")
            if isinstance(fund, dict):
                entry["pdf_url"] = fund.get("pdf_url")
    else:
        entry["local_text_path"] = None
        entry["text_error"] = "no text from drucksache-text API"

    return entry


def fetch_answers(date_start: date, date_end: date, key: str) -> list[dict]:
    """
    Fetch Drucksache entries for the given date range. Uses cursor pagination.
    Returns a list of document dicts from the DIP API.
    """
    headers = {"Authorization": f"ApiKey {key}",
               "Accept": "application/json", "User-Agent": UA}
    base_params = {
        "format": "json",
        "f.datum.start": date_start.strftime("%Y-%m-%d"),
        "f.datum.end": date_end.strftime("%Y-%m-%d"),
    }

    docs: list[dict] = []
    cursor: str | None = None
    last_cursor: str | None = None
    for _ in range(10000):  # generous safety bound
        params = dict(base_params)
        if cursor:
            params["cursor"] = cursor
        url = urljoin(BASE_URL, "drucksache") + "?" + urlencode(params)
        data = http_get(url, headers)
        page_docs = data.get("documents") or []
        if page_docs:
            docs.extend(page_docs)
        last_cursor, cursor = cursor, data.get("cursor")
        if not cursor or cursor == last_cursor:
            break
        time.sleep(SLEEP_SEC)
    return docs


def filter_only_ka_ga(docs: list[dict], key: str) -> list[dict]:
    """Filter only Antworten that relate to Kleine/Grosse Anfrage and normalize fields."""
    out: list[dict] = []
    for d in docs:
        if (d.get("drucksachetyp") or d.get("typ")) != "Antwort":
            continue
        vbez = d.get("vorgangsbezug") or []
        has_ka_ga = False
        for vb in vbez:
            vt = (vb.get("vorgangstyp") or "").lower()
            if "kleine anfrage" in vt or "gro" in vt:  # covers große/grosse
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


def build_md(date_start: date, date_end: date, entries: list[dict]) -> str:
    """Build markdown summary (without local text links)."""
    def group_key(urheber: str | None) -> str:
        if not urheber:
            return "Unbekannt"
        return urheber.strip() or "Unbekannt"

    head = (
        f"# Antworten der Bundesregierung auf Kleine/Grosse Anfragen\n"
        f"## Zeitraum: {date_start.strftime('%Y-%m-%d')} - {date_end.strftime('%Y-%m-%d')}\n\n"
    )
    if not entries:
        return head + "_Keine Eintraege gefunden._\n"

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
    md.append(f"_Anzahl Eintraege: {len(entries)}._\n")
    return "\n".join(md)


def save_texts_grouped_by_type(entries: list[dict], base_out_dir: str, key: str) -> list[dict]:
    """Save texts for all entries into subfolders by drucksachetyp."""
    enriched: list[dict] = []
    for e in tqdm(entries, total=len(entries), desc="Saving texts", unit="doc"):
        try:
            e_copy = dict(e)
            typ = e_copy.get("drucksachetyp") or e_copy.get("typ")
            target_dir = os.path.join(base_out_dir, _safe_dirname(typ))
            enriched.append(save_drucksache_text(e_copy, key, target_dir))
        except Exception as ex:
            e = dict(e)
            e["local_text_path"] = None
            e["text_error"] = str(ex)
            enriched.append(e)
        time.sleep(SLEEP_SEC)
    return enriched


def main() -> None:
    tz = ZoneInfo("Europe/Berlin")
    today = datetime.now(tz).date()
    week_start = today - timedelta(days=170 - 1)
    key = api_key()

    print(f"Fetching Drucksachen from {week_start} to {today}...")
    raw = fetch_answers(week_start, today, key)
    filtered = filter_only_ka_ga(raw, key)
    md = build_md(week_start, today, filtered)

    if PRINT_TO_STDOUT:
        print(md)

    print(f"Done. Filtered entries: {len(filtered)}")

    entries_with_texts = save_texts_grouped_by_type(raw, TEXT_DIR, key)

    print(
        f"Done. Saved texts: {sum(1 for e in entries_with_texts if e.get('local_text_path'))} of {len(entries_with_texts)}"
    )


if __name__ == "__main__":
    main()
