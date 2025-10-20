import os
import sys
import time
import json
import argparse
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from urllib.parse import urlencode, urljoin
import urllib.request
from dotenv import load_dotenv

load_dotenv()

PRINT_TO_STDOUT = True         # печатать итоговый markdown в вывод ячейки
WEEK_DAYS = 7                  # последняя неделя = 7 дней включая сегодня
UA = "dip-digest-bot/weekly/1.0"
BASE_URL = "https://search.dip.bundestag.de/api/v1/"
SLEEP_SEC = 0.6                # вежливая пауза между страницами
TEXT_DIR = "drucksache_texts_week"

def api_key() -> str:
    key = os.environ.get("DIP_API_KEY")
    if not key:
        raise RuntimeError("Не найден DIP_API_KEY в окружении. Загрузите .env или экспортируйте переменную.")
    return key

def http_get(url: str, headers: dict) -> dict:
    req = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=90) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status} for {url}")
        return json.loads(resp.read().decode("utf-8"))

def fetch_drucksache_text(drucksache_id: str, key: str) -> dict:
    """
    Возвращает JSON с полным текстом документа.
    Структура ответа включает метаданные и поле с текстом документа.
    """
    headers = {"Authorization": f"ApiKey {key}", "Accept": "application/json", "User-Agent": UA}
    url = urljoin(BASE_URL, f"drucksache-text/{drucksache_id}?format=json")
    return http_get(url, headers)

def save_texts_for_entries(entries: list[dict], out_dir: str, key: str) -> list[dict]:
    """
    Для каждой записи тянет полный текст и сохраняет в файл.
    Возвращает список записей, дополненный полем 'local_text_path'.
    """
    os.makedirs(out_dir, exist_ok=True)
    enriched = []
    for e in entries:
        ds_id = str(e["id"])
        try:
            data = fetch_drucksache_text(ds_id, key)
            # В JSON-ответе есть поле 'text' или аналогичное с полным телом. Сохраним плейнтекстовую версию.
            # Если текст вложен глубже, можно адаптировать, оставим универсально:
            # попробуем распространенные варианты: data.get("text"), data.get("dokumenttext"), data["drucksacheText"]["text"]
            txt = None
            if isinstance(data, dict):
                if "text" in data and isinstance(data["text"], str):
                    txt = data["text"]
                elif "dokumenttext" in data and isinstance(data["dokumenttext"], str):
                    txt = data["dokumenttext"]
                elif "drucksacheText" in data and isinstance(data["drucksacheText"], dict) and isinstance(data["drucksacheText"].get("text"), str):
                    txt = data["drucksacheText"]["text"]
            if not txt:
                # на всякий случай приведем весь JSON для диагностики
                raise ValueError("no text field in JSON")

            safe_num = (e.get("dokumentnummer") or f"id_{ds_id}").replace("/", "_")
            fname = f"{safe_num}.txt"
            fpath = os.path.join(out_dir, fname)
            with open(fpath, "w", encoding="utf-8") as f:
                f.write(txt)
            e = dict(e)
            e["local_text_path"] = fpath
            enriched.append(e)
        except Exception as ex:
            # не падаем на одном документе
            e = dict(e)
            e["local_text_path"] = None
            e["text_error"] = str(ex)
            enriched.append(e)
        time.sleep(SLEEP_SEC)
    return enriched

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


tz = ZoneInfo("Europe/Berlin")
today = datetime.now(tz).date()
week_start = today - timedelta(days=WEEK_DAYS - 1)
key = api_key()

print(f"Загружаем ответы с {week_start} по {today}...")
raw = fetch_answers(week_start, today, key)
filtered = filter_only_ka_ga(raw, key)
md = build_md(week_start, today, filtered)

out_name = f"digest-answers-week-{today.strftime('%Y%m%d')}.md"
with open(out_name, "w", encoding="utf-8") as f:
    f.write(md)

if PRINT_TO_STDOUT:
    print(md)

print(f"Готово. Файл: {out_name}. Ответов: {len(filtered)}")


key = api_key()
entries_with_texts = save_texts_for_entries(filtered, TEXT_DIR, key)

md_full = build_md_week_with_local_texts(week_start, today, entries_with_texts)

out_name = f"digest-answers-week-{today.strftime('%Y%m%d')}.md"
with open(out_name, "w", encoding="utf-8") as f:
    f.write(md_full)

print(f"Готово. Markdown: {out_name}. Тексты: {sum(1 for e in entries_with_texts if e.get('local_text_path'))} из {len(entries_with_texts)}")