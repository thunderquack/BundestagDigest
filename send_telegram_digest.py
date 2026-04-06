from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from typing import Any, Dict, Iterable, List
from urllib import parse, request

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    def load_dotenv() -> bool:
        return False


DB_PATH = os.path.join("reports", "json", "kleine-anfrage.json")
TELEGRAM_API_BASE = "https://api.telegram.org"
MAX_MESSAGE_LEN = 4000


def getenv_str(name: str) -> str | None:
    value = os.environ.get(name)
    if value is None:
        return None
    value = value.strip()
    return value or None


def load_today_entries(db_path: str, target_date: str) -> List[Dict[str, Any]]:
    if not os.path.isfile(db_path):
        return []

    with open(db_path, "r", encoding="utf-8") as f:
        data = json.load(f) or {}

    if not isinstance(data, dict):
        return []

    out: List[Dict[str, Any]] = []
    for record in data.values():
        if not isinstance(record, dict):
            continue
        if str(record.get("saved_date") or "") != target_date:
            continue
        out.append(record)

    out.sort(
        key=lambda item: (
            str(item.get("date") or ""),
            str(item.get("number") or ""),
            str(item.get("title") or ""),
        )
    )
    return out


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def build_entry_message(record: Dict[str, Any]) -> str:
    title = _clean_text(record.get("title")) or "Без названия"
    number = _clean_text(record.get("number")) or "без номера"
    doc_date = _clean_text(record.get("date")) or "дата не указана"
    author = _clean_text(record.get("author")) or "автор не указан"
    summary = _clean_text(record.get("description_ru")) or "Краткое описание отсутствует."

    lines = [
        f"{title}",
        f"BT-Drucksache: {number}",
        f"Дата: {doc_date}",
        f"Автор: {author}",
        "",
        summary,
    ]
    return "\n".join(lines).strip()


def _split_long_text(text: str, max_len: int = MAX_MESSAGE_LEN) -> List[str]:
    if len(text) <= max_len:
        return [text]

    chunks: List[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= max_len:
            chunks.append(remaining)
            break

        split_at = remaining.rfind("\n\n", 0, max_len)
        if split_at == -1:
            split_at = remaining.rfind("\n", 0, max_len)
        if split_at == -1:
            split_at = remaining.rfind(" ", 0, max_len)
        if split_at == -1 or split_at < max_len // 2:
            split_at = max_len

        chunk = remaining[:split_at].rstrip()
        chunks.append(chunk)
        remaining = remaining[split_at:].lstrip()

    return chunks


def build_messages(records: Iterable[Dict[str, Any]], target_date: str) -> List[str]:
    records = list(records)
    if not records:
        return []

    messages: List[str] = [
        f"Новые суммаризации ответов федерального правительства за {target_date}: {len(records)}"
    ]
    for index, record in enumerate(records, start=1):
        text = build_entry_message(record)
        chunks = _split_long_text(text)
        for chunk_index, chunk in enumerate(chunks, start=1):
            prefix = f"[{index}/{len(records)}]"
            if len(chunks) > 1:
                prefix += f" часть {chunk_index}"
            messages.append(f"{prefix}\n{chunk}")
    return messages


def send_telegram_message(bot_token: str, chat_id: str, text: str) -> None:
    payload = parse.urlencode(
        {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": "true",
        }
    ).encode("utf-8")
    url = f"{TELEGRAM_API_BASE}/bot{bot_token}/sendMessage"
    req = request.Request(url, data=payload, method="POST")
    with request.urlopen(req, timeout=30) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as ex:
        raise RuntimeError(f"Telegram API returned non-JSON response: {raw[:200]}") from ex
    if not data.get("ok"):
        raise RuntimeError(f"Telegram API error: {data}")


def send_today_digest(
    *,
    db_path: str = DB_PATH,
    target_date: str | None = None,
    bot_token: str | None = None,
    chat_id: str | None = None,
) -> int:
    target_date = target_date or date.today().isoformat()
    bot_token = bot_token or getenv_str("TELEGRAM_API_KEY")
    chat_id = chat_id or getenv_str("TELEGRAM_USER_ID")

    if not bot_token or not chat_id:
        print("TELEGRAM_API_KEY or TELEGRAM_USER_ID not set. Skipping Telegram delivery.")
        return 0

    records = load_today_entries(db_path, target_date)
    if not records:
        print(f"No summaries with saved_date={target_date}. Skipping Telegram delivery.")
        return 0

    messages = build_messages(records, target_date)
    for message in messages:
        send_telegram_message(bot_token, chat_id, message)

    print(f"Telegram messages sent: {len(messages)}")
    return len(messages)


def main(argv: List[str]) -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Send today's Russian Bundestag summaries to Telegram."
    )
    parser.add_argument(
        "--db-path",
        default=DB_PATH,
        help="Path to kleine-anfrage JSON database.",
    )
    parser.add_argument(
        "--date",
        default=None,
        help="Target saved_date in ISO format YYYY-MM-DD. Defaults to today.",
    )
    args = parser.parse_args(argv)

    try:
        send_today_digest(db_path=args.db_path, target_date=args.date)
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as ex:
        print(f"Telegram delivery failed: {ex}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
