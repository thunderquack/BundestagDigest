from __future__ import annotations

import argparse
import os
import sys
from typing import Optional, Any, Dict

from dotenv import load_dotenv


def getenv_str(name: str) -> Optional[str]:
    val = os.environ.get(name)
    if val is None:
        return None
    val = val.strip()
    return val or None


ALLOWED_AUTHORS = ["SPD", "CDU/CSU", "die Linke", "die Grünen", "AfD"]
AUTHOR_SYNONYMS = {
    "AfD": ("afd",),
    "CDU/CSU": ("cdu/csu",),
    "SPD": ("spd",),
    "die Linke": ("linke",),
    "die Grünen": ("grüne", "gruene", "grune", "grün"),
}


def normalize_author(value: Any) -> Any:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None

    lowered = s.lower()
    for canonical, synonyms in AUTHOR_SYNONYMS.items():
        if canonical.lower() in lowered:
            return canonical
        if any(token in lowered for token in synonyms):
            return canonical
    if "cdu" in lowered and "csu" in lowered:
        return "CDU/CSU"
    return None


def build_prompt_ru() -> str:
    return (
        "Ты помощник-референт. Тебе дают текст официального документа. "
        "Отвечай строго структурированным JSON по запрошенной схеме. "
        "В полях, оканчивающихся на _ru — пиши по‑русски, в _de — по‑немецки. "
        "Если в тексте чего-то нет — указывай null, без домыслов."
    )


def build_user_instruction_ru() -> str:
    return (
        "Извлеки из текста: номер (без слова 'Drucksache'), дату, автора (фракция, если указана). "
        "Затем кратко изложи суть (2–3 абзаца) на русском. Если чего-то нет в тексте, верни null."
    )


def read_text_file(path: str, max_chars: Optional[int]) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read()
    if max_chars is not None and max_chars > 0 and len(content) > max_chars:
        return content[:max_chars]
    return content


def call_openai_markdown(text: str, model: str, temperature: float) -> str:
    try:
        from openai import OpenAI  # type: ignore
    except Exception as ex:  # pragma: no cover
        raise RuntimeError(
            "OpenAI SDK is not installed. Add 'openai' to requirements and install."
        ) from ex

    api_key = getenv_str("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing. Put it into .env or environment.")

    client = OpenAI(api_key=api_key)

    system_prompt = build_prompt_ru()
    user_instruction = build_user_instruction_ru()

    # Передаём чёткие, но короткие инструкции. Текст документа ограничиваем разделителями.
    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": (
                f"{user_instruction}\n\n"  # краткая инструкция
                "<документ>\n"  # начало
                f"{text}\n"  # сам текст
                "</документ>"  # конец
            ),
        },
    ]

    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
    )

    choice = resp.choices[0]
    content = (choice.message.content or "").strip()
    if not content:
        raise RuntimeError("Empty response from OpenAI.")
    return content


def call_openai_structured(
    text: str,
    model: str,
    temperature: float = 0.1,
) -> Dict[str, Any]:
    """
    Use OpenAI structured outputs to extract fields into a JSON object:
    - number: str (без слова "Drucksache"), может быть вида "20/1234"; если отсутствует — null
    - author: str|null (фракция или отправитель, если указан)
    - date: str|null (ISO-8601 YYYY-MM-DD, если можно однозначно определить)
    - title: str (одно короткое предложение по-русски)
    - description_ru: str|null (2–3 абзаца по‑русски: что спрашивают депутаты и как отвечает правительство)
    - description_de: str|null (2–3 Absätze auf Deutsch: was die Abgeordneten fragen und wie die Bundesregierung antwortet)
    """
    try:
        from openai import OpenAI  # type: ignore
    except Exception as ex:  # pragma: no cover
        raise RuntimeError(
            "OpenAI SDK is not installed. Add 'openai' to requirements and install."
        ) from ex

    api_key = getenv_str("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing. Put it into .env or environment.")

    client = OpenAI(api_key=api_key)

    system_prompt = build_prompt_ru()
    allowed_authors_text = ", ".join(ALLOWED_AUTHORS)
    user_instruction = (
        "Верни строго JSON по схеме без лишних ключей. "
        "number — номер документа без слова 'Drucksache'; "
        f"author — только одно из значений: {allowed_authors_text}; "
        "если в документе указана фракция в развернутом виде вроде 'Fraktion der AfD', "
        "верни только нормализованное короткое значение из этого списка; "
        "если автора нельзя однозначно отнести к одному из этих значений, установи null; "
        "date — ISO дата YYYY-MM-DD при наличии; title — одно короткое предложение по-русски без вводных слов 'Ответ на' или 'Запрос о'; "
        "description_ru — 2–3 абзаца по‑русски (что спрашивают депутаты и как отвечает правительство); "
        "description_de — 2–3 Absätze auf Deutsch (was gefragt wird und wie die Bundesregierung antwortet). "
        "Если поле невозможно извлечь, установи значение null."
    )

    messages = [
        {"role": "system", "content": system_prompt},
        {
            "role": "user",
            "content": (
                f"{user_instruction}\n\n"
                "<документ>\n"
                f"{text}\n"
                "</документ>"
            ),
        },
    ]

    schema = {
        "name": "drucksache_summary",
        "strict": True,
        "schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "number": {"type": ["string", "null"], "description": "Номер без слова 'Drucksache'"},
                "author": {"type": ["string", "null"], "description": "Фракция/отправитель, если указан"},
                "date": {"type": ["string", "null"], "description": "Дата в формате YYYY-MM-DD"},
                "title": {"type": "string", "description": "Одно короткое предложение по-русски без вводных слов 'Ответ на' или 'Запрос о'"},
                "description_ru": {"type": ["string", "null"], "description": "2–3 абзаца по‑русски"},
                "description_de": {"type": ["string", "null"], "description": "2–3 Absätze auf Deutsch"},
            },
            "required": ["title", "description_ru", "description_de", "number", "date", "author"],
        },
    }

    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
        response_format={
            "type": "json_schema",
            "json_schema": schema,
        },
    )

    choice = resp.choices[0]
    # Prefer parsed if SDK provides it; fallback to JSON in content
    parsed = getattr(choice.message, "parsed", None)
    if parsed is not None:
        if not isinstance(parsed, dict):
            raise RuntimeError("Structured response not a dict.")
        parsed["author"] = normalize_author(parsed.get("author"))
        return parsed

    import json

    content = (choice.message.content or "").strip()
    if not content:
        raise RuntimeError("Empty response from OpenAI.")
    try:
        data = json.loads(content)
        if not isinstance(data, dict):
            raise RuntimeError("Structured response not a dict.")
        data["author"] = normalize_author(data.get("author"))
        return data
    except Exception as ex:  # pragma: no cover
        raise RuntimeError(f"Failed to parse JSON content: {content[:200]}...") from ex


def main(argv: list[str]) -> int:
    load_dotenv()

    parser = argparse.ArgumentParser(
        description=(
            "Суммаризация текста документа в Markdown на русском (номер, дата, отправитель, суть)."
        )
    )
    parser.add_argument("path", help="Путь к текстовому файлу документа (.txt)")
    parser.add_argument(
        "--model",
        default=getenv_str("OPENAI_MODEL") or "gpt-4o-mini",
        help="Модель OpenAI (по умолчанию: env OPENAI_MODEL или gpt-4o-mini)",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.2,
        help="Температура выборки (по умолчанию 0.2)",
    )
    parser.add_argument(
        "--max-chars",
        type=int,
        default=None,
        help="Макс. символов из файла для отправки в модель (опционально)",
    )
    parser.add_argument(
        "--structured-json",
        action="store_true",
        help=(
            "Вернуть структурированный JSON (number, author, date, title, description_ru, description_de) вместо Markdown"
        ),
    )

    args = parser.parse_args(argv)

    path = args.path
    if not os.path.isfile(path):
        print(f"Файл не найден: {path}", file=sys.stderr)
        return 2

    try:
        text = read_text_file(path, args.max_chars)
        if not text.strip():
            print("Файл пустой после чтения.", file=sys.stderr)
            return 3

        # Печатаем результат в stdout
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        if args.structured_json:
            data = call_openai_structured(text, args.model, args.temperature)
            import json

            print(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            md = call_openai_markdown(text, args.model, args.temperature)
            print(md)
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as ex:
        print(f"Ошибка: {ex}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
