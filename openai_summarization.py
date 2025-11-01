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


def build_prompt_ru() -> str:
    # Короткий, "щадящий" системный промпт: минимум инструкций, без жёстких требований.
    return (
        "Ты помощник-референт на русском языке. Тебе дают текст официального документа. "
        "Отвечай только по-русски. Если в тексте чего-то нет — указывай null, а не домысливай."
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
    - description: str (2–3 абзаца по-русски, по делу)
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
    user_instruction = (
        "Верни строго JSON по схеме без лишних ключей. "
        "number — номер документа без слова 'Drucksache'; author — фракция/отправитель; "
        "date — ISO дата YYYY-MM-DD при наличии; title — одно короткое предложение по-русски; "
        "description — 2–3 абзаца по-русски. Если поле невозможно извлечь, установи значение null."
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
                "title": {"type": "string", "description": "Одно короткое предложение по-русски"},
                "description": {"type": "string", "description": "2–3 абзаца по-русски"},
            },
            "required": ["title", "description", "number", "date", "author"],
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
        return parsed

    import json

    content = (choice.message.content or "").strip()
    if not content:
        raise RuntimeError("Empty response from OpenAI.")
    try:
        return json.loads(content)
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
        help="Вернуть структурированный JSON (number, author, date, title, description) вместо Markdown",
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
