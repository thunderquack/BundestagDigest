from __future__ import annotations

import argparse
import os
import sys
from typing import Optional

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
        "Нужно кратко оформить ответ в Markdown и ничего лишнего. "
        "Если в тексте чего-то нет — так и пиши: 'не указано'. "
        "Выводи чистый Markdown без обрамления в кодовые блоки (без ``` и указания языка)."
    )


def build_user_instruction_ru() -> str:
    return (
        "Извлеки из текста и оформи в Markdown:") + (
        "\n- номер"  # номер документа
        "\n- дату"  # дата документа
        "\n- кто отправитель запроса: фракция, если указана"  # автор запроса
        "\n\nЗатем дай 2–3 абзаца суть документа кратко и по делу. "
        "Пиши только по-русски. Без прелюдий и заключений, только результат. "
        "Текст документа может быть частично усечён — опирайся на доступную часть; если чего-то нет, пиши 'не указано'. "
        "Не оборачивай ответ в тройные кавычки и кодовые блоки — нужен чистый Markdown."
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

        md = call_openai_markdown(text, args.model, args.temperature)
        # Печатаем результат в stdout
        sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
        print(md)
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as ex:
        print(f"Ошибка: {ex}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
