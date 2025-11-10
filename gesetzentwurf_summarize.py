from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv


SECTIONS = [
    ("A", "Problem und Ziel"),
    ("B", "Lösung"),
    ("C", "Alternativen"),
    ("D", "Haushaltsausgaben ohne Erfüllungsaufwand"),
    ("E", "Erfüllungsaufwand"),
]


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def _normalize_ws(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def extract_sections_ae(text: str) -> Dict[str, str]:
    """Extract sections A–E from the given text using robust heading detection.

    We look for lines starting with e.g. "A. Problem und Ziel", case-insensitive.
    Returns a mapping by section key ("A", "B", ...) to raw section body text (without the heading line).
    """
    # Title variants to handle ASCII/orthographic differences (e.g., Lösung/Loesung/Losung)
    title_variants: Dict[str, List[str]] = {
        "Problem und Ziel": ["Problem und Ziel"],
        "Lösung": ["Lösung", "Loesung", "Losung"],
        "Alternativen": ["Alternativen"],
        "Haushaltsausgaben ohne Erfüllungsaufwand": [
            "Haushaltsausgaben ohne Erfüllungsaufwand",
            "Haushaltsausgaben ohne Erfuellungsaufwand",
            "Haushaltsausgaben ohne Erfullungsaufwand",
        ],
        "Erfüllungsaufwand": ["Erfüllungsaufwand", "Erfuellungsaufwand", "Erfullungsaufwand"],
    }

    # Build regexes for each section heading
    patterns: List[Tuple[str, str, re.Pattern[str]]] = []
    for key, title in SECTIONS:
        alts = title_variants.get(title, [title])
        # Accept variations: "A.", "A)" and optional additional text on the same line
        alt_regex = "|".join(re.escape(a) for a in alts)
        pat = re.compile(
            rf"^\s*{re.escape(key)}[\.)]\s*(?:{alt_regex})\b.*$",
            re.IGNORECASE | re.MULTILINE,
        )
        patterns.append((key, title, pat))

    # Find all matches and their spans
    found: List[Tuple[str, str, int, int]] = []  # (key, title, start, end_of_heading_line)
    for key, title, pat in patterns:
        for m in pat.finditer(text):
            # compute end of heading line
            line_end = text.find("\n", m.start())
            if line_end == -1:
                line_end = len(text)
            else:
                line_end += 1  # include newline
            found.append((key, title, m.start(), line_end))

    # Sort by start position
    found.sort(key=lambda x: x[2])

    # Map section key -> (start_of_body, end)
    out: Dict[str, str] = {}
    for idx, (key, _title, _start, heading_end) in enumerate(found):
        body_start = heading_end
        if idx + 1 < len(found):
            next_start = found[idx + 1][2]
        else:
            next_start = len(text)
        body = text[body_start:next_start]
        out[key.upper()] = body.strip("\n\r ")
    return out


def extract_header_before_a(text: str) -> str:
    """Return everything before the first A.* heading (Problem und Ziel)."""
    # Build detection regex for the first A heading using the same variants as above
    alts = ["Problem und Ziel"]
    pat = re.compile(rf"^\s*A[\.)]\s*(?:{'|'.join(map(re.escape, alts))})\b.*$", re.IGNORECASE | re.MULTILINE)
    m = pat.search(text)
    if not m:
        return ""
    start = m.start()
    header = text[:start]
    return header.strip()


def trim_sections(sections: Dict[str, str], max_chars_per_section: int) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for key, raw in sections.items():
        s = _normalize_ws(raw)
        if max_chars_per_section > 0 and len(s) > max_chars_per_section:
            s = s[: max_chars_per_section].rstrip()
        out[key] = s
    return out


def _build_messages_for_openai(sections: Dict[str, str], header: str) -> List[Dict[str, str]]:
    # System and user content in Russian as requested by the user
    system_prompt = (
        "Ты аналитик, который структурирует законопроекты Бундестага. "
        "Тебе переданы только выдержки из разделов A–E (A. Problem und Ziel, B. Lösung, C. Alternativen, "
        "D. Haushaltsausgaben ohne Erfüllungsaufwand, E. Erfüllungsaufwand). Если каких‑то сведений нет в этих "
        "разделах, ставь null в JSON. Отвечай строго в рамках предоставленного материала, без домыслов."
    )

    user_instruction = (
        "Проанализируй заголовочную часть (текст до раздела A) и разделы A–E. "
        "Верни строго JSON со следующими полями: "
        "number (строка|null, номер Drucksache), date (строка|null, ISO YYYY-MM-DD), "
        "title (строка|null, официальный заголовок), initiator (строка|null, кто внес), "
        "goal (строка|null, краткая цель, по-русски), solution (строка|null, по-русски), alternatives (строка|null, по-русски), "
        "budget_wo_fulfillment (строка|null, по-русски, из D), fulfillment_costs (строка|null, по-русски, из E), "
        "overall_summary (строка, по-русски, 2–3 предложения по сути). "
        "Все текстовые поля должны быть на русском языке (кратко перефразуй по смыслу, сохраняя факты/цифры). "
        "Если сведений нет — ставь null. Ничего не выдумывай."
    )

    # Assemble a single user message that contains the sections
    parts: List[str] = [user_instruction, "", "<HEADER>"]
    if header:
        parts.append(header)
    parts.append("</HEADER>")
    parts.append("")
    parts.append("<SECTIONS>")
    for key, title in SECTIONS:
        content = sections.get(key.upper())
        if content:
            parts.append(f"<SECTION name=\"{key}. {title}\">\n{content}\n</SECTION>")
    parts.append("</SECTIONS>")

    user_content = "\n".join(parts)

    return [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]


def call_openai_structured_ae(
    sections: Dict[str, str],
    header: str,
    model: str,
    temperature: float = 0.1,
) -> Dict[str, Any]:
    try:
        from openai import OpenAI  # type: ignore
    except Exception as ex:  # pragma: no cover
        raise RuntimeError(
            "OpenAI SDK is not installed. Add 'openai' to requirements and install."
        ) from ex

    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing. Put it into .env or environment.")

    client = OpenAI(api_key=api_key)

    messages = _build_messages_for_openai(sections, header)

    schema = {
        "name": "gesetzentwurf_summary",
        "strict": True,
        "schema": {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "number": {"type": ["string", "null"], "description": "Номер Drucksache"},
                "date": {"type": ["string", "null"], "description": "Дата ISO YYYY-MM-DD"},
                "title": {"type": ["string", "null"], "description": "Название законопроекта"},
                "initiator": {"type": ["string", "null"], "description": "Кем внесен"},
                "goal": {"type": ["string", "null"], "description": "Цель (из A)"},
                "solution": {"type": ["string", "null"], "description": "Решение (из B)"},
                "alternatives": {"type": ["string", "null"], "description": "Альтернативы (из C)"},
                "budget_wo_fulfillment": {"type": ["string", "null"], "description": "Бюджет без исполнит. затрат (из D)"},
                "fulfillment_costs": {"type": ["string", "null"], "description": "Исполнительские затраты (из E)"},
                "overall_summary": {"type": "string", "description": "2–3 предложения о сути"},
            },
            "required": [
                "overall_summary",
                "number",
                "date",
                "title",
                "initiator",
                "goal",
                "solution",
                "alternatives",
                "budget_wo_fulfillment",
                "fulfillment_costs",
            ],
        },
    }

    resp = client.chat.completions.create(
        model=model,
        messages=messages,
        temperature=temperature,
        response_format={"type": "json_schema", "json_schema": schema},
    )

    choice = resp.choices[0]
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


def _md_from_json(data: Dict[str, Any]) -> str:
    def vv(key: str) -> str:
        v = data.get(key)
        if v is None:
            return ""
        return str(v).strip()

    lines: List[str] = []
    title = vv("title")
    if title:
        lines.append(f"# {title}")
    else:
        lines.append("# Gesetzentwurf")

    meta_parts = []
    if vv("number"):
        meta_parts.append(vv("number"))
    if vv("date"):
        meta_parts.append(vv("date"))
    if meta_parts:
        lines.append(" ".join(meta_parts))

    if vv("initiator"):
        lines.append(f"Initiator: {vv('initiator')}")

    if vv("overall_summary"):
        lines.append("")
        lines.append(vv("overall_summary"))

    # Details
    if any(vv(k) for k in ("goal", "solution", "alternatives", "budget_wo_fulfillment", "fulfillment_costs")):
        lines.append("")
    if vv("goal"):
        lines.append("## Ziel")
        lines.append(vv("goal"))
    if vv("solution"):
        lines.append("")
        lines.append("## Lösung")
        lines.append(vv("solution"))
    if vv("alternatives"):
        lines.append("")
        lines.append("## Alternativen")
        lines.append(vv("alternatives"))
    if vv("budget_wo_fulfillment"):
        lines.append("")
        lines.append("## Haushaltsausgaben ohne Erfüllungsaufwand")
        lines.append(vv("budget_wo_fulfillment"))
    if vv("fulfillment_costs"):
        lines.append("")
        lines.append("## Erfüllungsaufwand")
        lines.append(vv("fulfillment_costs"))

    return "\n".join(lines).strip() + "\n"


def summarize_file(
    path: str,
    *,
    model: str,
    temperature: float,
    max_chars_per_section: int,
    out_md: Optional[str] = None,
    out_json: Optional[str] = None,
) -> Tuple[str, str]:
    text = _read_text(path)
    sections_raw = extract_sections_ae(text)
    if not sections_raw:
        raise RuntimeError("Не удалось найти секции A–E в файле.")
    header = extract_header_before_a(text)
    sections_trimmed = trim_sections(sections_raw, max_chars_per_section)

    data = call_openai_structured_ae(sections_trimmed, header, model=model, temperature=temperature)

    # Normalize string placeholders "null" -> None on optional fields
    for k in [
        "title",
        "initiator",
        "goal",
        "solution",
        "alternatives",
        "budget_wo_fulfillment",
        "fulfillment_costs",
    ]:
        v = data.get(k)
        if isinstance(v, str) and v.strip().lower() == "null":
            data[k] = None

    md = _md_from_json(data)

    # Derive default outputs when not provided
    base = os.path.splitext(os.path.basename(path))[0]
    reports_dir = os.path.join("reports", "Gesetzentwurf")
    os.makedirs(reports_dir, exist_ok=True)

    if out_md is None:
        out_md = os.path.join(reports_dir, f"{base}-Report.md")
    if out_json is None:
        out_json = os.path.join(reports_dir, f"{base}-Report.json")

    with open(out_md, "w", encoding="utf-8-sig") as f:
        f.write(md)
    import json as _json
    with open(out_json, "w", encoding="utf-8-sig") as f:
        _json.dump(data, f, ensure_ascii=False, indent=2)

    return out_md, out_json


def main(argv: List[str]) -> int:
    load_dotenv()

    ap = argparse.ArgumentParser(
        description=(
            "Суммаризация Gesetzentwurf: извлечение разделов A–E, короткая обрезка и структурный JSON + MD."
        )
    )
    ap.add_argument("path", help="Путь к текстовому файлу Gesetzentwurf (.txt)")
    ap.add_argument(
        "--model",
        default=os.environ.get("OPENAI_MODEL") or "gpt-4o-mini",
        help="OpenAI модель (по умолчанию env OPENAI_MODEL или gpt-4o-mini)",
    )
    ap.add_argument(
        "--temperature",
        type=float,
        default=0.1,
        help="Температура выборки (по умолчанию 0.1)",
    )
    ap.add_argument(
        "--max-chars-per-section",
        type=int,
        default=int(os.environ.get("AE_MAX_CHARS", "1500")),
        help="Ограничение символов на каждую секцию (по умолчанию 1500)",
    )
    ap.add_argument("--out-md", default=None, help="Путь для Markdown отчета (по умолчанию reports/Gesetzentwurf/<base>-Report.md)")
    ap.add_argument("--out-json", default=None, help="Путь для JSON файла (по умолчанию reports/Gesetzentwurf/<base>-Report.json)")

    args = ap.parse_args(argv)

    if not os.path.isfile(args.path):
        print(f"Файл не найден: {args.path}", file=sys.stderr)
        return 2

    try:
        md_path, json_path = summarize_file(
            args.path,
            model=args.model,
            temperature=args.temperature,
            max_chars_per_section=args.max_chars_per_section,
            out_md=args.out_md,
            out_json=args.out_json,
        )
        print(f"MD: {md_path}")
        print(f"JSON: {json_path}")
        return 0
    except KeyboardInterrupt:
        return 130
    except Exception as ex:
        print(f"Ошибка: {ex}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

