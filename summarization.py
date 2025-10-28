import os
from pathlib import Path
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM, pipeline

INPUT_DIR = Path("drucksache_texts/Antwort")
OUTPUT_DIR = Path("summaries")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Берем первые 8000 символов как "первую страницу" в текстовом виде
MAX_CHARS = 8000

# 1) Немецкая суммаризация
SUM_MODEL = "deutsche-telekom/mt5-small-sum-de-mit-v1"
sum_tokenizer = AutoTokenizer.from_pretrained(SUM_MODEL, use_fast=False)
sum_model = AutoModelForSeq2SeqLM.from_pretrained(SUM_MODEL)

# 2) Перевод de -> ru через NLLB-200 distilled 600M
# Языковые коды: deu_Latn -> rus_Cyrl
NLLB_MODEL = "facebook/nllb-200-distilled-600M"
nllb_translator = pipeline(
    "translation",
    model=NLLB_MODEL,
    tokenizer=NLLB_MODEL,
    src_lang="deu_Latn",
    tgt_lang="rus_Cyrl",
    use_fast=False,
)

def summarize_de(text_de: str) -> str:
    # Инструкция для лаконичного дайджеста
    prompt = (
        "Fasse den folgenden deutschen Parlamentsdokumenttext sehr kurz zusammen. "
        "Gib 3-7 Aufzählungspunkte in sachlichem Stil. Keine Erfindungen.\n\n"
        + text_de
    )
    inputs = sum_tokenizer(prompt, return_tensors="pt", truncation=True, max_length=4096)
    output = sum_model.generate(**inputs, max_new_tokens=384)
    summary_de = sum_tokenizer.decode(output[0], skip_special_tokens=True).strip()
    return summary_de

def translate_de_ru(text_de: str) -> str:
    res = nllb_translator(text_de)
    return res[0]["translation_text"].strip()

def process_file(txt_path: Path) -> None:
    with txt_path.open("r", encoding="utf-8") as f:
        text = f.read(MAX_CHARS)

    if not text.strip():
        return

    summary_de = summarize_de(text)
    summary_ru = translate_de_ru(summary_de)

    out_path = OUTPUT_DIR / (txt_path.stem + ".md")
    with out_path.open("w", encoding="utf-8") as f:
        f.write(f"# Дайджест {txt_path.name}\n\n")
        f.write("## Кратко по первой странице\n\n")
        # Выводим уже по-русски
        if "\n" in summary_ru or "•" in summary_ru or "-" in summary_ru:
            # Нормализуем в маркеры списка
            lines = [ln.strip(" •\t") for ln in summary_ru.splitlines() if ln.strip()]
            for ln in lines:
                f.write(f"- {ln}\n")
        else:
            f.write(f"- {summary_ru}\n")

def main():
    for file in sorted(INPUT_DIR.glob("*.txt")):
        process_file(file)
    print("Done.")

if __name__ == "__main__":
    main()
