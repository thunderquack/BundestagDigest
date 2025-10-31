import os
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

INPUT_DIR = "drucksache_texts/Antwort"
OUTPUT_DIR = "summaries"
MAX_CHARS = 8000

model_name = "Trelis/Mistral-7B-Instruct-v0.1-Summarize-16k"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSeq2SeqLM.from_pretrained(model_name)

def summarize_text(text):
    prompt = (
        "Сделай краткий дайджест на русском языке, чтобы понять содержание документа. "
        "Факты не придумывай. Дай 3-7 пунктов:\n\n"
        + text
    )
    inputs = tokenizer(prompt, return_tensors="pt", truncation=True, max_length=4096)
    output = model.generate(**inputs, max_new_tokens=512)
    result = tokenizer.decode(output[0], skip_special_tokens=True)
    return result

for file in os.listdir(INPUT_DIR):
    if not file.endswith(".txt"):
        continue

    input_path = os.path.join(INPUT_DIR, file)
    output_path = os.path.join(OUTPUT_DIR, file.replace(".txt", ".md"))

    with open(input_path, "r", encoding="utf-8") as f:
        text = f.read(MAX_CHARS)

    summary = summarize_text(text)

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(f"# Дайджест {file}\n\n")
        f.write(summary)

print("Done.")
