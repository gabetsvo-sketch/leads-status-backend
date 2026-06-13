#!/usr/bin/env python3
"""Вшивает обезличенные scenario-паки из Obsidian в бэкенд (чтобы Render их читал).

Чистит служебную обвязку (frontmatter, obsidian-ссылки, провенанс deal_id, длинные
числа) и проверяет результат тем же PII-гейтом, что и рантайм. Запускать при
обновлении паков в Obsidian. Результат: backend/style-packs/<pack_id>.md
"""
import re
import sys
import pathlib

BACKEND = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND))
import main  # noqa: E402

VAULT_PACKS = pathlib.Path(
    "/Users/vladimir/Desktop/Obsidian/Хранилище 1/Assist - Real estate/office/style-engine/packs"
)
OUT = BACKEND / "style-packs"


def clean(text: str) -> str:
    # 1) убрать YAML frontmatter
    if text.startswith("---"):
        parts = text.split("---", 2)
        if len(parts) == 3:
            text = parts[2]
    # 2) убрать obsidian wiki-ссылки и строки-бэклинки
    text = re.sub(r"\[\[[^\]]*\]\]", "", text)
    text = re.sub(r"(?im)^\s*backlink:.*$", "", text)
    # 3) убрать провенанс «source `deal_id: NNNN`:» — оставить саму фразу
    text = re.sub(r"source\s+`?deal_id:\s*\d+`?:\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"`?deal_id:\s*\d+`?", "", text, flags=re.IGNORECASE)
    # 4) длинные числовые последовательности (id) → плейсхолдер
    text = re.sub(r"\b\d{6,}\b", "[id]", text)
    # 5) схлопнуть пустые строки
    text = re.sub(r"\n{3,}", "\n\n", text).strip() + "\n"
    return text


def main_run() -> int:
    index = main._style_load_runtime_index()
    pack_ids = [p.get("pack_id") for p in (index.get("packs") or []) if p.get("pack_id")]
    OUT.mkdir(exist_ok=True)
    written, skipped = 0, []
    for pid in pack_ids:
        src = VAULT_PACKS / f"{pid}.md"
        if not src.exists():
            skipped.append((pid, "нет .md в vault"))
            continue
        cleaned = clean(src.read_text(encoding="utf-8"))
        if main._style_text_has_pii(cleaned):
            skipped.append((pid, "после чистки ОСТАЛСЯ PII — не вшиваю"))
            continue
        (OUT / f"{pid}.md").write_text(cleaned, encoding="utf-8")
        written += 1
        print(f"OK   {pid:42} {len(cleaned)} симв")
    for pid, why in skipped:
        print(f"SKIP {pid:42} {why}")
    print(f"\nвшито: {written}, пропущено: {len(skipped)}")
    return 0 if written else 1


if __name__ == "__main__":
    sys.exit(main_run())
