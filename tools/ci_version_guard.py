#!/usr/bin/env python3
"""CI-страж версии (§12.4 стандарта VIBE-CODE v0.01.001).

Правило: если в пуше изменён КОД ПРОДУКТА (корневые `*.py` — у backend это
`main.py` — или `requirements.txt`), то `VERSION.json` ОБЯЗАН быть повышен в том
же пуше. Иначе сборка падает и деплой блокируется.

НЕ требуют повышения версии: изменения в `tests/`, `tools/`, документации (`*.md`),
файлах CI (`.github/`) и самого `VERSION.json`. Это инфраструктура/проверки, а не
поставляемое поведение сервиса (Render запускает только `main:app`).

Запуск (в CI): python tools/ci_version_guard.py <before_sha> <after_sha>
Локально для проверки:  python tools/ci_version_guard.py HEAD~1 HEAD
"""
import json
import re
import subprocess
import sys


def _git(*args: str) -> str:
    return subprocess.run(["git", *args], capture_output=True, text=True).stdout


def _version_int(raw: str) -> int:
    v = json.loads(raw)
    return int(v["generation"]) * 1_000_000 + int(v["milestone"]) * 1_000 + int(v["revision"])


def _is_product_file(path: str) -> bool:
    # Корневой .py (main.py и любые будущие корневые модули) или requirements.txt.
    # tests/, tools/, .github/ — это подкаталоги, под шаблон не попадают.
    return bool(re.match(r"^[^/]+\.py$", path)) or path == "requirements.txt"


def main() -> int:
    if len(sys.argv) < 3:
        print("usage: ci_version_guard.py <before_sha> <after_sha>")
        return 2
    before, after = sys.argv[1], sys.argv[2]

    if re.fullmatch(r"0+", before or ""):
        print("OK: новая ветка / нет базового коммита — страж пропущен.")
        return 0

    changed = [p for p in _git("diff", "--name-only", before, after).splitlines() if p.strip()]
    product = [p for p in changed if _is_product_file(p)]
    if not product:
        print("OK: код продукта не изменён — повышение версии не требуется.")
        print("    изменены:", ", ".join(changed) or "(ничего)")
        return 0

    try:
        old = _version_int(_git("show", f"{before}:VERSION.json"))
    except Exception:
        old = -1  # не было VERSION.json — любое наличие версии считается повышением
    try:
        new = _version_int(open("VERSION.json", encoding="utf-8").read())
    except Exception as exc:
        print(f"FAIL: не читается VERSION.json: {exc}")
        return 1

    print(f"Изменён код продукта: {', '.join(product)}")
    print(f"VERSION.json: было {old} → стало {new}")
    if new > old:
        print("OK: версия повышена вместе с кодом (§11.2).")
        return 0
    print("FAIL: код продукта изменён, но VERSION.json НЕ повышен (§12.4).")
    print("      Подними версию в VERSION.json в этом же пуше и повтори.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
