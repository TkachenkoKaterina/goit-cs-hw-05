#!/usr/bin/env python3
"""
gen_files.py

Створює тестову вихідну папку з файлами (і підпапками), щоб потім
перевірити асинхронний сортувальник.

Приклади:
    python gen_files.py
    python gen_files.py -d test_src -n 40 --subdirs 3
"""

from __future__ import annotations

import argparse
from pathlib import Path
from random import choice, randint
from uuid import uuid4


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Згенерувати вихідну папку з файлами для сортування."
    )
    p.add_argument(
        "-d", "--dest",
        type=Path,
        default=Path("test_src"),
        help="Куди створити вихідну папку з файлами (стандартно: ./test_src).",
    )
    p.add_argument(
        "-n", "--files",
        type=int,
        default=30,
        help="Скільки файлів створити загалом (стандартно: 30).",
    )
    p.add_argument(
        "--subdirs",
        type=int,
        default=3,
        help="Скільки підпапок зробити всередині (стандартно: 3).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    dest: Path = args.dest
    n_files: int = args.files
    n_subdirs: int = args.subdirs

    # Набір можливих розширень (деякі файли будуть без розширення)
    exts = ["txt", "jpg", "png", "pdf", "docx", "json", "csv", "md", "py", None]

    # Створюємо корінь і підпапки
    dest.mkdir(parents=True, exist_ok=True)
    subdirs = []
    for i in range(1, n_subdirs + 1):
        sd = dest / f"folder_{i}"
        sd.mkdir(exist_ok=True)
        subdirs.append(sd)

    # Генеруємо файли (частину — в корені, частину — у випадкових підпапках)
    created = 0
    for i in range(n_files):
        parent = choice([dest] + subdirs)   # куди покласти файл
        ext = choice(exts)
        name = f"file_{i}_{uuid4().hex[:8]}"
        if ext:
            filename = f"{name}.{ext}"
        else:
            filename = name  # без розширення

        path = parent / filename

        # Контент — простий текст (навіть для «картинок» це ок, нам важливе лише розширення)
        lines = randint(2, 8)
        content = "\n".join([f"Sample line {j} for {filename}" for j in range(lines)])
        path.write_text(content, encoding="utf-8")
        created += 1

    print(f"✅ Створено: {created} файлів у «{dest}» (з підпапками: {n_subdirs}).")


if __name__ == "__main__":
    main()