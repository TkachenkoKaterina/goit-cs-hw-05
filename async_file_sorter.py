#!/usr/bin/env python3
"""
async_file_sorter.py

Асинхронно читає всі файли у вихідній папці (рекурсивно) та копіює їх у цільову папку,
розкладаючи за підпапками відповідно до розширень файлів.

Приклади:
    python async_file_sorter.py --source /path/to/src --output /path/to/dest
    python async_file_sorter.py -s src_dir -o out_dir --workers 16
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import shutil
import sys
import time
from pathlib import Path
from typing import Iterable, List, Tuple


# ---------------------------
# Налаштування логування
# ---------------------------
logger = logging.getLogger("async_file_sorter")


def setup_logging(verbosity: int) -> None:
    level = logging.WARNING
    if verbosity == 1:
        level = logging.INFO
    elif verbosity >= 2:
        level = logging.DEBUG

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%H:%M:%S",
    )


# ---------------------------
# Корисні функції
# ---------------------------
def files_recursive(src: Path) -> List[Path]:
    """Синхронно отримує список усіх файлів рекурсивно."""
    return [p for p in src.rglob("*") if p.is_file()]


def normalized_ext(path: Path) -> str:
    """
    Повертає назву підпапки для файлу за його розширенням.
    Напр., 'report.PDF' -> 'pdf', без розширення -> '_no_ext'
    """
    ext = path.suffix.lower().lstrip(".")
    return ext if ext else "_no_ext"


# ---------------------------
# Копіювання (у пул потоків)
# ---------------------------
async def copy_file(src: Path, out_root: Path, sem: asyncio.Semaphore) -> Tuple[Path, Path] | None:
    """
    Копіює файл у підпапку (за розширенням) всередині out_root.
    Повертає (src, dst) у разі успіху, або None у разі помилки (логуються).
    """
    subdir = normalized_ext(src)
    dst_dir = out_root / subdir
    dst = dst_dir / src.name

    try:
        async with sem:
            # Створення каталогу призначення
            await asyncio.to_thread(dst_dir.mkdir, parents=True, exist_ok=True)
            # Копіювання з метаданими
            await asyncio.to_thread(shutil.copy2, src, dst)
        logger.debug("Copied: %s -> %s", src, dst)
        return src, dst
    except Exception as exc:  # noqa: BLE001
        logger.error("Помилка копіювання '%s': %s", src, exc)
        return None


# ---------------------------
# Рекурсивне читання + запуск копій
# ---------------------------
async def read_folder(src: Path, out_root: Path, workers: int) -> Tuple[int, int]:
    """
    Рекурсивно читає всі файли з src і асинхронно копіює в out_root.
    Повертає (успішно_скопійовано, помилок).
    """
    # Отримуємо список файлів у пулі потоків (щоб не блокувати цикл)
    all_files = await asyncio.to_thread(files_recursive, src)
    logger.info("Знайдено файлів: %d", len(all_files))

    sem = asyncio.Semaphore(workers)
    tasks = [asyncio.create_task(copy_file(p, out_root, sem)) for p in all_files]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    ok = 0
    err = 0
    for res in results:
        if isinstance(res, Exception):
            logger.error("Непередбачена помилка: %s", res)
            err += 1
        elif res is None:
            err += 1
        else:
            ok += 1

    return ok, err


# ---------------------------
# CLI
# ---------------------------
def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Асинхронне сортування файлів за розширеннями."
    )
    parser.add_argument(
        "-s", "--source",
        required=True,
        type=Path,
        help="Вихідна папка (рекурсивно читається)."
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        type=Path,
        help="Цільова папка (сюди будуть скопійовані файли за підпапками)."
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Кількість одночасних копіювань (стандартно 8)."
    )
    parser.add_argument(
        "-v", "--verbose",
        action="count",
        default=0,
        help="Рівень деталізації логів: -v (INFO), -vv (DEBUG)."
    )
    return parser.parse_args(argv)


async def amain(args: argparse.Namespace) -> int:
    if not args.source.exists() or not args.source.is_dir():
        logger.error("Вихідна папка не існує або не є директорією: %s", args.source)
        return 2

    # Створимо цільовий каталог, якщо його нема
    try:
        await asyncio.to_thread(args.output.mkdir, parents=True, exist_ok=True)
    except Exception as exc:  # noqa: BLE001
        logger.error("Не вдалося створити цільову папку '%s': %s", args.output, exc)
        return 2

    start = time.perf_counter()
    ok, err = await read_folder(args.source, args.output, args.workers)
    dur = time.perf_counter() - start

    logger.info("Успішно скопійовано: %d, помилок: %d, час: %.2fs", ok, err, dur)
    print(f"✅ Done. Copied: {ok}, errors: {err}, time: {dur:.2f}s")
    return 0 if err == 0 else 1


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    setup_logging(args.verbose)
    try:
        raise SystemExit(asyncio.run(amain(args)))
    except KeyboardInterrupt:
        logger.warning("Перервано користувачем (Ctrl+C).")
        raise SystemExit(130)


if __name__ == "__main__":
    main()