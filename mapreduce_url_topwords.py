#!/usr/bin/env python3
"""
mapreduce_url_topwords.py

1) Завантажує текст за URL.
2) Виконує підрахунок частоти слів за парадигмою MapReduce:
   Map  -> розбиваємо текст на чанки і паралельно мапимо (ThreadPoolExecutor)
   Shuffle -> групуємо значення за ключем
   Reduce  -> сумуємо
3) Візуалізує топ-N слів matplotlib'ом (горизонтальна стовпчикова діаграма).

Приклади:
  python mapreduce_url_topwords.py --url https://www.gutenberg.org/cache/epub/1342/pg1342.txt
  python mapreduce_url_topwords.py --url https://example.com/text.txt --top 15 --workers 12
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import logging
import re
import sys
from collections import Counter, defaultdict
from typing import DefaultDict, Iterable, Iterator, List, Sequence, Tuple

import matplotlib.pyplot as plt
import requests


# ----------------------------
# Налаштування логування
# ----------------------------
logger = logging.getLogger("mapreduce_url_topwords")


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


# ----------------------------
# Завантаження тексту
# ----------------------------
def fetch_text(url: str, timeout: int = 30) -> str:
    """Завантажує текст із URL з коректним декодуванням."""
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or resp.encoding
        text = resp.text
        logger.info("Отримано %d символів із %s", len(text), url)
        return text
    except Exception as exc:  # noqa: BLE001
        logger.exception("Не вдалося завантажити текст: %s", exc)
        raise SystemExit(1)


# ----------------------------
# Токенізація/пре-процесинг
# ----------------------------
WORD_RE = re.compile(r"[a-zA-Zа-яА-ЯёЁіІїЇєЄ0-9']+", re.UNICODE)
DEFAULT_STOPWORDS = {
    "the", "and", "a", "an", "of", "to", "in", "on", "is", "it", "that", "this",
    "і", "й", "та", "а", "але", "або", "що", "це", "до", "від", "за", "на", "у", "в",
    "з", "із", "зі", "по", "як", "не", "є"
}


def tokenize(text: str) -> List[str]:
    """Виділяє слова (літери/цифри/апостроф), у нижній регістр."""
    return [m.group(0).lower() for m in WORD_RE.finditer(text)]


# ----------------------------
# Реалізація MapReduce (з конспекту) + потоки
# ----------------------------
def map_function(text: str) -> List[Tuple[str, int]]:
    """Map: перетворює текст у пари (слово, 1)."""
    words = text.split()
    return [(word, 1) for word in words]


def shuffle_function(mapped_values: Iterable[Tuple[str, int]]) -> Iterator[Tuple[str, List[int]]]:
    """Shuffle: групування значень за ключем."""
    shuffled: DefaultDict[str, List[int]] = defaultdict(list)
    for key, value in mapped_values:
        shuffled[key].append(value)
    return shuffled.items()


def reduce_function(shuffled_values: Iterable[Tuple[str, List[int]]]) -> dict[str, int]:
    """Reduce: сумування значень за ключем."""
    reduced: dict[str, int] = {}
    for key, values in shuffled_values:
        reduced[key] = sum(values)
    return reduced


def chunk_text(text: str, chunks: int) -> List[str]:
    """
    Грубе розбиття тексту на N частин за символами (для паралельної Map-фази).
    Цілий текст -> N підрядків.
    """
    if chunks <= 1:
        return [text]
    n = len(text)
    step = max(1, n // chunks)
    return [text[i : min(n, i + step)] for i in range(0, n, step)]


def map_reduce_parallel(
    text: str,
    workers: int = 8,
    min_len: int = 1,
    use_stopwords: bool = True,
) -> Counter:
    """
    Паралельна MapReduce:
      - Map (паралельно): map_function на чанках тексту
      - Shuffle: об’єднання mapped пар
      - Reduce: сумування
      Додатково: простий фільтр по довжині та стоп-словах.
    """
    # 1) Map (паралельно): розіб’ємо на чанки та мапимо
    parts = chunk_text(text, workers)
    logger.info("Map: частин для мапінгу: %d", len(parts))

    mapped_all: List[Tuple[str, int]] = []
    with cf.ThreadPoolExecutor(max_workers=workers) as pool:
        for mapped in pool.map(map_function, parts):
            mapped_all.extend(mapped)

    # 2) Shuffle
    shuffled = dict(shuffle_function(mapped_all))

    # 3) Reduce
    reduced = reduce_function(shuffled.items())

    # 4) Постобробка: токенізація/фільтри на випадок "сирого" розбиття
    #    (ми могли розбивати text.split(), але для надійності — ще раз нормалізуємо)
    normalized = Counter()
    stop = DEFAULT_STOPWORDS if use_stopwords else set()
    for word, cnt in reduced.items():
        # виділимо нормальні токени із слова (захист від сміття)
        toks = tokenize(word)
        for t in toks:
            if len(t) >= min_len and t not in stop:
                normalized[t] += cnt

    return normalized


# ----------------------------
# Візуалізація
# ----------------------------
def visualize_top_words(freq: Counter, top_n: int, out_png: str | None = None) -> None:
    """
    Будує горизонтальний bar chart для top_n слів.
    Якщо out_png задано — зберігає у файл, інакше показує вікно.
    """
    top = freq.most_common(top_n)
    if not top:
        logger.warning("Немає даних для візуалізації.")
        return

    words, counts = zip(*top)

    plt.figure(figsize=(10, 6))
    plt.barh(range(len(words)), counts, color="#87CEEB")
    plt.yticks(range(len(words)), words)
    plt.xlabel("Frequency")
    plt.ylabel("Words")
    plt.title(f"Top {top_n} Most Frequent Words")
    plt.gca().invert_yaxis()  # найчастіші зверху
    plt.tight_layout()

    if out_png:
        plt.savefig(out_png)
        logger.info("Графік збережено у: %s", out_png)
        plt.close()
    else:
        plt.show()


# ----------------------------
# CLI
# ----------------------------
def parse_args(argv: Iterable[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="MapReduce підрахунок частоти слів у тексті, завантаженому за URL, із візуалізацією топ-слів."
    )
    p.add_argument("--url", required=True, help="URL текстового ресурсу")
    p.add_argument("--top", type=int, default=10, help="Скільки топ-слів показати (default: 10)")
    p.add_argument("--workers", type=int, default=8, help="Кількість потоків для Map-фази (default: 8)")
    p.add_argument("--min-len", type=int, default=2, help="Мінімальна довжина слова для урахування")
    p.add_argument("--no-stopwords", action="store_true", help="Не використовувати стоп-слова")
    p.add_argument("--out", type=str, default=None, help="Шлях для збереження графіка (PNG). Якщо не задано — показати")
    p.add_argument("-v", "--verbose", action="count", default=0, help="Логи: -v (INFO), -vv (DEBUG)")
    return p.parse_args(argv)


def main(argv: Iterable[str] | None = None) -> None:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    setup_logging(args.verbose)

    text = fetch_text(args.url)

    freq = map_reduce_parallel(
        text=text,
        workers=args.workers,
        min_len=args.min_len,
        use_stopwords=not args.no_stopwords,
    )

    total_tokens = sum(freq.values())
    print(f"✅ Готово. Унікальних слів: {len(freq)}, токенів: {total_tokens}")
    visualize_top_words(freq, args.top, args.out)


if __name__ == "__main__":
    main()