from __future__ import annotations

import argparse
import ast
import gzip
import json
import re
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd


TEXT_COLUMNS_PRIORITY = [
    "quote",
    "quotes",
    "text",
    "content",
]

AUTHOR_COLUMNS_PRIORITY = [
    "author",
    "author_name",
    "writer",
]

TAGS_COLUMNS_PRIORITY = [
    "tags",
    "tag",
]


def standardize_col_name(name: str) -> str:
    name = name.strip().lower()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    return name.strip("_")


def normalize_text(value: object) -> str:
    if value is None:
        return ""

    if isinstance(value, (list, tuple, dict, set, np.ndarray)):
        return normalize_text(" ".join(map(str, value)))

    if pd.isna(value):
        return ""

    text = str(value)
    text = text.replace("\ufeff", "")
    text = text.replace("�", "")
    text = text.replace("“", '"').replace("”", '"')
    text = text.replace("‘", "'").replace("’", "'")
    text = re.sub(r"\s+", " ", text)
    return text.strip().strip('"')


def parse_tags(value: object) -> list[str]:
    if value is None:
        return []

    if isinstance(value, np.ndarray):
        raw_items = value.tolist()
    elif isinstance(value, list):
        raw_items = value
    else:
        if pd.isna(value):
            return []

        raw = str(value).strip()
        if not raw:
            return []

        # Try to parse a serialized list first, fallback to separators.
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = ast.literal_eval(raw)
                raw_items = parsed if isinstance(parsed, list) else [raw]
            except (ValueError, SyntaxError):
                raw_items = re.split(r"[,;|]", raw)
        else:
            raw_items = re.split(r"[,;|]", raw)

    clean = []
    for item in raw_items:
        tag = normalize_text(item).lower()
        if tag:
            clean.append(tag)

    # Preserve order while removing duplicates.
    return list(dict.fromkeys(clean))


def detect_column(df: pd.DataFrame, priorities: Iterable[str]) -> str | None:
    cols = set(df.columns)
    for col in priorities:
        if col in cols:
            return col
    return None


def read_csv_with_fallback(path: Path) -> pd.DataFrame:
    # Pandas reads .csv.gz natively — compression is auto-detected.
    for encoding in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(path, encoding=encoding)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path)


def read_source(path: Path) -> pd.DataFrame:
    suffixes = [s.lower() for s in path.suffixes]

    # .csv or .csv.gz
    if suffixes[-1] == ".csv" or suffixes[-2:] == [".csv", ".gz"]:
        return read_csv_with_fallback(path)

    # .json or .json.gz
    if suffixes[-1] == ".json" or suffixes[-2:] == [".json", ".gz"]:
        if suffixes[-1] == ".gz":
            with gzip.open(path, "rt", encoding="utf-8") as f:
                data = json.load(f)
        else:
            data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, list):
            return pd.DataFrame(data)
        if isinstance(data, dict):
            return pd.json_normalize(data)
        raise ValueError(f"Unsupported JSON structure: {path}")

    raise ValueError(f"Unsupported file type: {path}")


def normalize_quotes_frame(df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
    renamed = {col: standardize_col_name(col) for col in df.columns}
    df = df.rename(columns=renamed)

    quote_col = detect_column(df, TEXT_COLUMNS_PRIORITY)
    if quote_col is None:
        return pd.DataFrame()

    author_col = detect_column(df, AUTHOR_COLUMNS_PRIORITY)
    tags_col = detect_column(df, TAGS_COLUMNS_PRIORITY)

    normalized = pd.DataFrame()
    normalized["quote"] = df[quote_col].map(normalize_text)
    normalized["author"] = (
        df[author_col].map(normalize_text) if author_col else "unknown"
    )
    normalized["tags"] = df[tags_col].map(parse_tags) if tags_col else [[] for _ in range(len(df))]

    # Keep source metadata to support lineage and quality checks.
    normalized["source_file"] = str(source_file.relative_to(source_file.parents[1]))
    normalized["source_ext"] = source_file.suffix.lower().lstrip(".")

    for optional_col in ("category", "type", "year", "movie", "book", "popularity"):
        if optional_col in df.columns:
            normalized[optional_col] = df[optional_col]

    normalized = normalized[normalized["quote"].str.len() > 0].copy()
    normalized["author"] = normalized["author"].replace("", "unknown")

    return normalized


def collect_quote_files(quotes_dir: Path) -> list[Path]:
    candidates = []
    for ext in ("*.csv", "*.json", "*.csv.gz", "*.json.gz"):
        candidates.extend(quotes_dir.rglob(ext))
    return sorted(path for path in candidates if path.is_file())


def build_unified_base(quotes_dir: Path) -> pd.DataFrame:
    frames = []
    for source_file in collect_quote_files(quotes_dir):
        try:
            raw = read_source(source_file)
            cleaned = normalize_quotes_frame(raw, source_file)
            if not cleaned.empty:
                frames.append(cleaned)
        except Exception as exc:
            print(f"[warn] failed to parse {source_file}: {exc}")

    if not frames:
        return pd.DataFrame(columns=["quote", "author", "tags", "source_file", "source_ext"])

    unified = pd.concat(frames, ignore_index=True)

    # De-duplicate using a deterministic key.
    unified["dedupe_key"] = (
        unified["quote"].str.lower().str.strip()
        + "||"
        + unified["author"].str.lower().str.strip()
    )
    unified = unified.drop_duplicates(subset="dedupe_key").drop(columns=["dedupe_key"])

    unified = unified.sort_values(by=["author", "quote"]).reset_index(drop=True)
    return unified


def write_outputs(unified: pd.DataFrame, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    csv_path = output_dir / "quotes_unified.csv"
    json_path = output_dir / "quotes_unified.json"
    summary_path = output_dir / "quotes_summary.csv"

    unified.to_csv(csv_path, index=False, encoding="utf-8")
    unified.to_json(json_path, orient="records", force_ascii=False, indent=2)

    summary = pd.DataFrame(
        {
            "metric": [
                "total_quotes",
                "unique_authors",
                "files_used",
                "avg_quote_length",
            ],
            "value": [
                int(len(unified)),
                int(unified["author"].nunique()),
                int(unified["source_file"].nunique()),
                float(unified["quote"].str.len().mean()),
            ],
        }
    )
    summary.to_csv(summary_path, index=False)

    top_authors = (
        unified.groupby("author", as_index=False)
        .size()
        .sort_values("size", ascending=False)
        .head(20)
    )
    top_authors.to_csv(output_dir / "top_20_authors.csv", index=False)

    tags_exploded = unified.explode("tags")
    tags_exploded = tags_exploded[tags_exploded["tags"].notna() & (tags_exploded["tags"] != "")]
    top_tags = (
        tags_exploded.groupby("tags", as_index=False)
        .size()
        .sort_values("size", ascending=False)
        .head(30)
    )
    top_tags.to_csv(output_dir / "top_30_tags.csv", index=False)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a unified quotes dataset from multiple CSV/JSON files."
    )
    parser.add_argument(
        "--quotes-dir",
        default="quotes",
        help="Directory containing quote files (default: quotes)",
    )
    parser.add_argument(
        "--output-dir",
        default="data/processed",
        help="Output directory for unified dataset and summaries",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    quotes_dir = Path(args.quotes_dir)
    output_dir = Path(args.output_dir)

    if not quotes_dir.exists():
        raise FileNotFoundError(f"Quotes directory not found: {quotes_dir}")

    unified = build_unified_base(quotes_dir)
    if unified.empty:
        print("No valid quote rows were found.")
        return

    write_outputs(unified, output_dir)

    print("Unified base generated successfully")
    print(f"quotes: {len(unified)}")
    print(f"authors: {unified['author'].nunique()}")
    print(f"files: {unified['source_file'].nunique()}")
    print(f"output_dir: {output_dir}")


if __name__ == "__main__":
    main()