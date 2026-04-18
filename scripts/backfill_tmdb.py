#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""One-shot TMDB backfill for data/movies.json.

Usage (from repo root):
    python scripts/backfill_tmdb.py          # enrich records lacking tmdb_id
    python scripts/backfill_tmdb.py --all    # re-enrich everything

Writes atomically: dump to a temp file in the same directory, then rename.
Prints a summary at the end: total, ok, not_found, error.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from collections import Counter

# Allow `python scripts/backfill_tmdb.py` from repo root by putting the repo
# on sys.path so `enrich` / `tmdb_client` resolve.
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from enrich import enrich_all, enrich_new_movies  # noqa: E402

DATA_FILE = os.path.join(REPO_ROOT, "data", "movies.json")


def _atomic_write_json(path: str, payload) -> None:
    dir_ = os.path.dirname(path) or "."
    fd, tmp = tempfile.mkstemp(prefix=".movies_", suffix=".json.tmp", dir=dir_)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
    )
    parser = argparse.ArgumentParser(description="Backfill TMDB metadata into data/movies.json")
    parser.add_argument(
        "--all",
        action="store_true",
        help="Re-enrich every record (default: only records missing tmdb_id).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process at most N records (handy for smoke tests). "
             "Applies to the filtered subset when --all is absent.",
    )
    parser.add_argument(
        "--data-file",
        default=DATA_FILE,
        help="Path to movies.json (default: data/movies.json).",
    )
    args = parser.parse_args()

    if not os.path.exists(args.data_file):
        print(f"[fatal] data file not found: {args.data_file}", file=sys.stderr)
        return 2

    with open(args.data_file, "r", encoding="utf-8") as f:
        records = json.load(f)

    if args.limit is not None:
        # Slice the records list itself so we write back the full set —
        # just fewer of them enriched on this pass.
        if args.all:
            subset = records[: args.limit]
            enrich_all(subset)
        else:
            # Filter then slice, but only enrich the sliced subset.
            need = [r for r in records if not r.get("tmdb_id")][: args.limit]
            enrich_new_movies(need)
    else:
        if args.all:
            enrich_all(records)
        else:
            enrich_new_movies(records)

    _atomic_write_json(args.data_file, records)

    stats = Counter(r.get("enrichment_status") for r in records if "enrichment_status" in r)
    total = len(records)
    print(
        f"[done] total={total} "
        f"ok={stats.get('ok', 0)} "
        f"not_found={stats.get('not_found', 0)} "
        f"error={stats.get('error', 0)} "
        f"unprocessed={total - sum(stats.values())}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
