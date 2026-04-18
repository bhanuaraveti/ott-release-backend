# -*- coding: utf-8 -*-
"""TMDB enrichment for scraped movie records.

See the AdSense remediation plan §Data Contract for the target record shape.

Rate limiting:
  TMDB's published limit is 40 requests / 10 seconds. Each enriched record
  costs ~2 calls (search + detail), so a hard sleep of 0.3s between records
  caps us at ~6.6 records/sec (~13 req/sec), well under the ceiling with
  headroom for retries. We keep it simple rather than a token bucket; the
  daily path touches few records, and the one-shot backfill is infrequent.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Iterable

import requests
from slugify import slugify

from tmdb_client import TMDBError, get_movie, search_movie

logger = logging.getLogger(__name__)

POSTER_BASE = "https://image.tmdb.org/t/p/w500"
# TMDB's published limit is 40 req / 10s = 4 req/s average. Each record
# costs ~2 calls (search + detail), so a 0.55s sleep caps us at ~3.6 req/s —
# safely under the ceiling with headroom for the occasional year-retry.
RATE_LIMIT_SLEEP_SEC = 0.55


def _now_iso_z() -> str:
    """UTC now as RFC 3339 / ISO 8601 with a trailing 'Z'."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _extract_year(available_on: str | None) -> int | None:
    """Best-effort year parse from the scraper's available_on string.

    Supports the ISO form '2024-02-23' (plan example) and the current scraper
    output '17 Apr 2026'. Returns None for 'Soon', empty, or unparseable.
    """
    if not available_on:
        return None
    s = available_on.strip()
    if not s or s.lower() in {"soon", "coming soon", "tba"}:
        return None

    for fmt in ("%Y-%m-%d", "%d %b %Y", "%d %B %Y", "%b %Y", "%B %Y"):
        try:
            return datetime.strptime(s, fmt).year
        except ValueError:
            continue

    # Last-ditch: grab a 4-digit year token if present.
    for tok in s.replace(",", " ").split():
        if len(tok) == 4 and tok.isdigit():
            y = int(tok)
            if 1900 <= y <= 2100:
                return y
    return None


def _director_from_crew(crew: list[dict]) -> str | None:
    for person in crew or []:
        if person.get("job") == "Director":
            name = person.get("name")
            if name:
                return name
    return None


def _top_cast(cast: list[dict], limit: int = 5) -> list[dict]:
    out: list[dict] = []
    for person in (cast or [])[:limit]:
        name = person.get("name")
        if not name:
            continue
        out.append({"name": name, "character": person.get("character") or None})
    return out


def _genres(raw: list[dict]) -> list[str]:
    return [g["name"] for g in raw or [] if g.get("name")]


def _assign_slug(record: dict, existing_slugs: set[str]) -> str:
    """Stable slug for the record; append -<year> on collision.

    Persists the slug on the record (so later renames of `name` do not move
    the canonical URL).
    """
    if record.get("slug"):
        # Already assigned previously — keep it to preserve URLs.
        existing_slugs.add(record["slug"])
        return record["slug"]

    base = slugify(record.get("name") or "untitled")
    if not base:
        base = "untitled"

    slug = base
    if slug in existing_slugs:
        year = _extract_year(record.get("available_on"))
        if year is not None:
            slug = f"{base}-{year}"
        # If still colliding (very rare), append -2, -3, ...
        i = 2
        while slug in existing_slugs:
            slug = f"{base}-{i}"
            i += 1

    existing_slugs.add(slug)
    record["slug"] = slug
    return slug


def _apply_not_found(record: dict) -> dict:
    record["tmdb_id"] = None
    record["overview"] = None
    record["poster_path"] = None
    record["poster_url"] = None
    record["release_date"] = None
    record["runtime"] = None
    record["genres"] = []
    record["cast"] = []
    record["director"] = None
    record["tmdb_checked_at"] = _now_iso_z()
    record["enrichment_status"] = "not_found"
    return record


def _apply_error(record: dict) -> dict:
    # Leave any previously-fetched fields alone; just mark the latest attempt.
    record.setdefault("tmdb_id", None)
    record.setdefault("overview", None)
    record.setdefault("poster_path", None)
    record.setdefault("poster_url", None)
    record.setdefault("release_date", None)
    record.setdefault("runtime", None)
    record.setdefault("genres", [])
    record.setdefault("cast", [])
    record.setdefault("director", None)
    record["tmdb_checked_at"] = _now_iso_z()
    record["enrichment_status"] = "error"
    return record


def _apply_ok(record: dict, full: dict) -> dict:
    poster_path = full.get("poster_path") or None
    credits = full.get("credits") or {}

    record["tmdb_id"] = full.get("id")
    record["overview"] = full.get("overview") or None
    record["poster_path"] = poster_path
    record["poster_url"] = f"{POSTER_BASE}{poster_path}" if poster_path else None
    record["release_date"] = full.get("release_date") or None
    record["runtime"] = full.get("runtime") or None
    record["genres"] = _genres(full.get("genres") or [])
    record["cast"] = _top_cast(credits.get("cast") or [])
    record["director"] = _director_from_crew(credits.get("crew") or [])
    record["tmdb_checked_at"] = _now_iso_z()
    record["enrichment_status"] = "ok"
    return record


def enrich_movie(record: dict) -> dict:
    """Enrich a single record in place (and return it).

    Does NOT assign a slug — slug assignment is collision-aware and happens
    at the batch level. Call sites that want a slug should invoke one of the
    batch functions below, or pre-seed `record['slug']`.
    """
    name = record.get("name") or ""
    year = _extract_year(record.get("available_on"))

    try:
        hit = search_movie(name, year=year)
    except (requests.RequestException, TMDBError):
        logger.warning("TMDB search failed for %r", name, exc_info=True)
        return _apply_error(record)

    if hit is None:
        return _apply_not_found(record)

    tmdb_id = hit.get("id")
    if not tmdb_id:
        return _apply_not_found(record)

    try:
        full = get_movie(int(tmdb_id))
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 404:
            return _apply_not_found(record)
        logger.warning("TMDB detail fetch failed for %r (id=%s)", name, tmdb_id, exc_info=True)
        return _apply_error(record)
    except (requests.RequestException, TMDBError):
        logger.warning("TMDB detail fetch failed for %r (id=%s)", name, tmdb_id, exc_info=True)
        return _apply_error(record)

    return _apply_ok(record, full)


def _seed_existing_slugs(records: Iterable[dict]) -> set[str]:
    return {r["slug"] for r in records if r.get("slug")}


def _enrich_each(records: list[dict], predicate) -> list[dict]:
    existing_slugs = _seed_existing_slugs(records)
    # Assign slugs up front so collisions are deterministic regardless of
    # which records we actually enrich on this pass.
    for r in records:
        _assign_slug(r, existing_slugs)

    targets = [r for r in records if predicate(r)]
    total = len(targets)
    for i, r in enumerate(targets):
        enrich_movie(r)
        status = r.get("enrichment_status") or "?"
        logger.info("[%d/%d] %s -> %s", i + 1, total, r.get("name"), status)
        if i < total - 1:
            time.sleep(RATE_LIMIT_SLEEP_SEC)
    return records


def enrich_new_movies(records: list[dict]) -> list[dict]:
    """Enrich only records that are missing a `tmdb_id` (or were never checked).

    Used by the daily scrape path; skips already-enriched rows to stay well
    inside the TMDB rate limit.
    """
    return _enrich_each(
        records,
        predicate=lambda r: not r.get("tmdb_id") and r.get("enrichment_status") != "not_found",
    )


def enrich_all(records: list[dict]) -> list[dict]:
    """Re-enrich every record. Used by the one-shot backfill script."""
    return _enrich_each(records, predicate=lambda r: True)
