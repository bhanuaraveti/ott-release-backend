# -*- coding: utf-8 -*-
"""Thin TMDB v3 client using a v4 read access token (Bearer auth).

Only exposes the two endpoints the enrichment pipeline needs:
  - search_movie(name, year=None)  -> best match dict or None
  - get_movie(tmdb_id)             -> full movie payload with credits

Retries 5xx and transient requests.RequestException with exponential backoff.
404s are surfaced immediately (no point retrying a "not found").
"""
from __future__ import annotations

import logging
import os
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

BASE_URL = "https://api.themoviedb.org/3"
DEFAULT_TIMEOUT = 10  # seconds


class TMDBError(RuntimeError):
    """Raised on a non-retryable TMDB failure (auth, 404, malformed response)."""


def _read_token() -> str:
    token = os.environ.get("TMDB_READ_TOKEN")
    if not token:
        raise TMDBError(
            "TMDB_READ_TOKEN env var is not set. "
            "Generate a v4 read access token at themoviedb.org -> Settings -> API."
        )
    return token


def _should_retry(exc: BaseException) -> bool:
    """Retry on 5xx and transport errors. Do NOT retry on 4xx (incl. 404)."""
    if isinstance(exc, requests.HTTPError):
        resp = exc.response
        if resp is None:
            return True
        return 500 <= resp.status_code < 600
    if isinstance(exc, requests.RequestException):
        return True
    return False


_retry = retry(
    retry=retry_if_exception(_should_retry),
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=8),
    reraise=True,
)


@_retry
def _get(path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    headers = {
        "Authorization": f"Bearer {_read_token()}",
        "Accept": "application/json",
    }
    resp = requests.get(
        f"{BASE_URL}{path}",
        params=params or {},
        headers=headers,
        timeout=DEFAULT_TIMEOUT,
    )
    # Log request + response for visibility during backfill. The query string
    # is already in resp.url, including the v4 token is NOT (it's a header).
    logger.info("TMDB %s -> %s", resp.url, resp.status_code)
    resp.raise_for_status()
    return resp.json()


def search_movie(name: str, year: int | None = None) -> dict | None:
    """Return the first/best result from /search/movie, or None if zero results.

    TMDB already sorts by popularity, so results[0] is the pragmatic "best match".
    """
    if not name or not name.strip():
        return None

    params: dict[str, Any] = {
        "query": name.strip(),
        "include_adult": "false",
        "language": "en-US",
    }
    if year is not None:
        params["year"] = year

    try:
        data = _get("/search/movie", params=params)
    except requests.HTTPError as e:
        # 404 on /search is unusual; treat as "no results". Auth failures propagate.
        if e.response is not None and e.response.status_code == 404:
            return None
        raise

    results = data.get("results") or []
    if not results:
        # Retry once without year — TMDB's year filter is strict and misses
        # titles whose release_date year differs from the OTT availability year.
        if year is not None:
            params.pop("year", None)
            data = _get("/search/movie", params=params)
            results = data.get("results") or []
    return results[0] if results else None


def get_movie(tmdb_id: int) -> dict:
    """Fetch a movie with credits appended. Raises on 404 (caller decides)."""
    return _get(f"/movie/{tmdb_id}", params={"append_to_response": "credits"})


if __name__ == "__main__":
    # Tiny live smoke test — only runs if TMDB_READ_TOKEN is set in env.
    # Avoids spamming the API in CI/import paths.
    import json

    if not os.environ.get("TMDB_READ_TOKEN"):
        print("[skip] TMDB_READ_TOKEN not set; skipping live call.")
    else:
        hit = search_movie("Hanu-Man", 2024)
        if hit is None:
            print("[warn] search returned None for 'Hanu-Man' 2024")
        else:
            print("[ok] search hit:", hit.get("title"), "id=", hit.get("id"))
            full = get_movie(hit["id"])
            genres = [g["name"] for g in full.get("genres", [])]
            print("[ok] get_movie genres:", genres)
            print("[ok] credits keys:", list((full.get("credits") or {}).keys()))
            # Compact echo for visual confirmation.
            print(json.dumps({"id": full["id"], "title": full.get("title")}))
