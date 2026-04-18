# -*- coding: utf-8 -*-
"""Tiny helper for triggering the frontend's Render deploy hook.

Exports a single function used by both the scheduled scrape (scheduler.py)
and the manual admin endpoint (app.py). Failures are non-fatal: the caller
logs the returned message and moves on.
"""
import logging
import os

import requests

logger = logging.getLogger(__name__)

DEPLOY_HOOK_ENV = "FRONTEND_DEPLOY_HOOK"


def trigger_frontend_rebuild():
    """POST to the configured deploy hook. Returns (success, message)."""
    url = os.environ.get(DEPLOY_HOOK_ENV)
    if not url:
        return False, f"{DEPLOY_HOOK_ENV} not set"
    try:
        response = requests.post(url, timeout=10)
    except requests.RequestException as exc:
        return False, f"deploy hook request failed: {exc}"
    return response.ok, f"deploy hook responded {response.status_code}"
