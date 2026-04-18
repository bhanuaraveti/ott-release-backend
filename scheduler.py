# -*- coding: utf-8 -*-
"""In-process daily scraper scheduler.

Runs inside the gunicorn web process. See TODO.md for the migration path to a
dedicated worker + durable storage when the project outgrows this.
"""
import logging
import os

from apscheduler.schedulers.background import BackgroundScheduler

from deploy_hook import trigger_frontend_rebuild
from scrapper import scrape_movies

logger = logging.getLogger(__name__)

SCRAPE_HOUR_UTC = int(os.environ.get("SCRAPE_HOUR_UTC", "20"))
SCRAPE_MINUTE_UTC = int(os.environ.get("SCRAPE_MINUTE_UTC", "30"))


def _run_scrape():
    logger.info("Scheduled scrape starting")
    try:
        scrape_movies()
    except Exception:
        logger.exception("Scheduled scrape failed; skipping frontend rebuild")
        return

    logger.info("Scheduled scrape finished; triggering frontend rebuild")
    success, message = trigger_frontend_rebuild()
    if success:
        logger.info("Frontend deploy hook triggered: %s", message)
    else:
        logger.warning("Frontend deploy hook not triggered: %s", message)


def start_scheduler():
    """Start the background scheduler. Safe to call once per process."""
    scheduler = BackgroundScheduler(timezone="UTC")
    scheduler.add_job(
        _run_scrape,
        trigger="cron",
        hour=SCRAPE_HOUR_UTC,
        minute=SCRAPE_MINUTE_UTC,
        id="daily_scrape",
        max_instances=1,
        coalesce=True,
        misfire_grace_time=3600,
    )
    scheduler.start()
    logger.info(
        "Scheduler started; daily scrape at %02d:%02d UTC",
        SCRAPE_HOUR_UTC,
        SCRAPE_MINUTE_UTC,
    )
    return scheduler
