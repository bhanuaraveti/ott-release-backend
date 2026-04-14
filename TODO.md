# TODO: migrate the daily scraper off the web process

The scraper currently runs in-process via APScheduler inside the gunicorn web
worker (see `scheduler.py`, wired up in `app.py`). This is a deliberate
short-term choice. The correct long-term design is:

1. **Dedicated scheduled worker.** On Render, use a Cron Job service that runs
   `python -m scrapper` (or equivalent) on a schedule and exits. The web
   service should not run batch work.
2. **Durable storage.** Move `data/movies.json` out of the local filesystem.
   Render web dynos have ephemeral disk, so scraped data is lost on every
   redeploy or restart. Options: managed Postgres (preferred), object storage
   (S3/R2), or a Render persistent disk add-on.
3. **Atomic writes + validation.** Write to a staging location, validate, then
   swap. Drop the `movies_backup_*.json` files — a real store gives you
   point-in-time recovery for free.
4. **Observability.** Structured stdout logs, failure alerting (Sentry or an
   email-on-non-zero-exit), and a parser-regression test in CI that runs the
   scraper against a fixture HTML page so we find out when the source site
   changes its markup.
5. **Config via env only.** No hardcoded paths, no venv assumptions. Already
   mostly done; keep it that way.

## Known limitations of the current setup

- Scrape runs only while the web dyno is awake. If Render sleeps the dyno, the
  job does not fire.
- `movies.json` is on ephemeral disk. Every redeploy wipes the latest scrape
  until the next scheduled run.
- If gunicorn is ever scaled past `--workers=1`, remove the `RUN_SCHEDULER=1`
  env var from all but one worker, or switch to a real scheduler. Running N
  schedulers will cause N concurrent scrapes.
- The web process shares CPU/memory with the scraper. A slow scrape degrades
  API latency.
