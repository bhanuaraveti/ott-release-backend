# -*- coding: utf-8 -*-
"""
Telugu Movies OTT Release Backend API

Virtual Environment: /Users/aravetibhanu/projects/virtualenvs/python3.10
Python Version: 3.10.19

To run locally:
    source /Users/aravetibhanu/projects/virtualenvs/python3.10/bin/activate
    python app.py

Server runs on port 5001
"""
from flask import Flask, jsonify, request, Response
from flask_cors import CORS
import logging
import os
import json
import subprocess
import threading
from datetime import datetime

from deploy_hook import trigger_frontend_rebuild
from scrapper import scrape_movies

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
)

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Start the in-process daily scraper when explicitly enabled. Gate with an env
# var so that only one gunicorn worker schedules (set RUN_SCHEDULER=1 and run
# gunicorn with --workers=1 in production).
if os.environ.get("RUN_SCHEDULER") == "1":
    from scheduler import start_scheduler
    start_scheduler()

# File path for reading the movie data
DATA_DIR = "data"
DATA_FILE = os.path.join(DATA_DIR, "movies.json")

def ensure_data_exists():
    """Checks if data file exists, and runs scraper if it doesn't."""
    if not os.path.exists(DATA_FILE):
        print("[INFO] Movie data file not found. Running scrapper...")
        try:
            # Run the scrapper.py script to fetch and save data
            result = subprocess.run(["python", "scrapper.py"],
                                capture_output=True, text=True, check=True)
            print(f"[INFO] Scrapper output: {result.stdout}")
            return True
        except subprocess.CalledProcessError as e:
            print(f"[ERROR] Error running scrapper: {e}")
            print(f"Stderr: {e.stderr}")
            return False
    return True

# API Route to Fetch Movies
@app.route("/movies", methods=["GET"])
def get_movies():
    try:
        # Ensure data file exists, run scrapper if necessary
        if not ensure_data_exists():
            return jsonify({"error": "Could not generate movie data"}), 500
            
        # If file still doesn't exist after running scrapper
        if not os.path.exists(DATA_FILE):
            return jsonify({"error": "Movie data not found"}), 404
        
        # Read movies from JSON file
        with open(DATA_FILE, 'r', encoding='utf-8') as file:
            movies = json.load(file)
            
        return jsonify(movies)
    
    except Exception as e:
        print(f"Error reading movie data: {str(e)}")
        return jsonify({"error": "Failed to load movie data"}), 500

_scrape_lock = threading.Lock()
_scrape_state = {"running": False, "last_finished_at": None, "last_error": None}


def _run_scrape_and_rebuild():
    """Run the scrape + TMDB enrichment, then fire the frontend rebuild hook.

    Runs in a background thread so the HTTP request returns immediately — a
    full scrape can take minutes and Render's HTTP timeout is ~30s.
    """
    logger = logging.getLogger(__name__)
    try:
        logger.info("Manual scrape starting")
        scrape_movies()
        logger.info("Manual scrape finished; triggering frontend rebuild")
        success, message = trigger_frontend_rebuild()
        if success:
            logger.info("Frontend deploy hook triggered: %s", message)
        else:
            logger.warning("Frontend deploy hook not triggered: %s", message)
        _scrape_state["last_error"] = None
    except Exception as exc:  # noqa: BLE001 - background job must not crash worker
        logger.exception("Manual scrape failed")
        _scrape_state["last_error"] = str(exc)
    finally:
        _scrape_state["last_finished_at"] = datetime.utcnow().isoformat() + "Z"
        _scrape_state["running"] = False


@app.route("/admin/run-scrape", methods=["POST"])
def run_scrape():
    """Manual scrape + enrich + frontend rebuild trigger.

    Same auth model as /admin/rebuild-frontend. Fires the work in a
    background thread and returns 202 immediately. Poll the same endpoint
    with GET to check status.
    """
    expected_secret = os.environ.get("ADMIN_REBUILD_SECRET")
    if not expected_secret:
        return jsonify({"error": "admin scrape disabled"}), 503

    provided_secret = request.headers.get("X-Admin-Secret")
    if not provided_secret or provided_secret != expected_secret:
        return jsonify({"error": "unauthorized"}), 401

    if not _scrape_lock.acquire(blocking=False):
        return jsonify({"error": "scrape already running"}), 409
    try:
        if _scrape_state["running"]:
            return jsonify({"error": "scrape already running"}), 409
        _scrape_state["running"] = True
    finally:
        _scrape_lock.release()

    thread = threading.Thread(target=_run_scrape_and_rebuild, daemon=True)
    thread.start()
    return jsonify({"status": "started"}), 202


@app.route("/admin/run-scrape", methods=["GET"])
def run_scrape_status():
    """Return the state of the most recent manual scrape."""
    expected_secret = os.environ.get("ADMIN_REBUILD_SECRET")
    if not expected_secret:
        return jsonify({"error": "admin scrape disabled"}), 503

    provided_secret = request.headers.get("X-Admin-Secret")
    if not provided_secret or provided_secret != expected_secret:
        return jsonify({"error": "unauthorized"}), 401

    return jsonify(_scrape_state), 200


@app.route("/admin/rebuild-frontend", methods=["POST"])
def rebuild_frontend():
    """Manual deploy-hook trigger, guarded by a shared secret header.

    - If ADMIN_REBUILD_SECRET is unset: 503 (feature disabled).
    - If set but X-Admin-Secret header is missing or wrong: 401.
    - On success: 200 {"status": "triggered"}.
    - On hook failure: 502 {"error": "..."}.
    """
    expected_secret = os.environ.get("ADMIN_REBUILD_SECRET")
    if not expected_secret:
        return jsonify({"error": "admin rebuild disabled"}), 503

    provided_secret = request.headers.get("X-Admin-Secret")
    if not provided_secret or provided_secret != expected_secret:
        return jsonify({"error": "unauthorized"}), 401

    success, message = trigger_frontend_rebuild()
    if success:
        return jsonify({"status": "triggered", "detail": message}), 200
    return jsonify({"error": message}), 502


@app.route("/rss.xml", methods=["GET"])
def get_rss():
    """Generate RSS feed for latest movie releases."""
    try:
        # Load movies data
        if not os.path.exists(DATA_FILE):
            return Response("RSS feed unavailable", status=404)

        with open(DATA_FILE, 'r', encoding='utf-8') as file:
            movies = json.load(file)

        # Get last modification time
        lastmod_timestamp = os.path.getmtime(DATA_FILE)
        lastmod_date = datetime.fromtimestamp(lastmod_timestamp).strftime('%a, %d %b %Y %H:%M:%S GMT')

        # Get latest 50 movies for RSS feed
        latest_movies = movies[:50]

        # Build RSS XML
        rss_items = []
        for movie in latest_movies:
            title = movie.get('name', 'Unknown Movie')
            platform = movie.get('platform', 'Unknown')
            available_on = movie.get('available_on', 'TBA')
            movie_type = movie.get('type', 'Movie')
            imdb_rating = movie.get('imdb_rating', 'N/A')

            description = f"{movie_type} - Available on {platform} from {available_on}"
            if imdb_rating != 'N/A':
                description += f" | IMDb: {imdb_rating}"

            # Create a unique guid for each movie
            guid = f"telugumoviesott-{title.replace(' ', '-').lower()}-{platform.replace(' ', '-').lower()}"

            rss_items.append(f'''    <item>
      <title>{title}</title>
      <description>{description}</description>
      <link>https://telugumoviesott.onrender.com/</link>
      <guid isPermaLink="false">{guid}</guid>
      <pubDate>{lastmod_date}</pubDate>
      <category>{movie_type}</category>
    </item>''')

        rss_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title>TeluguMoviesOTT - Latest Telugu Movie OTT Releases</title>
    <link>https://telugumoviesott.onrender.com/</link>
    <description>Stay updated with the latest Telugu movies on OTT platforms including Netflix, Prime Video, Hotstar, Aha, and more.</description>
    <language>en-us</language>
    <lastBuildDate>{lastmod_date}</lastBuildDate>
    <atom:link href="https://ott-release-backend.onrender.com/rss.xml" rel="self" type="application/rss+xml" />
    <image>
      <url>https://telugumoviesott.onrender.com/og-image.png</url>
      <title>TeluguMoviesOTT</title>
      <link>https://telugumoviesott.onrender.com/</link>
    </image>
{chr(10).join(rss_items)}
  </channel>
</rss>'''

        return Response(rss_xml, mimetype='application/rss+xml')

    except Exception as e:
        print(f"Error generating RSS feed: {str(e)}")
        return Response("Error generating RSS feed", status=500)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)