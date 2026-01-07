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
from flask import Flask, jsonify, Response
from flask_cors import CORS
import os
import json
import subprocess
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

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

@app.route("/sitemap.xml", methods=["GET"])
def get_sitemap():
    """Generate dynamic sitemap with actual lastmod date from data file."""
    try:
        # Get last modification time of movies.json
        if os.path.exists(DATA_FILE):
            lastmod_timestamp = os.path.getmtime(DATA_FILE)
            lastmod_date = datetime.fromtimestamp(lastmod_timestamp).strftime('%Y-%m-%d')
        else:
            lastmod_date = datetime.now().strftime('%Y-%m-%d')

        base_url = "https://telugumoviesott.onrender.com"

        sitemap_xml = f'''<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url>
    <loc>{base_url}/</loc>
    <lastmod>{lastmod_date}</lastmod>
    <changefreq>daily</changefreq>
    <priority>1.0</priority>
  </url>
  <url>
    <loc>{base_url}/about</loc>
    <lastmod>{lastmod_date}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.8</priority>
  </url>
  <url>
    <loc>{base_url}/privacy</loc>
    <lastmod>{lastmod_date}</lastmod>
    <changefreq>monthly</changefreq>
    <priority>0.5</priority>
  </url>
</urlset>'''

        return Response(sitemap_xml, mimetype='application/xml')

    except Exception as e:
        print(f"Error generating sitemap: {str(e)}")
        return Response("Error generating sitemap", status=500)

@app.route("/robots.txt", methods=["GET"])
def get_robots():
    """Serve robots.txt file."""
    robots_txt = '''User-agent: *
Allow: /
Sitemap: https://telugumoviesott.onrender.com/sitemap.xml
'''
    return Response(robots_txt, mimetype='text/plain')

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