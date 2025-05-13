from flask import Flask, jsonify
from flask_cors import CORS
import os
import json
import subprocess

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# File path for reading the movie data
DATA_DIR = "data"
DATA_FILE = os.path.join(DATA_DIR, "movies.json")

def ensure_data_exists():
    """Checks if data file exists, and runs scraper if it doesn't."""
    if not os.path.exists(DATA_FILE):
        print("📁 Movie data file not found. Running scrapper...")
        try:
            # Run the scrapper.py script to fetch and save data
            result = subprocess.run(["python", "scrapper.py"], 
                                capture_output=True, text=True, check=True)
            print(f"🔄 Scrapper output: {result.stdout}")
            return True
        except subprocess.CalledProcessError as e:
            print(f"❌ Error running scrapper: {e}")
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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)