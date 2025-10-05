import requests
import time
import json
import os
from datetime import datetime
from bs4 import BeautifulSoup

# Target URL
URL = "https://trendraja.in/telugu-movie-ott-release-dates-2021/"

# Headers to mimic a browser request (to avoid getting blocked)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

# File path for storing the scraped data
DATA_DIR = "data"
DATA_FILE = os.path.join(DATA_DIR, "movies.json")

def scrape_movies():
    """Scrapes Telugu movies from Trendraja and saves to a JSON file."""
    print("🔍 Fetching movie data...")
    response = requests.get(URL, headers=HEADERS)
    
    if response.status_code != 200:
        print(f"❌ Failed to fetch data. Status Code: {response.status_code}")
        return

    soup = BeautifulSoup(response.text, "html.parser")

    movies_list = []

    # Select the table containing movie data
    table = soup.select_one("table#tablepress-116")

    if not table:
        print("❌ Could not find the movie table. Check the selector!")
        return

    rows = table.select("tbody tr")  # Select all rows in the table

    for row in rows:
        columns = row.find_all("td")
        if len(columns) >= 4:  # Ensure there are enough columns
            name = columns[0].text.strip()
            available_on = columns[1].text.strip()  # Digital Release Date
            platform = columns[2].text.strip()  # Streaming Platform
            type_ = columns[3].text.strip()  # Category

            imdb_rating = None  # IMDb rating is not available on this site

            movies_list.append({
                "name": name,
                "platform": platform,
                "available_on": available_on,
                "type": type_,
                "imdb_rating": imdb_rating
            })

    print(f"✅ Scraped {len(movies_list)} movies successfully!")

    # Save to file
    save_to_file(movies_list)

def parse_date(date_str):
    """Parse date string to a datetime object for sorting."""
    if not date_str:
        return datetime.max  # Put empty dates at the end

    date_str = date_str.strip().lower()

    # Handle "Soon" or "Coming Soon" - put these at the very top
    if 'soon' in date_str:
        return datetime.min

    try:
        # Try parsing various date formats
        # Format: "15 May 2025" or "15 May 2024"
        return datetime.strptime(date_str, "%d %b %Y")
    except ValueError:
        try:
            # Format: "May 2025" (day not specified)
            return datetime.strptime(date_str, "%b %Y")
        except ValueError:
            try:
                # Format: "15 May" (year not specified, assume current year)
                current_year = datetime.now().year
                return datetime.strptime(f"{date_str} {current_year}", "%d %b %Y")
            except ValueError:
                # If all parsing fails, put at the end
                return datetime.max

def save_to_file(movies):
    """Saves scraped movies to a JSON file."""
    # Create data directory if it doesn't exist
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"📁 Created directory: {DATA_DIR}")

    # Load existing data if file exists
    existing_movies = []
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'r', encoding='utf-8') as file:
            try:
                existing_movies = json.load(file)
                print(f"📚 Loaded {len(existing_movies)} existing movies from file")
            except json.JSONDecodeError:
                print("⚠️ Error reading existing file, creating new one")

    # Update with new movies (avoiding duplicates)
    updated_movies = existing_movies.copy()
    new_count = 0

    for movie in movies:
        # Check if movie already exists
        if not any(existing['name'] == movie['name'] and existing['platform'] == movie['platform']
                  for existing in existing_movies):
            updated_movies.append(movie)
            new_count += 1
            print(f"📝 Adding {movie['name']} to the file...")

    # Sort movies: "Soon" first, then by date (newest first), then by name
    print("🔄 Sorting movies by release date...")
    def sort_key(movie):
        parsed_date = parse_date(movie.get('available_on', ''))
        # "Soon" movies: smallest value to appear first
        if parsed_date == datetime.min:
            return (0, datetime.min, movie.get('name', ''))
        # Invalid/empty dates: largest value to appear last
        elif parsed_date == datetime.max:
            return (2, datetime.max, movie.get('name', ''))
        # Normal dates: reverse timestamp for newest first
        else:
            return (1, -parsed_date.timestamp(), movie.get('name', ''))

    updated_movies.sort(key=sort_key)

    # Save the updated list
    with open(DATA_FILE, 'w', encoding='utf-8') as file:
        json.dump(updated_movies, file, indent=2, ensure_ascii=False)

    print(f"🎉 Movie data saved! Added {new_count} new movies to the file.")

    # Create a timestamped backup file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_file = os.path.join(DATA_DIR, f"movies_backup_{timestamp}.json")
    with open(backup_file, 'w', encoding='utf-8') as file:
        json.dump(updated_movies, file, indent=2, ensure_ascii=False)
    print(f"💾 Created backup: {backup_file}")

if __name__ == "__main__":
    scrape_movies()
    time.sleep(20)  # Respect the crawl delay to avoid getting blocked