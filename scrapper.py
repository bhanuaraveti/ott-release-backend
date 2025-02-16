import requests
import time
from bs4 import BeautifulSoup
from app import db, Movie, app

# Target URL
URL = "https://trendraja.in/telugu-movie-ott-release-dates-2021/"

# Headers to mimic a browser request (to avoid getting blocked)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def scrape_movies():
    """Scrapes Telugu movies from Trendraja and inserts into the database."""
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

    # Insert into the database
    insert_into_db(movies_list)

def insert_into_db(movies):
    """Inserts scraped movies into the PostgreSQL database."""
    with app.app_context():
        for movie in movies:
            existing_movie = Movie.query.filter_by(name=movie["name"], platform=movie["platform"]).first()
            if not existing_movie:
                new_movie = Movie(
                    name=movie["name"],
                    platform=movie["platform"],
                    available_on=movie["available_on"],
                    type=movie["type"],
                    imdb_rating=movie["imdb_rating"]
                )
                print(f"📝 Adding {new_movie.name} to the database...")
                db.session.add(new_movie)

        db.session.commit()
        print("🎉 Movies inserted into the database!")

if __name__ == "__main__":
    scrape_movies()
    time.sleep(20)  # Respect the crawl delay to avoid getting blocked