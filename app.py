from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_cors import CORS  # Import CORS
from dotenv import load_dotenv
import os

# Load environment variables only in local development
if os.getenv("RENDER") is None:  # Render sets "RENDER" variable automatically
    from dotenv import load_dotenv
    load_dotenv()

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Database Configuration
print(os.getenv("DATABASE_URL"))
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL")  # Use Render's PostgreSQL URL
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

# Movie Model
class Movie(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(200), nullable=False)
    platform = db.Column(db.String(100), nullable=False)
    available_on = db.Column(db.String(50), nullable=False)
    type = db.Column(db.String(50), nullable=False)
    imdb_rating = db.Column(db.Float, nullable=True)

# API Route to Fetch Movies
@app.route("/movies", methods=["GET"])
def get_movies():
    movies = Movie.query.all()
    return jsonify([
        {"name": m.name, "platform": m.platform, "available_on": m.available_on, "type": m.type, "imdb_rating": m.imdb_rating}
        for m in movies
    ])

# Run database migration when the app starts
with app.app_context():
    db.create_all()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)