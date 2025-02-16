from flask import Flask, jsonify
from flask_sqlalchemy import SQLAlchemy
import os

app = Flask(__name__)

# Database Configuration (Render will provide DATABASE_URL)
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv("DATABASE_URL")  # Set this in Render later
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

# API Route to fetch movies
@app.route("/movies", methods=["GET"])
def get_movies():
    movies = Movie.query.all()
    return jsonify([
        {"name": m.name, "platform": m.platform, "available_on": m.available_on, "type": m.type, "imdb_rating": m.imdb_rating}
        for m in movies
    ])

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)