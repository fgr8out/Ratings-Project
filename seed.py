"""Utility file to seed ratings database from MovieLens data in seed_data/"""


from model import User
from model import Rating
from model import Movie

from model import connect_to_db, db
from server import app
from datetime import datetime


def load_users():
    """Load users from u.user into database."""

    print "Users"

    # Delete all rows in table, so if we need to run this a second time,
    # we won't be trying to add duplicate users
    User.query.delete()

    # Read u.user file and insert data
    for row in open("seed_data/u.user"):
        row = row.rstrip()
        user_id, age, gender, occupation, zipcode = row.split("|")

        user = User(user_id=user_id,
                    age=age,
                    zipcode=zipcode)

        # We need to add to the session or it won't ever be stored
        db.session.add(user)

    # Once we're done, we should commit our work
    db.session.commit()


def load_movies():
    """Load movies from u.item into database."""


    print "movies"

    Movie.query.delete()


    for row in open("seed_data/u.item"):
        row = row.strip()
        items = row.split("|")

        # We first check if released date is empty before we go ahead
        # and create a Movie object. Conditional "if items[2]:" returns 
        # False if it is an empty string.

        if items[2]:
            movie_id = items[0]
            title = items[1]
            released_at = items[2]
            imdb_url = items[4]

            released_at = datetime.strptime(released_at, "%d-%b-%Y")
            title = title[:-7] #:" (YEAR) " == 7
            movie = Movie(movie_id=movie_id,
                            title=title,
                            released_at=released_at,
                            imdb_url=imdb_url)

            db.session.add(movie)

    db.session.commit()




def load_ratings():
    """Load ratings from u.data into database."""

    print "ratings"
    
    Rating.query.delete()

    for row in open("seed_data/u.data"):
        row = row.strip()
        items = row.split("\t")

        rating = Rating(user_id=items[0],
                        movie_id=items[1],
                        score=items[2])

        db.session.add(rating)

    db.session.commit()
    

if __name__ == "__main__":
    connect_to_db(app)

    # In case tables haven't been created, create them
    db.create_all()

    # Import different types of data
    load_users()
    load_movies()
    load_ratings()
