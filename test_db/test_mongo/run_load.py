import random

from common import load_ids
from pymongo import MongoClient

if __name__ == "__main__":
    client = MongoClient()

    user_ids, movie_ids = load_ids()

    db = client.test
    estimations = db.estimations

    while True:
        estimations.insert_one(
            {
                "user_id": random.choice(user_ids),
                "movie_id": random.choice(movie_ids),
                "estimation": random.randint(0, 10),
            }
        )
