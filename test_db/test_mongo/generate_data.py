import argparse
import random
from uuid import uuid4

import pymongo
import tqdm
from common import write_ids
from settings import settings

BATCH_SIZE = 10000
USERS_NUMBER = 1000
MOVIES_NUMBER = 1000
ESTIMATIONS_NUMBER = 10000000
POSTPONED_NUMBER = 10000000


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("-b", "--batch-size", type=int, default=BATCH_SIZE)
    parser.add_argument("-u", "--users", type=int, default=USERS_NUMBER)
    parser.add_argument("-m", "--movies", type=int, default=MOVIES_NUMBER)
    parser.add_argument(
        "-e",
        "--estimations",
        type=int,
        default=ESTIMATIONS_NUMBER,
    )
    parser.add_argument(
        "-p",
        "--postponed",
        type=int,
        default=POSTPONED_NUMBER,
    )
    return parser.parse_args()


def create_ids(user_count, movie_count):
    user_ids = [str(uuid4()) for _ in range(user_count)]
    movie_ids = [str(uuid4()) for _ in range(movie_count)]
    write_ids(user_ids, movie_ids)
    return user_ids, movie_ids


def get_db():
    client = pymongo.MongoClient(
        username=settings.mongo_username,
        password=settings.mongo_password,
    )
    client.drop_database("test")
    return client.test


def generate_estimations(db, items_count, batch_size, user_ids, movie_ids):
    estimations = db.estimations
    estimations.create_index(
        [("user_id", pymongo.DESCENDING), ("estimation", pymongo.DESCENDING)]
    )
    estimations.create_index(
        [("movie_id", pymongo.DESCENDING), ("estimation", pymongo.DESCENDING)]
    )

    def gen_item():
        return {
            "user_id": random.choice(user_ids),
            "movie_id": random.choice(movie_ids),
            "estimation": random.randint(0, 10),
        }

    insert_batches(estimations, gen_item, items_count, batch_size)


def generate_postponed(db, items_count, batch_size, user_ids, movie_ids):
    postponed = db.postponed
    postponed.create_index("user_id")

    def gen_item():
        return {
            "user_id": random.choice(user_ids),
            "movie_id": random.choice(movie_ids),
        }

    insert_batches(postponed, gen_item, items_count, batch_size)


def insert_batches(collection, item_generator, items_count, batch_size):

    batches = items_count // batch_size * [batch_size]  # get full batches
    if items_count % batch_size:
        batches.append(items_count % batch_size)  # add reminder items

    for size in tqdm.tqdm(batches):
        collection.insert_many([item_generator() for _ in range(size)])


if __name__ == "__main__":
    args = get_args()
    user_ids, movie_ids = create_ids(args.users, args.movies)
    db = get_db()
    generate_estimations(db, args.estimations, args.batch_size, user_ids, movie_ids)
    generate_postponed(db, args.postponed, args.batch_size, user_ids, movie_ids)
