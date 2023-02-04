import argparse
import random
from uuid import uuid4

import tqdm
from common import write_ids
from pymongo import MongoClient

BATCH_SIZE = 10000
USERS_NUMBER = 1000
MOVIES_NUMBER = 1000
ESTIMATIONS_BATCH_NUMBER = 1000
POSTPONED_BATCH_NUMBER = 1000


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--size", type=int, default=BATCH_SIZE)
    parser.add_argument("-u", "--users", type=int, default=USERS_NUMBER)
    parser.add_argument("-m", "--movies", type=int, default=MOVIES_NUMBER)
    parser.add_argument(
        "-e",
        "--estimations",
        type=int,
        default=ESTIMATIONS_BATCH_NUMBER,
    )
    parser.add_argument(
        "-p",
        "--postponed",
        type=int,
        default=POSTPONED_BATCH_NUMBER,
    )
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    user_ids = [str(uuid4()) for _ in range(args.users)]
    movie_ids = [str(uuid4()) for _ in range(args.movies)]
    write_ids(user_ids, movie_ids)

    client = MongoClient()
    db = client.test

    estimations = db.estimations
    for _ in tqdm.tqdm(range(args.estimations)):
        new_estimations = [
            {
                "user_id": random.choice(user_ids),
                "movie_id": random.choice(movie_ids),
                "estimation": random.randint(0, 10),
            }
            for _ in range(args.size)
        ]
        estimations.insert_many(new_estimations)

    postponed = db.postponed
    for _ in tqdm.tqdm(range(args.postponed)):
        new_postponed = [
            {
                "user_id": random.choice(user_ids),
                "movie_id": random.choice(movie_ids),
            }
            for _ in range(args.size)
        ]
        postponed.insert_many(new_postponed)
