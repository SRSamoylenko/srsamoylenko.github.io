import argparse
import random
from uuid import uuid4

import tqdm
from clickhouse_driver import Client

BATCH_SIZE = 1000
BATCH_NUMBER = 10000
USERS_NUMBER = 1000
MOVIES_NUMBER = 1000


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--size", type=int, default=BATCH_SIZE)
    parser.add_argument("-n", "--number", type=int, default=BATCH_NUMBER)
    parser.add_argument("-u", "--users", type=int, default=USERS_NUMBER)
    parser.add_argument("-m", "--movies", type=int, default=MOVIES_NUMBER)
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    user_ids = [str(uuid4()) for _ in range(args.users)]
    movie_ids = [str(uuid4()) for _ in range(args.movies)]

    client = Client(host="localhost")
    for _ in tqdm.tqdm(range(args.number)):
        values = [
            {
                "user_id": random.choice(user_ids),
                "movie_id": random.choice(movie_ids),
                "timestamp": random.randint(0, 100000),
            }
            for _ in range(args.size)
        ]
        client.execute(
            """
            INSERT INTO test.stats (user_id, movie_id, timestamp) VALUES
            """,
            values,
        )
