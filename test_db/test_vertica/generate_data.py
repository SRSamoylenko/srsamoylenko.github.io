import argparse
import random
from uuid import uuid4

import tqdm
import vertica_python
from config import CONNECTION_INFO

BATCH_SIZE = 5000
BATCH_NUMBER = 2000
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

    with vertica_python.connect(**CONNECTION_INFO) as connection:
        cursor = connection.cursor()
        for _ in tqdm.tqdm(range(args.number)):
            values = [
                (
                    random.choice(user_ids),
                    random.choice(movie_ids),
                    random.randint(0, 100000),
                )
                for _ in range(args.size)
            ]
            cursor.executemany(
                (
                    "INSERT INTO test.stats (user_id, movie_id, timestamp)"
                    "VALUES (?, ?, ?)"
                ),
                values,
                use_prepared_statements=True,
            )
