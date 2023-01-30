import argparse
import random
from time import sleep

import vertica_python
from common import select_movie_ids, select_user_ids
from config import CONNECTION_INFO

BATCH_SIZE = 1
DELAY = 0


QUERY = """INSERT INTO test.stats (user_id, movie_id, timestamp) VALUES (?, ?, ?)"""


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--size", type=int, default=BATCH_SIZE)
    parser.add_argument("-d", "--delay", type=float, default=DELAY)
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    with vertica_python.connect(**CONNECTION_INFO) as connection:
        cursor = connection.cursor()

        user_ids = select_user_ids(cursor)
        movie_ids = select_movie_ids(cursor)

        while True:
            values = [
                (
                    random.choice(user_ids),
                    random.choice(movie_ids),
                    random.randint(0, 100000),
                )
                for _ in range(args.size)
            ]
            cursor.executemany(QUERY, values, use_prepared_statements=True)
            sleep(args.delay)
