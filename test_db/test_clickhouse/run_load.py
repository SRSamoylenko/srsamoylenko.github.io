import argparse
import random
from time import sleep

from clickhouse_driver import Client
from common import (calculate_rmse, select_movie_ids, select_user_ids,
                    write_results)

BATCH_SIZE = 100
DELAY = 0


QUERY = """INSERT INTO test.stats (user_id, movie_id, timestamp) VALUES """


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--size", type=int, default=BATCH_SIZE)
    parser.add_argument("-d", "--delay", type=float, default=DELAY)
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    client = Client(host="localhost")

    user_ids = select_user_ids(client)
    movie_ids = select_movie_ids(client)

    client = Client(host="localhost")
    while True:
        values = [
            {
                "user_id": random.choice(user_ids),
                "movie_id": random.choice(movie_ids),
                "timestamp": random.randint(0, 100000),
            }
            for _ in range(args.size)
        ]
        client.execute(QUERY, values)
        sleep(args.delay)
