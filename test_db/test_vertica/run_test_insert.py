import argparse
import random
import time

import tqdm
import vertica_python
from common import (calculate_rmse, select_movie_ids, select_user_ids,
                    write_results)
from config import CONNECTION_INFO

BATCH_SIZE = 1000
ITERATIONS = 200


QUERY = """INSERT INTO test.stats (user_id, movie_id, timestamp) VALUES (?, ?, ?)"""


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--size", type=int, default=BATCH_SIZE)
    parser.add_argument("-i", "--iterations", type=int, default=ITERATIONS)
    parser.add_argument("-l", "--load", type=int, default=0)
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    with vertica_python.connect(**CONNECTION_INFO) as connection:
        cursor = connection.cursor()

        user_ids = select_user_ids(cursor)
        movie_ids = select_movie_ids(cursor)

        results = []
        for _ in tqdm.tqdm(range(args.iterations)):
            values = [
                (
                    random.choice(user_ids),
                    random.choice(movie_ids),
                    random.randint(0, 100000),
                )
                for _ in range(args.size)
            ]
            start = time.time()
            cursor.executemany(QUERY, values, use_prepared_statements=True)
            exec_time = time.time() - start
            results.append(exec_time)

        mean, rmse = calculate_rmse(results)
        write_results(f"insert_batch_{args.size}", mean, rmse, args.load)
