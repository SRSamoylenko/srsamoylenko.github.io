import argparse
import random

from clickhouse_driver import Client
from common import (calculate_rmse, measure_exec_time, select_movie_ids,
                    select_user_ids, write_results)
from tqdm import tqdm

ITERATIONS = 200

TESTS = {
    "select_average_timestamps": """
        SELECT movie_id, AVG(timestamp) 
        FROM test.stats 
        GROUP BY movie_id
    """,
    "select_max_timestamp_by_user_movie": """
        SELECT MAX (timestamp) 
        FROM test.stats 
        WHERE movie_id = %(movie_id)s 
        AND user_id = %(user_id)s
    """,
    "select_max_timestamps_by_user": """
        SELECT movie_id, max(timestamp) 
        FROM test.stats 
        WHERE user_id = %(user_id)s 
        GROUP BY movie_id
    """,
    "select_movies_by_user": """
        SELECT DISTINCT (movie_id) 
        FROM test.stats 
        WHERE user_id = %(user_id)s
    """,
    "select_users_by_movie": """
        SELECT DISTINCT (user_id) FROM test.stats WHERE movie_id = %(movie_id)s
    """,
}


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("test_name", choices=["all", *TESTS.keys()])
    parser.add_argument("-i", "--iterations", type=int, default=ITERATIONS)
    parser.add_argument("-l", "--loaded", action="store_true")
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    client = Client(host="localhost")

    user_ids = select_user_ids(client)
    movie_ids = select_movie_ids(client)

    if args.test_name == "all":
        tests_names = TESTS.keys()
    else:
        tests_names = [args.test_name]

    for test_name in tests_names:
        print(f"Running test {test_name}")
        query = TESTS[test_name]

        results = []
        for _ in tqdm(range(args.iterations)):
            data = {
                "user_id": random.choice(user_ids),
                "movie_id": random.choice(movie_ids),
            }
            result = measure_exec_time(client, query, data)
            results.append(result)

        mean, rmse = calculate_rmse(results)
        write_results(test_name, mean, rmse, args.loaded)
