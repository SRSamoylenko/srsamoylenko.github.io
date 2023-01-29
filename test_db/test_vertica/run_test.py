import argparse
import random

import vertica_python
from common import (calculate_rmse, measure_exec_time, select_movie_ids,
                    select_user_ids, write_results)
from config import CONNECTION_INFO
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
        WHERE movie_id = :movie_id
        AND user_id = :user_id
    """,
    "select_max_timestamps_by_user": """
        SELECT movie_id, max(timestamp) 
        FROM test.stats 
        WHERE user_id = :user_id
        GROUP BY movie_id
    """,
    "select_movies_by_user": """
        SELECT DISTINCT (movie_id) 
        FROM test.stats 
        WHERE user_id = :user_id
    """,
    "select_users_by_movie": """
        SELECT DISTINCT (user_id) FROM test.stats WHERE movie_id = :movie_id
    """,
}


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("test_name", choices=["all", *TESTS.keys()])
    parser.add_argument("-i", "--iterations", type=int, default=ITERATIONS)
    parser.add_argument("-l", "--load", type=int, default=0)
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    if args.test_name == "all":
        tests_names = TESTS.keys()
    else:
        tests_names = [args.test_name]

    with vertica_python.connect(**CONNECTION_INFO) as connection:
        cursor = connection.cursor()

        user_ids = select_user_ids(cursor)
        movie_ids = select_movie_ids(cursor)

        for test_name in tests_names:
            print(f"Running test {test_name}")
            query = TESTS[test_name]

            results = []
            for _ in tqdm(range(args.iterations)):
                data = {
                    "user_id": random.choice(user_ids),
                    "movie_id": random.choice(movie_ids),
                }
                result = measure_exec_time(cursor, query, data)
                results.append(result)

            mean, rmse = calculate_rmse(results)
            write_results(test_name, mean, rmse, args.load)
