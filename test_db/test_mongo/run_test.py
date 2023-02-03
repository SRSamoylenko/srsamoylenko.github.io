import argparse
import random

from pymongo import MongoClient
from tqdm import tqdm

from common import load_ids, calculate_rmse, write_results
from test_cases import TEST_CASES, get_test


ITERATIONS = 100


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("test_name", choices=["all", *TEST_CASES.keys()])
    parser.add_argument("-i", "--iterations", type=int, default=ITERATIONS)
    parser.add_argument("-l", "--load", type=int, default=0)
    return parser


if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args()

    user_ids, movie_ids = load_ids()

    client = MongoClient()
    db = client.test

    if args.test_name == "all":
        tests_names = TEST_CASES.keys()
    else:
        tests_names = [args.test_name]

    for test_name in tests_names:
        print(f"Running test {test_name}")
        test_case = get_test(test_name)

        results = [
            test_case.measure_exec_time(
                db=db,
                user_id=random.choice(user_ids),
                movie_id=random.choice(movie_ids)
            ) for _ in tqdm(range(args.iterations))
        ]

        mean, rmse = calculate_rmse(results)
        write_results(test_name, mean, rmse, args.load)
