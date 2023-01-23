import random

from clickhouse_driver import Client
import tqdm

from common import load_ids, write_results, measure_exec_time


TESTS_NUM = 1000
TEST_NAME = 'select_movies_by_user'


if __name__ == '__main__':
    client = Client(host='localhost')
    user_ids, movie_ids = load_ids()

    query = """
        SELECT DISTINCT (movie_id) FROM test.stats WHERE user_id = %(user_id)s
    """

    results = []
    for _ in tqdm.tqdm(range(TESTS_NUM)):
        data = {'user_id': random.choice(user_ids)}
        result = measure_exec_time(client, query, data)
        results.append(result)

    write_results(TEST_NAME, results)
