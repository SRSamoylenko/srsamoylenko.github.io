import random

from clickhouse_driver import Client
import tqdm

from common import load_ids, write_results, measure_exec_time


TESTS_NUM = 1000
TEST_NAME = 'max_timestamp_by_user_movie'


if __name__ == '__main__':
    client = Client(host='localhost')
    user_ids, movie_ids = load_ids()

    query = """
        SELECT MAX (timestamp) FROM test.stats WHERE movie_id = %(movie_id)s AND user_id = %(user_id)s
    """

    results = []
    for _ in tqdm.tqdm(range(TESTS_NUM)):
        data = {
            'movie_id': random.choice(movie_ids),
            'user_id': random.choice(user_ids),
        }
        result = measure_exec_time(client, query, data)
        results.append(result)

    write_results(TEST_NAME, results)
