from clickhouse_driver import Client
import tqdm

from common import load_ids, write_results, measure_exec_time


TESTS_NUM = 1000
TEST_NAME = 'select_average_timestamps'


if __name__ == '__main__':
    client = Client(host='localhost')
    user_ids, movie_ids = load_ids()

    query = """
        SELECT movie_id, AVG(timestamp) FROM test.stats GROUP BY movie_id
    """

    results = []
    for _ in tqdm.tqdm(range(TESTS_NUM)):
        data = {}
        result = measure_exec_time(client, query, data)
        results.append(result)

    write_results(TEST_NAME, results)
