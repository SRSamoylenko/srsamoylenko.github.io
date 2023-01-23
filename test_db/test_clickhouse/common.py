import json
import time

from clickhouse_driver import Client


def load_ids(filename: str = 'ids.json') -> tuple[list[str], list[str]]:
    with open(filename, 'r') as fp:
        ids_values = json.load(fp)
    return ids_values.get('user_ids', {}), ids_values.get('movie_ids', {})


def write_results(test_name: str, data: list[float]) -> None:
    with open(f'{test_name}_results.json', 'w') as fp:
        json.dump(data, fp)


def measure_exec_time(client: Client, query: str, data: dict) -> float:
    start = time.time()
    client.execute(query, data)
    return time.time() - start
