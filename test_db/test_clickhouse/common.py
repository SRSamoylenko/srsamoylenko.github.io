import json
import time
from math import sqrt

from clickhouse_driver import Client


def calculate_mean(vals: list[float]) -> float:
    return sum(vals) / len(vals)


def calculate_rmse(vals: list[float]) -> tuple[float, float]:
    mean = calculate_mean(vals)
    return mean, sqrt(sum([(val - mean) ** 2 for val in vals]) / len(vals))


def write_results(
    test_name: str,
    mean: float,
    rmse: float,
    loaded: bool = False,
) -> None:
    filename = test_name if not loaded else test_name + "_loaded"
    with open(f"results/{filename}.json", "w") as fp:
        json.dump({"mean": mean, "rmse": rmse}, fp)


def measure_exec_time(client: Client, query: str, data: dict) -> float:
    start = time.time()
    client.execute(query, data)
    return time.time() - start


def select_user_ids(client) -> list[str]:
    rows = client.execute("""SELECT DISTINCT user_id from test.stats""")
    return [str(row[0]) for row in rows]


def select_movie_ids(client) -> list[str]:
    rows = client.execute("""SELECT DISTINCT movie_id from test.stats""")
    return [str(row[0]) for row in rows]
