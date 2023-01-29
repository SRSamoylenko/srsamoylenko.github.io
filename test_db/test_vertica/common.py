import json
import time
from math import sqrt


def calculate_mean(vals: list[float]) -> float:
    return sum(vals) / len(vals)


def calculate_rmse(vals: list[float]) -> tuple[float, float]:
    mean = calculate_mean(vals)
    return mean, sqrt(sum([(val - mean) ** 2 for val in vals]) / len(vals))


def write_results(
    test_name: str,
    mean: float,
    rmse: float,
    load: int = 0,
) -> None:
    filename = test_name if not load else test_name + f"_{load}_rps"
    with open(f"results/{filename}.json", "w") as fp:
        json.dump({"mean": mean, "rmse": rmse}, fp)


def measure_exec_time(cursor, query: str, data: dict) -> float:
    start = time.time()
    cursor.execute(query, data)
    cursor.fetchall()
    return time.time() - start


def select_user_ids(cursor) -> list[str]:
    cursor.execute("""SELECT DISTINCT user_id from test.stats""")
    rows = cursor.fetchall()
    return [str(row[0]) for row in rows]


def select_movie_ids(cursor) -> list[str]:
    cursor.execute("""SELECT DISTINCT movie_id from test.stats""")
    rows = cursor.fetchall()
    return [str(row[0]) for row in rows]
