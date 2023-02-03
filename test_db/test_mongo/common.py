import json
from math import sqrt
import time
from typing import Callable

USER_IDS = 'user_ids'
MOVIE_IDS = 'movie_ids'


def write_ids(user_ids: list[str], movie_ids: list[str]) -> None:
    with open('ids.json', 'w') as fp:
        json.dump({
            USER_IDS: user_ids,
            MOVIE_IDS: movie_ids,
        }, fp)


def load_ids() -> tuple[list[str], list[str]]:
    with open('ids.json', 'r') as fp:
        data = json.load(fp)
    return data[USER_IDS], data[MOVIE_IDS]


def measure_exec_time(func: Callable, **kwargs) -> float:
    start = time.time()
    func(**kwargs)
    return time.time() - start


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
