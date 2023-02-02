import json

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
