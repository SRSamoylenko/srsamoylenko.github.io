import json
from uuid import uuid4


if __name__ == '__main__':
    user_ids = [str(uuid4()) for _ in range(1000)]
    movie_ids = [str(uuid4()) for _ in range(1000)]
    with open('ids.json', 'w') as fp:
        json.dump({'user_ids': user_ids, 'movie_ids': movie_ids}, fp)
