from uuid import uuid4
import random

from clickhouse_driver import Client
import tqdm

from common import load_ids

KEYSPACE = 'movies'
BATCH_SIZE = 1000
BATCH_NUMBER = 10000


if __name__ == '__main__':
    user_ids, movie_ids = load_ids()

    client = Client(host='localhost')
    for _ in tqdm.tqdm(range(BATCH_NUMBER)):
        values = [
            {
                'id': str(uuid4()),
                'user_id': random.choice(user_ids),
                'movie_id': random.choice(movie_ids),
                'timestamp': random.randint(0, 100000)
            }
        ]
        client.execute(
            """
            INSERT INTO test.stats (id, user_id, movie_id, timestamp) VALUES
            """,
            values
        )
