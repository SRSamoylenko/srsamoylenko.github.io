import json
import random
from uuid import uuid4

import tqdm
from cassandra.cluster import BatchStatement, Cluster, ConsistencyLevel

KEYSPACE = "movies"
BATCH_SIZE = 200
BATCH_NUMBER = 50000


def load_ids(filename: str = "ids.json") -> tuple[list[str], list[str]]:
    with open(filename, "r") as fp:
        ids_values = json.load(fp)
    return ids_values.get("user_ids", {}), ids_values.get("movie_ids", {})


if __name__ == "__main__":
    user_ids, movie_ids = load_ids()

    cluster = Cluster()
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)
    insert_statement = session.prepare(
        """
        INSERT INTO stats (id, user_id, movie_id, timestamp)
        VALUES (?, ?, ?, ?)
        """
    )

    for _ in tqdm.tqdm(range(BATCH_NUMBER)):
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for _ in range(BATCH_SIZE):
            batch.add(
                insert_statement,
                (
                    str(uuid4()),
                    random.choice(user_ids),
                    random.choice(movie_ids),
                    random.randint(0, 100000),
                ),
            )
        session.execute(batch)
