from clickhouse_driver import Client


if __name__ == '__main__':
    client = Client(host='localhost')
    client.execute('CREATE DATABASE IF NOT EXISTS test')
    client.execute(
        """
        CREATE TABLE test.stats (
            id UUID, 
            user_id UUID, 
            movie_id UUID, 
            timestamp UInt32
        ) Engine=MergeTree() ORDER BY id
        """
    )
