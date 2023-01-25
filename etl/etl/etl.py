from clickhouse_driver import Client


def create_tables(cfg):
    client = Client(host=cfg.CLICKHOUSE_HOST, port=cfg.CLICKHOUSE_PORT)

    client.execute(
        "CREATE DATABASE IF NOT EXISTS {db} ON CLUSTER {cluster}".format(
            **{"db": cfg.CLICKHOUSE_DB, "cluster": cfg.CLICKHOUSE_CLUSTER}
        )
    )

    columns = """
        user_id UUID,
        movie_id UUID,
        timestamp DateTime
    """

    client.execute(
        """
    CREATE TABLE IF NOT EXISTS {db}.views ON CLUSTER {cluster}
    ({columns}) ENGINE = MergeTree() ORDER BY (user_id, movie_id)
    """.format(
            **{
                "db": cfg.CLICKHOUSE_DB,
                "cluster": cfg.CLICKHOUSE_CLUSTER,
                "columns": columns,
            }
        )
    )

    # kafka parameters may be changed so recreate connection table
    client.execute(
        "DROP TABLE IF EXISTS {db}.views_queue ON CLUSTER {cluster}".format(
            **{"db": cfg.CLICKHOUSE_DB, "cluster": cfg.CLICKHOUSE_CLUSTER}
        )
    )

    client.execute(
        """
    CREATE TABLE IF NOT EXISTS {db}.views_queue ON CLUSTER {cluster}
    ({columns})
    ENGINE = Kafka(
        '{kf_host}:{kf_port}',
        '{kf_topic}',
        '{kf_consumer_group}',
        'JSONEachRow'
    );
    """.format(
            **{
                "columns": columns,
                "db": cfg.CLICKHOUSE_DB,
                "cluster": cfg.CLICKHOUSE_CLUSTER,
                "kf_host": cfg.KAFKA_HOST,
                "kf_port": cfg.KAFKA_PORT,
                "kf_topic": cfg.KAFKA_TOPIC,
                "kf_consumer_group": cfg.KAFKA_CONSUMER_GROUP,
            }
        )
    )

    client.execute(
        """
    CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.views_mv ON CLUSTER {cluster}
    TO {db}.views AS
    SELECT *
    FROM {db}.views_queue;
    """.format(
            **{"db": cfg.CLICKHOUSE_DB, "cluster": cfg.CLICKHOUSE_CLUSTER}
        )
    )
