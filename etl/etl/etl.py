from clickhouse_driver import Client


def create_tables(cfg):
    client = Client(host=cfg.CLICKHOUSE_HOST, port=cfg.CLICKHOUSE_PORT)

    client.execute(
        "CREATE DATABASE IF NOT EXISTS {db} ON CLUSTER {cluster}".format(
            **{"db": cfg.CLICKHOUSE_DB, "cluster": cfg.CLICKHOUSE_CLUSTER}
        )
    )

    columns = """
        file_time DateTime,
        event_type Enum(
            'CommitCommentEvent' = 1,
            'CreateEvent' = 2,
            'DeleteEvent' = 3,
            'ForkEvent' = 4,
            'GollumEvent' = 5,
            'IssueCommentEvent' = 6,
            'IssuesEvent' = 7,
            'MemberEvent' = 8,
            'PublicEvent' = 9,
            'PullRequestEvent' = 10,
            'PullRequestReviewCommentEvent' = 11,
            'PushEvent' = 12,
            'ReleaseEvent' = 13,
            'SponsorshipEvent' = 14,
            'WatchEvent' = 15,
            'GistEvent' = 16,
            'FollowEvent' = 17,
            'DownloadEvent' = 18,
            'PullRequestReviewEvent' = 19,
            'ForkApplyEvent' = 20,
            'Event' = 21,
            'TeamAddEvent' = 22
        ),
        actor_login LowCardinality(String)
    """

    client.execute(
        """
    CREATE TABLE IF NOT EXISTS {db}.events ON CLUSTER {cluster}
    ({columns}) ENGINE = MergeTree ORDER BY file_time
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
        "DROP TABLE IF EXISTS {db}.events_queue ON CLUSTER {cluster}".format(
            **{"db": cfg.CLICKHOUSE_DB, "cluster": cfg.CLICKHOUSE_CLUSTER}
        )
    )

    client.execute(
        """
    CREATE TABLE IF NOT EXISTS {db}.events_queue ON CLUSTER {cluster}
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
    CREATE MATERIALIZED VIEW IF NOT EXISTS {db}.events_mv ON CLUSTER {cluster}
    TO {db}.events AS
    SELECT *
    FROM {db}.events_queue;
    """.format(
            **{"db": cfg.CLICKHOUSE_DB, "cluster": cfg.CLICKHOUSE_CLUSTER}
        )
    )
