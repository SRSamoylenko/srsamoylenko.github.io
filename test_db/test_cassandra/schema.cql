CREATE KEYSPACE movies WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' };

CREATE TABLE IF NOT EXISTS movies.stats (
    id text PRIMARY KEY,
    user_id text,
    movie_id text,
    timestamp bigint
);