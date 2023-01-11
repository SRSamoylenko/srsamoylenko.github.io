#!/bin/sh
set -e

until wget -q -O /dev/null "$CLICKHOUSE_HOST:$CLICKHOUSE_PORT_HTTP/ping" &> /dev/null; do
    sleep 3
    echo 'Waiting for clickhouse to be ready...'
done

echo 'Clickhouse is up'

exec "$@"
