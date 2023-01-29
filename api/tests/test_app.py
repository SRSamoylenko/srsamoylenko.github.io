from unittest.mock import AsyncMock


def test_write_timestamp(client, mocker):
    """Dummy test to use in github actions test matrix."""
    mocker.patch("src.app.AIOKafkaProducer", return_value=AsyncMock())
    res = client.post(
        "/write-timestamp",
        json={
            "user_id": "00000000-0000-0000-0000-000000000000",
            "movie_id": "00000000-0000-0000-0000-000000000000",
            "ts": 0,
        },
    )
    assert res.status_code == 201
    assert res.text.strip() == '{"status":"success"}'
