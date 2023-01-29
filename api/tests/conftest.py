import pytest
from src.app import app


@pytest.fixture()
def application():
    app.config["TESTING"] = True
    yield app


@pytest.fixture()
def client(application):
    return application.test_client()
