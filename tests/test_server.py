import redis
import pytest
import os

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", 9999)


# Server should be running before running the tests
@pytest.fixture(scope="function")
def client():
    r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    yield r
    r.close()


def test_ping(client):
    response = client.ping()
    print(response)
    assert response == True


def test_echo(client):
    response = client.echo("Hello World")
    assert response == "Hello World"


def test_set_get(client):
    response = client.set("test_key", "test_value")
    assert response == True

    value = client.get("test_key")
    assert value == "test_value"


def test_nonexistent_get(client):
    value = client.get("random")
    assert value is None
