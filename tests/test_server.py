import redis
import pytest
import threading
from redis_clone.server import RedisServer

@pytest.fixture(scope="session", autouse=True)
def server():
    # Start server in a separate thread
    server = RedisServer(host='localhost', port=9999)
    thread = threading.Thread(target=server.start)
    thread.daemon = True
    thread.start()
    yield
    # If you have a shutdown method for your server, call it here to gracefully stop the server
    server.stop()
    thread.join()

@pytest.fixture(scope="function")
def client():
    r = redis.StrictRedis(host='localhost', port=9999, decode_responses=True)
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
