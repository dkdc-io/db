"""Shared fixtures for Python integration tests."""

import os
import socket
import subprocess
import tempfile
import time

import pytest


def _free_port() -> int:
    """Find a free TCP port."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def server_port():
    """Start a db server on a random port for the test session."""
    port = _free_port()
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    server_bin = os.path.join(repo_root, "target", "debug", "db")

    # Use a temp directory for database storage so tests don't pollute ~/.dkdc
    data_dir = tempfile.mkdtemp(prefix="dkdc-db-test-")
    env = {**os.environ, "DKDC_HOME": data_dir}

    proc = subprocess.Popen(
        [server_bin, "serve", "--foreground", "--port", str(port)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for server to be ready
    from dkdc_db import Db

    client = Db(f"http://127.0.0.1:{port}")
    for _ in range(100):
        try:
            if client.health():
                break
        except Exception:
            pass
        time.sleep(0.05)
    else:
        proc.kill()
        raise RuntimeError(f"Server failed to start on port {port}")

    yield port

    proc.terminate()
    proc.wait(timeout=5)


@pytest.fixture()
def db(server_port):
    """Return a fresh Db client. Creates a unique database per test and cleans up after."""
    from dkdc_db import Db

    return Db(f"http://127.0.0.1:{server_port}")


@pytest.fixture()
def test_db(db):
    """Create a uniquely-named database for the test and drop it after."""
    import uuid

    name = f"test_{uuid.uuid4().hex[:8]}"
    db.create_db(name)
    yield db, name
    try:
        db.drop_db(name)
    except Exception:
        pass
