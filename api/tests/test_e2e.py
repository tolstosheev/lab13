import json
import os
import shutil
import subprocess
import time

import httpx
import pytest

docker_available = shutil.which("docker") is not None
inside_docker = os.path.exists("/.dockerenv")

API_URL = "http://api:8000" if inside_docker else "http://localhost:8000"


def run_docker_compose(args) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose"] + args,
        capture_output=True, text=True, timeout=60,
    )


@pytest.fixture(scope="module")
def docker_stack() -> None:
    if docker_available:
        run_docker_compose(["up", "-d", "nats", "agent-1", "agent-2", "agent-3", "api"])
    for _ in range(30):
        try:
            r = httpx.get(f"{API_URL}/health", timeout=5)
            if r.status_code == 200:
                break
        except (httpx.ConnectError, httpx.TimeoutException, httpx.RemoteProtocolError):
            time.sleep(2)
    else:
        if docker_available:
            run_docker_compose(["down"])
        pytest.fail("API did not become healthy")
    yield
    if docker_available:
        run_docker_compose(["down"])


VALID_PAYLOAD = {"markers": [{"id": "BLACKLIST_HIT", "confidence": 1.0}]}


class TestE2E:
    def test_health(self, docker_stack) -> None:
        resp = httpx.get(f"{API_URL}/health", timeout=10)
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

    def test_assess_sync(self, docker_stack) -> None:
        resp = httpx.post(
            f"{API_URL}/assess",
            json=VALID_PAYLOAD,
            timeout=30,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["risk_score"] == 80
        assert data["verdict"] == "MEDIUM"
        assert "BLACKLIST_HIT" in data["reason"]

    def test_assess_batch(self, docker_stack) -> None:
        resp = httpx.post(
            f"{API_URL}/assess/batch",
            json={"tasks": [VALID_PAYLOAD, VALID_PAYLOAD]},
            timeout=60,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["results"]) == 2
        for r in data["results"]:
            assert r["risk_score"] == 80

    def test_assess_async_flow(self, docker_stack) -> None:
        resp = httpx.post(
            f"{API_URL}/assess/async",
            json=VALID_PAYLOAD,
            timeout=30,
        )
        assert resp.status_code == 200
        task_id = resp.json()["task_id"]
        assert resp.json()["status"] == "pending"

        for _ in range(10):
            resp = httpx.get(f"{API_URL}/status/{task_id}", timeout=10)
            if resp.json()["status"] == "completed":
                break
            time.sleep(2)
        else:
            pytest.fail(f"Task {task_id} did not complete")

        data = resp.json()
        assert data["status"] == "completed"
        assert data["result"]["risk_score"] == 80

    def test_assess_validation_error(self, docker_stack) -> None:
        resp = httpx.post(
            f"{API_URL}/assess",
            json={},
            timeout=10,
        )
        assert resp.status_code == 422

    def test_status_not_found(self, docker_stack) -> None:
        resp = httpx.get(f"{API_URL}/status/non-existent", timeout=10)
        assert resp.status_code == 404
