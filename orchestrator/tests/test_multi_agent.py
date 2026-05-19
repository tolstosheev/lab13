import asyncio
import os
import shutil
import subprocess

import pytest

docker_available = shutil.which("docker") is not None
inside_docker = os.path.exists("/.dockerenv")
PROJECT_DIR = os.path.join(os.path.dirname(__file__), "..", "..")
NATS_URL = os.environ.get("NATS_URL", "nats://nats:4222" if inside_docker else "nats://localhost:4222")


def run_docker_compose(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose"] + args,
        capture_output=True, text=True, timeout=120,
        cwd=PROJECT_DIR,
    )


@pytest.fixture(scope="module")
def docker_stack():
    if docker_available:
        run_docker_compose(["up", "-d", "nats", "agent-1", "agent-2", "agent-3"])
        import time
        time.sleep(5)
    yield
    if docker_available:
        run_docker_compose(["down", "--remove-orphans"])


@pytest.fixture(scope="module")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.mark.usefixtures("docker_stack")
@pytest.mark.asyncio
class TestMultiAgent:

    @pytest.mark.parametrize("task_count", [3, 6])
    @pytest.mark.asyncio
    async def test_load_balancing(self, task_count):
        from orchestrator import AgentOrchestrator
        orch = AgentOrchestrator()
        await orch.connect(NATS_URL)
        await orch.start_listener()

        payload = {"markers": [{"id": "TEST", "confidence": 0.5}]}
        tasks = [orch.send_task(payload, timeout=10) for _ in range(task_count)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await orch.disconnect()

        success = [r for r in results if not isinstance(r, Exception)]
        assert len(success) == task_count

        agents_seen = set()
        if docker_available:
            logs = run_docker_compose(["logs", "agent-1", "agent-2", "agent-3"])
            for line in logs.stdout.splitlines():
                for aid in ("agent-1", "agent-2", "agent-3"):
                    if f"[{aid}]" in line and "processing risk assessment" in line:
                        agents_seen.add(aid)

        if docker_available:
            if task_count >= 6:
                assert agents_seen == {"agent-1", "agent-2", "agent-3"}
            else:
                assert len(agents_seen) > 0

    @pytest.mark.asyncio
    async def test_no_messages_lost(self):
        from orchestrator import AgentOrchestrator
        orch = AgentOrchestrator()
        await orch.connect(NATS_URL)
        await orch.start_listener()

        payload = {"markers": [{"id": "TEST", "confidence": 0.5}]}
        tasks = [orch.send_task(payload, timeout=10) for _ in range(5)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await orch.disconnect()

        success = [r for r in results if not isinstance(r, Exception)]
        assert len(success) == 5
        assert all(r["risk_score"] == 0 for r in success)
