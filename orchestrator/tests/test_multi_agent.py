import asyncio
import logging
import os
import shutil
import subprocess

import pytest

docker_available = shutil.which("docker") is not None


def run_docker_compose(args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose"] + args,
        capture_output=True, text=True, timeout=60,
        cwd=os.path.join(os.path.dirname(__file__), "..", "..")
    )


@pytest.mark.skipif(not docker_available, reason="Docker not available")
@pytest.mark.asyncio
async def test_multi_agent_load_balancing():
    run_docker_compose(["up", "-d", "nats", "agent-1", "agent-2", "agent-3"])
    await asyncio.sleep(5)

    try:
        from orchestrator import AgentOrchestrator
        orch = AgentOrchestrator()
        await orch.connect("nats://localhost:4222")
        await orch.start_listener()

        payload = {"markers": [{"id": "TEST", "confidence": 0.5}]}

        async def resolve_all():
            for _ in range(200):
                for task_id, future in list(orch.results.items()):
                    if not future.done():
                        return
                await asyncio.sleep(0.05)

        asyncio.create_task(resolve_all())
        tasks = [orch.send_task(payload, timeout=10) for _ in range(9)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await orch.disconnect()

        success = [r for r in results if not isinstance(r, Exception)]
        assert len(success) == 9, f"Expected 9 successes, got {len(success)}"

        logs = run_docker_compose(["logs", "agent-1", "agent-2", "agent-3"])
        agents_seen = set()
        for line in logs.stdout.splitlines():
            for aid in ("agent-1", "agent-2", "agent-3"):
                if f"[{aid}]" in line and "processing risk assessment" in line:
                    agents_seen.add(aid)

        assert agents_seen == {"agent-1", "agent-2", "agent-3"}, (
            f"Not all agents received tasks: {agents_seen}"
        )

    finally:
        run_docker_compose(["down", "--remove-orphans"])
