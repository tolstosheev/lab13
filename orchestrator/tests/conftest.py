import asyncio
from typing import Dict, Any

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock

from orchestrator import AgentOrchestrator


@pytest_asyncio.fixture
async def orchestrator():
    orch = AgentOrchestrator()
    orch.nc = AsyncMock()
    orch.nc.is_connected = True
    return orch


async def resolve_futures(registry: Dict[str, asyncio.Future], result: Dict[str, Any], count: int = 1):
    for _ in range(200):
        for task_id, future in list(registry.items()):
            if not future.done():
                future.set_result({
                    "transaction_id": task_id,
                    "risk_score": result.get("risk_score", 50),
                    "verdict": result.get("verdict", "MEDIUM"),
                    "reason": result.get("reason", "Test reason")
                })
        if len([f for f in registry.values() if f.done()]) >= count:
            return
        await asyncio.sleep(0.01)
