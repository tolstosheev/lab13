from contextlib import asynccontextmanager
from typing import AsyncGenerator
from unittest.mock import AsyncMock

import pytest
from fastapi.testclient import TestClient

import api.main


@asynccontextmanager
async def noop_lifespan(app) -> AsyncGenerator[None, None]:
    yield


@pytest.fixture
def mock_orch() -> AsyncMock:
    mock = AsyncMock()
    mock.send_task.return_value = {
        "risk_score": 50,
        "verdict": "MEDIUM",
        "reason": "Score 50 based on: [TEST(50.00)]",
    }
    return mock


@pytest.fixture
def client(mock_orch) -> TestClient:
    old_lifespan = api.main.app.router.lifespan_context
    api.main.app.router.lifespan_context = noop_lifespan
    old_orch = api.main.orchestrator
    api.main.orchestrator = mock_orch

    with TestClient(api.main.app) as c:
        yield c

    api.main.app.router.lifespan_context = old_lifespan
    api.main.orchestrator = old_orch


@pytest.fixture(autouse=True)
def reset_state() -> None:
    api.main.background_tasks.clear()
    api.main.rate_history.clear()
