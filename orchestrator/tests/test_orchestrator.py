import asyncio
import logging

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
import pytest_asyncio
from orchestrator import AgentOrchestrator, SUBJECT_RISK_ASSESSMENT, SUBJECT_COMPLETED

@pytest_asyncio.fixture
async def orchestrator():
    orch = AgentOrchestrator()
    orch.nc = AsyncMock()
    orch.nc.is_connected = True
    return orch

@pytest.mark.asyncio
async def test_connect():
    with patch("nats.connect", new_callable=AsyncMock) as mock_connect:
        orch = AgentOrchestrator()
        url = "nats://test:4222"
        await orch.connect(url)
        mock_connect.assert_called_once_with(url)

@pytest.mark.asyncio
async def test_send_task_success(orchestrator):
    payload = {"markers": [{"id": "TEST", "confidence": 1.0}]}

    async def simulate_response():
        for _ in range(100):
            for task_id, future in list(orchestrator.results.items()):
                if not future.done():
                    future.set_result({
                        "transaction_id": task_id,
                        "risk_score": 50,
                        "verdict": "MEDIUM",
                        "reason": "Test reason"
                    })
                    return
            await asyncio.sleep(0.01)

    asyncio.create_task(simulate_response())

    result = await orchestrator.send_task(payload)
    
    assert result["risk_score"] == 50
    assert result["verdict"] == "MEDIUM"
    assert len(orchestrator.results) == 0
    orchestrator.nc.publish.assert_called_once()
    args, _ = orchestrator.nc.publish.call_args
    assert args[0] == SUBJECT_RISK_ASSESSMENT

@pytest.mark.asyncio
async def test_send_task_timeout(orchestrator):
    payload = {"markers": []}
    with pytest.raises(TimeoutError):
        await orchestrator.send_task(payload, timeout=0.01)
    assert len(orchestrator.results) == 0

@pytest.mark.asyncio
async def test_send_task_not_connected(orchestrator):
    orchestrator.nc.is_connected = False
    with pytest.raises(ConnectionError):
        await orchestrator.send_task({"markers": []})

@pytest.mark.parametrize("payload, expected_error", [
    ({"markers": "not a list"}, ValueError),
    ({"wrong_key": []}, ValueError),
    ({}, ValueError),
])
@pytest.mark.asyncio
async def test_send_task_invalid_payloads(orchestrator, payload, expected_error):
    with pytest.raises(expected_error):
        await orchestrator.send_task(payload)
    assert len(orchestrator.results) == 0

@pytest.mark.parametrize("payload_bytes, expected_score", [
    (b'{"transaction_id": "t1", "risk_score": 10, "verdict": "LOW", "reason": "OK"}', 10),
    (b'{"transaction_id": "t1", "risk_score": 90, "verdict": "HIGH", "reason": "Bad"}', 90),
])
@pytest.mark.asyncio
async def test_on_result_valid(orchestrator, payload_bytes, expected_score):
    task_id = "t1"
    future = asyncio.Future()
    orchestrator.results[task_id] = future
    
    msg = MagicMock()
    msg.data = payload_bytes
    
    await orchestrator.on_result(msg)
    
    assert future.done()
    assert future.result()["risk_score"] == expected_score
    assert task_id not in orchestrator.results

@pytest.mark.parametrize("malformed_payload", [
    b'invalid json',
    b'{"wrong_id": "t1"}',
    b'[]',
    b'',
])
@pytest.mark.asyncio
async def test_on_result_malformed(orchestrator, malformed_payload):
    task_id = "t1"
    future = asyncio.Future()
    orchestrator.results[task_id] = future
    
    msg = MagicMock()
    msg.data = malformed_payload
    
    await orchestrator.on_result(msg)
    
    assert not future.done()
    assert task_id in orchestrator.results

@pytest.mark.asyncio
async def test_on_result_unknown_task(orchestrator):
    msg = MagicMock()
    msg.data = b'{"transaction_id": "unknown", "risk_score": 10}'
    
    await orchestrator.on_result(msg)
    assert len(orchestrator.results) == 0

@pytest.mark.asyncio
async def test_disconnect(orchestrator):
    await orchestrator.disconnect()
    orchestrator.nc.close.assert_called_once()

@pytest.mark.asyncio
async def test_processed_counter_increments(orchestrator):
    assert orchestrator.processed == 0
    payload = {"markers": [{"id": "TEST", "confidence": 1.0}]}

    async def simulate_response():
        for _ in range(100):
            for task_id, future in list(orchestrator.results.items()):
                if not future.done():
                    future.set_result({
                        "transaction_id": task_id,
                        "risk_score": 30,
                        "verdict": "LOW",
                        "reason": "Counter test"
                    })
                    return
            await asyncio.sleep(0.01)

    asyncio.create_task(simulate_response())
    await orchestrator.send_task(payload)
    assert orchestrator.processed == 1

@pytest.mark.asyncio
async def test_disconnect_with_processed(orchestrator):
    orchestrator.processed = 5
    await orchestrator.disconnect()
    orchestrator.nc.close.assert_called_once()

@pytest.mark.asyncio
async def test_start_listener_subscribes_correctly(orchestrator):
    await orchestrator.start_listener()
    orchestrator.nc.subscribe.assert_called_once()
    args, _ = orchestrator.nc.subscribe.call_args
    assert args[0] == SUBJECT_COMPLETED

@pytest.mark.asyncio
async def test_connect_logs_url(caplog):
    caplog.set_level(logging.INFO)
    with patch("nats.connect", new_callable=AsyncMock):
        orch = AgentOrchestrator()
        await orch.connect("nats://test:4222")
        assert "Connected to NATS at nats://test:4222" in caplog.text

@pytest.mark.asyncio
async def test_send_task_logs_start_and_complete(orchestrator, caplog):
    caplog.set_level(logging.INFO)
    payload = {"markers": [{"id": "TEST", "confidence": 1.0}]}

    async def simulate_response():
        for _ in range(100):
            for task_id, future in list(orchestrator.results.items()):
                if not future.done():
                    future.set_result({
                        "transaction_id": task_id,
                        "risk_score": 50,
                        "verdict": "MEDIUM",
                        "reason": "Log test"
                    })
                    return
            await asyncio.sleep(0.01)

    asyncio.create_task(simulate_response())
    await orchestrator.send_task(payload)
    assert "Sending task" in caplog.text
    assert "completed" in caplog.text

@pytest.mark.asyncio
async def test_disconnect_logs_processed(orchestrator, caplog):
    caplog.set_level(logging.INFO)
    orchestrator.processed = 3
    await orchestrator.disconnect()
    assert "Total tasks processed: 3" in caplog.text
