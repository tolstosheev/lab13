import asyncio
import logging

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from orchestrator import AgentOrchestrator, SUBJECT_RISK_ASSESSMENT, SUBJECT_COMPLETED, MAX_RETRIES
from .conftest import resolve_futures


@pytest.mark.asyncio
async def test_connect() -> None:
    with patch("nats.connect", new_callable=AsyncMock) as mock_connect:
        orch = AgentOrchestrator()
        url = "nats://test:4222"
        await orch.connect(url)
        mock_connect.assert_called_once_with(url)


@pytest.mark.parametrize("markers, expected_score, expected_verdict", [
    ([{"id": "TEST", "confidence": 1.0}], 50, "MEDIUM"),
    ([{"id": "LOW_RISK", "confidence": 0.1}], 30, "LOW"),
    ([], 0, "LOW"),
    ([{"id": "HIGH_RISK", "confidence": 1.0}], 80, "MEDIUM"),
])
@pytest.mark.asyncio
async def test_send_task_success(orchestrator, markers, expected_score, expected_verdict) -> None:
    payload = {"markers": markers}

    asyncio.create_task(resolve_futures(
        orchestrator.results,
        {"risk_score": expected_score, "verdict": expected_verdict, "reason": "Parametrized test"},
    ))

    result = await orchestrator.send_task(payload)

    assert result["risk_score"] == expected_score
    assert result["verdict"] == expected_verdict
    assert len(orchestrator.results) == 0
    orchestrator.nc.publish.assert_called_once()
    args, _ = orchestrator.nc.publish.call_args
    assert args[0] == SUBJECT_RISK_ASSESSMENT


@pytest.mark.parametrize("timeout_val", [0.01, 0.001])
@pytest.mark.asyncio
async def test_send_task_timeout(orchestrator, timeout_val) -> None:
    payload = {"markers": []}
    with pytest.raises(TimeoutError):
        await orchestrator.send_task(payload, timeout=timeout_val)
    assert len(orchestrator.results) == 0


@pytest.mark.asyncio
async def test_send_task_not_connected(orchestrator) -> None:
    orchestrator.nc.is_connected = False
    with pytest.raises(ConnectionError):
        await orchestrator.send_task({"markers": []})


@pytest.mark.parametrize("payload, expected_error", [
    ({"markers": "not a list"}, ValueError),
    ({"wrong_key": []}, ValueError),
    ({}, ValueError),
])
@pytest.mark.asyncio
async def test_send_task_invalid_payloads(orchestrator, payload, expected_error) -> None:
    with pytest.raises(expected_error):
        await orchestrator.send_task(payload)
    assert len(orchestrator.results) == 0


@pytest.mark.parametrize("payload, expected_error", [
    ({"no_markers_field": "x"}, ValueError),
    ({"markers": None}, ValueError),
])
@pytest.mark.asyncio
async def test_send_task_additional_invalid(orchestrator, payload, expected_error) -> None:
    with pytest.raises(expected_error):
        await orchestrator.send_task(payload)


@pytest.mark.parametrize("payload_bytes, expected_score, expected_verdict", [
    (b'{"transaction_id": "t1", "risk_score": 10, "verdict": "LOW", "reason": "OK"}', 10, "LOW"),
    (b'{"transaction_id": "t1", "risk_score": 90, "verdict": "HIGH", "reason": "Bad"}', 90, "HIGH"),
    (b'{"transaction_id": "t1", "risk_score": 55, "verdict": "MEDIUM", "reason": "Border"}', 55, "MEDIUM"),
])
@pytest.mark.asyncio
async def test_on_result_valid(orchestrator, payload_bytes, expected_score, expected_verdict) -> None:
    task_id = "t1"
    future = asyncio.Future()
    orchestrator.results[task_id] = future

    msg = MagicMock()
    msg.data = payload_bytes

    await orchestrator.on_result(msg)

    assert future.done()
    assert future.result()["risk_score"] == expected_score
    assert future.result()["verdict"] == expected_verdict
    assert task_id not in orchestrator.results


@pytest.mark.parametrize("malformed_payload", [
    b'invalid json',
    b'{"wrong_id": "t1"}',
    b'[]',
    b'',
])
@pytest.mark.asyncio
async def test_on_result_malformed(orchestrator, malformed_payload) -> None:
    task_id = "t1"
    future = asyncio.Future()
    orchestrator.results[task_id] = future

    msg = MagicMock()
    msg.data = malformed_payload

    await orchestrator.on_result(msg)

    assert not future.done()
    assert task_id in orchestrator.results


@pytest.mark.asyncio
async def test_on_result_already_done(orchestrator) -> None:
    task_id = "t1"
    future = asyncio.Future()
    future.set_result("already done")
    orchestrator.results[task_id] = future

    msg = MagicMock()
    msg.data = b'{"transaction_id": "t1", "risk_score": 10, "verdict": "LOW", "reason": "Late"}'

    await orchestrator.on_result(msg)

    assert future.done()
    assert future.result() == "already done"
    assert task_id not in orchestrator.results


@pytest.mark.asyncio
async def test_on_result_unknown_task(orchestrator) -> None:
    msg = MagicMock()
    msg.data = b'{"transaction_id": "unknown", "risk_score": 10}'

    await orchestrator.on_result(msg)
    assert len(orchestrator.results) == 0


@pytest.mark.asyncio
async def test_disconnect(orchestrator) -> None:
    await orchestrator.disconnect()
    orchestrator.nc.close.assert_called_once()


@pytest.mark.asyncio
async def test_disconnect_when_not_connected() -> None:
    orch = AgentOrchestrator()
    await orch.disconnect()


@pytest.mark.asyncio
async def test_processed_counter_increments(orchestrator) -> None:
    assert orchestrator.processed == 0
    payload = {"markers": [{"id": "TEST", "confidence": 1.0}]}

    asyncio.create_task(resolve_futures(
        orchestrator.results,
        {"risk_score": 30, "verdict": "LOW", "reason": "Counter test"},
    ))
    await orchestrator.send_task(payload)
    assert orchestrator.processed == 1


@pytest.mark.asyncio
async def test_disconnect_with_processed(orchestrator) -> None:
    orchestrator.processed = 5
    await orchestrator.disconnect()
    orchestrator.nc.close.assert_called_once()


@pytest.mark.asyncio
async def test_start_listener_subscribes_correctly(orchestrator) -> None:
    await orchestrator.start_listener()
    orchestrator.nc.subscribe.assert_called_once()
    args, _ = orchestrator.nc.subscribe.call_args
    assert args[0] == SUBJECT_COMPLETED


@pytest.mark.asyncio
async def test_connect_logs_url(caplog) -> None:
    caplog.set_level(logging.INFO)
    with patch("nats.connect", new_callable=AsyncMock):
        orch = AgentOrchestrator()
        await orch.connect("nats://test:4222")
        assert "Connected to NATS at nats://test:4222" in caplog.text


@pytest.mark.asyncio
async def test_send_task_logs(orchestrator, caplog) -> None:
    caplog.set_level(logging.INFO)
    payload = {"markers": [{"id": "TEST", "confidence": 1.0}]}

    asyncio.create_task(resolve_futures(
        orchestrator.results,
        {"risk_score": 50, "verdict": "MEDIUM", "reason": "Log test"},
    ))
    await orchestrator.send_task(payload)

    assert "Sending task" in caplog.text
    assert "completed" in caplog.text


@pytest.mark.asyncio
async def test_disconnect_logs_processed(orchestrator, caplog) -> None:
    caplog.set_level(logging.INFO)
    orchestrator.processed = 3
    await orchestrator.disconnect()
    assert "Total tasks processed: 3" in caplog.text


@pytest.mark.asyncio
async def test_retry_success_on_second_attempt(orchestrator) -> None:
    payload = {"markers": [{"id": "RETRY", "confidence": 1.0}]}
    call_count = 0

    async def publish_side(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise Exception("Temporary network error")

    orchestrator.nc.publish.side_effect = publish_side

    asyncio.create_task(resolve_futures(
        orchestrator.results,
        {"risk_score": 30, "verdict": "LOW", "reason": "Retry success"},
    ))
    result = await orchestrator.send_task(payload, timeout=5)

    assert result["risk_score"] == 30
    assert result["verdict"] == "LOW"
    assert orchestrator.nc.publish.call_count == 2


@pytest.mark.asyncio
async def test_retry_exhaustion(orchestrator) -> None:
    payload = {"markers": []}
    with pytest.raises(TimeoutError) as exc_info:
        await orchestrator.send_task(payload, timeout=0.01)
    assert "timed out" in str(exc_info.value)
    assert orchestrator.nc.publish.call_count == MAX_RETRIES


@pytest.mark.parametrize("payload", [
    {"markers": "not a list"},
    {"wrong_key": []},
    {},
])
@pytest.mark.asyncio
async def test_retry_no_retry_on_validation_error(orchestrator, payload) -> None:
    with pytest.raises(ValueError):
        await orchestrator.send_task(payload, timeout=5)
    assert orchestrator.nc.publish.call_count == 0


@pytest.mark.asyncio
async def test_retry_logs_warning_on_each_retry(orchestrator, caplog) -> None:
    caplog.set_level(logging.WARNING)
    payload = {"markers": []}
    with pytest.raises(TimeoutError):
        await orchestrator.send_task(payload, timeout=0.01)
    warning_count = sum(
        1 for rec in caplog.records
        if rec.levelno == logging.WARNING and "timed out" in rec.message
    )
    assert warning_count == MAX_RETRIES


@pytest.mark.asyncio
async def test_concurrent_tasks(orchestrator) -> None:
    payload = {"markers": [{"id": "CONCURRENT", "confidence": 0.5}]}

    asyncio.create_task(resolve_futures(
        orchestrator.results,
        {"risk_score": 10, "verdict": "LOW", "reason": "Concurrent test"},
        count=3,
    ))

    tasks = [orchestrator.send_task(payload) for _ in range(3)]
    results = await asyncio.gather(*tasks)

    assert len(results) == 3
    assert all(r["risk_score"] == 10 for r in results)
    assert orchestrator.processed == 3
    assert len(orchestrator.results) == 0
