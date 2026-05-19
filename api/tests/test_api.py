import asyncio
import pytest

import api.main


class TestHealth:
    def test_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}

    def test_wrong_method(self, client):
        resp = client.post("/health")
        assert resp.status_code == 405

    def test_wrong_path(self, client):
        resp = client.get("/healthx")
        assert resp.status_code == 404


class TestAssess:
    SUCCESS_CASES = [
        pytest.param(
            {"markers": [{"id": "BLACKLIST_HIT", "confidence": 1.0}]},
            80, "MEDIUM",
            "Score 80 based on: [BLACKLIST_HIT(80.00)]",
            id="high_blacklist"
        ),
        pytest.param(
            {"markers": [{"id": "HIGH_FREQUENCY", "confidence": 0.3}]},
            30, "LOW",
            "Score 30 based on: [HIGH_FREQUENCY(30.00)]",
            id="low_frequency"
        ),
        pytest.param(
            {"markers": [{"id": "GEO_ANOMALY", "confidence": 0.5}]},
            50, "MEDIUM",
            "Score 50 based on: [GEO_ANOMALY(50.00)]",
            id="medium_geo"
        ),
        pytest.param(
            {"markers": []},
            0, "LOW",
            "Score 0 based on: []",
            id="empty_markers"
        ),
    ]

    @pytest.mark.parametrize(
        "payload, expected_score, expected_verdict, expected_reason",
        SUCCESS_CASES,
    )
    def test_success(
        self, client, mock_orch,
        payload, expected_score, expected_verdict, expected_reason,
    ):
        mock_orch.send_task.return_value = {
            "risk_score": expected_score,
            "verdict": expected_verdict,
            "reason": expected_reason,
        }
        resp = client.post("/assess", json=payload)
        assert resp.status_code == 200
        data = resp.json()
        assert data["risk_score"] == expected_score
        assert data["verdict"] == expected_verdict
        assert data["reason"] == expected_reason

    def test_timeout(self, client, mock_orch):
        mock_orch.send_task.side_effect = asyncio.TimeoutError()
        resp = client.post(
            "/assess",
            json={"markers": [{"id": "T", "confidence": 0.5}]},
        )
        assert resp.status_code == 504
        assert "time" in resp.json()["detail"].lower()

    def test_value_error(self, client, mock_orch):
        mock_orch.send_task.side_effect = ValueError("Bad payload")
        resp = client.post(
            "/assess",
            json={"markers": [{"id": "T", "confidence": 0.5}]},
        )
        assert resp.status_code == 422
        assert "Bad payload" in resp.json()["detail"]

    def test_internal_error(self, client, mock_orch):
        mock_orch.send_task.side_effect = Exception("Unexpected")
        resp = client.post(
            "/assess",
            json={"markers": [{"id": "T", "confidence": 0.5}]},
        )
        assert resp.status_code == 500

    INVALID_BODIES = [
        pytest.param({}, id="empty_object"),
        pytest.param({"markers": "not_a_list"}, id="markers_not_list"),
        pytest.param(
            {"markers": [{"id": 123, "confidence": 0.5}]},
            id="id_not_string",
        ),
        pytest.param(
            {"markers": [{"id": "T", "confidence": "high"}]},
            id="confidence_not_number",
        ),
        pytest.param(
            {"markers": [{"id": "T"}]},
            id="missing_confidence",
        ),
        pytest.param(
            {"markers": [{"confidence": 0.5}]},
            id="missing_id",
        ),
        pytest.param(
            {"markers": [{"id": "T", "confidence": -1}]},
            id="negative_confidence",
        ),
        pytest.param(
            {"markers": [{"id": "T", "confidence": 1.5}]},
            id="overflow_confidence",
        ),
    ]

    @pytest.mark.parametrize("payload", INVALID_BODIES)
    def test_invalid_body(self, client, payload):
        resp = client.post("/assess", json=payload)
        assert resp.status_code == 422

    def test_wrong_method(self, client):
        resp = client.get("/assess")
        assert resp.status_code == 405


class TestAssessAsync:
    def test_creates_task(self, client):
        resp = client.post(
            "/assess/async",
            json={"markers": [{"id": "T", "confidence": 0.5}]},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "pending"
        assert len(data["task_id"]) > 0

    def test_invalid_body(self, client):
        resp = client.post("/assess/async", json={})
        assert resp.status_code == 422

    def test_background_stores_entry(self, client):
        resp = client.post(
            "/assess/async",
            json={"markers": [{"id": "T", "confidence": 0.5}]},
        )
        task_id = resp.json()["task_id"]
        assert task_id in api.main.background_tasks

    def test_wrong_method(self, client):
        resp = client.get("/assess/async")
        assert resp.status_code == 405


class TestStatus:
    def test_completed(self, client):
        api.main.background_tasks["t1"] = {
            "status": "completed",
            "result": {
                "risk_score": 80, "verdict": "HIGH", "reason": "Critical",
            },
        }
        resp = client.get("/status/t1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["task_id"] == "t1"
        assert data["status"] == "completed"
        assert data["result"]["risk_score"] == 80
        assert data["result"]["verdict"] == "HIGH"
        assert data["error"] is None

    def test_pending(self, client):
        api.main.background_tasks["t2"] = {"status": "pending"}
        resp = client.get("/status/t2")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "pending"
        assert data["result"] is None
        assert data["error"] is None

    def test_failed(self, client):
        api.main.background_tasks["t3"] = {
            "status": "failed", "error": "Something went wrong",
        }
        resp = client.get("/status/t3")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "failed"
        assert data["error"] == "Something went wrong"

    def test_not_found(self, client):
        resp = client.get("/status/non-existent")
        assert resp.status_code == 404

    def test_wrong_method(self, client):
        resp = client.post("/status/t1")
        assert resp.status_code == 405


class TestBatch:
    def test_success(self, client, mock_orch):
        mock_orch.send_task.return_value = {
            "risk_score": 50, "verdict": "MEDIUM", "reason": "OK",
        }
        resp = client.post("/assess/batch", json={
            "tasks": [
                {"markers": [{"id": "A", "confidence": 0.5}]},
                {"markers": [{"id": "B", "confidence": 0.3}]},
            ],
        })
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["results"]) == 2
        for r in data["results"]:
            assert r["risk_score"] == 50
            assert r["verdict"] == "MEDIUM"

    def test_empty_tasks(self, client):
        resp = client.post("/assess/batch", json={"tasks": []})
        assert resp.status_code == 200
        assert resp.json()["results"] == []

    def test_timeout(self, client, mock_orch):
        mock_orch.send_task.side_effect = asyncio.TimeoutError()
        resp = client.post("/assess/batch", json={
            "tasks": [{"markers": [{"id": "A", "confidence": 0.5}]}],
        })
        assert resp.status_code == 504

    def test_value_error(self, client, mock_orch):
        mock_orch.send_task.side_effect = ValueError("Invalid marker")
        resp = client.post("/assess/batch", json={
            "tasks": [{"markers": [{"id": "A", "confidence": 0.5}]}],
        })
        assert resp.status_code == 422

    def test_invalid_body(self, client):
        resp = client.post("/assess/batch", json={})
        assert resp.status_code == 422

    def test_wrong_method(self, client):
        resp = client.get("/assess/batch")
        assert resp.status_code == 405


class TestRateLimit:
    def test_exceeded(self, client):
        for _ in range(10):
            resp = client.post(
                "/assess",
                json={"markers": [{"id": "T", "confidence": 0.5}]},
            )
            assert resp.status_code == 200

        resp = client.post(
            "/assess",
            json={"markers": [{"id": "T", "confidence": 0.5}]},
        )
        assert resp.status_code == 429
        assert "exceed" in resp.json()["detail"].lower()



