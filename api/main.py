import asyncio
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel

from orchestrator import AgentOrchestrator

logger = logging.getLogger("api")

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
RATE_LIMIT = int(os.getenv("RATE_LIMIT", "10"))
RATE_WINDOW = 60.0

class MarkerModel(BaseModel):
    id: str
    confidence: float

class AssessRequest(BaseModel):
    markers: List[MarkerModel]

class AssessResponse(BaseModel):
    risk_score: int
    verdict: str
    reason: str

class TaskStatus(BaseModel):
    task_id: str
    status: str
    result: Optional[AssessResponse] = None
    error: Optional[str] = None

class BatchRequest(BaseModel):
    tasks: List[AssessRequest]

class BatchResponse(BaseModel):
    results: List[AssessResponse]

class HealthResponse(BaseModel):
    status: str = "ok"


orchestrator: AgentOrchestrator = None
background_tasks: Dict[str, Dict[str, Any]] = {}
rate_history: List[float] = []


async def process_background(task_id: str, payload: Dict[str, Any]) -> None:
    try:
        result = await orchestrator.send_task(payload, timeout=30)
        background_tasks[task_id] = {
            "status": "completed",
            "result": {
                "risk_score": result["risk_score"],
                "verdict": result["verdict"],
                "reason": result["reason"],
            }
        }
    except asyncio.TimeoutError:
        background_tasks[task_id] = {"status": "failed", "error": "Task timed out"}
    except Exception as e:
        background_tasks[task_id] = {"status": "failed", "error": str(e)}


def check_rate_limit(client_ip: str) -> None:
    now = time.time()
    global rate_history
    rate_history = [t for t in rate_history if now - t < RATE_WINDOW]
    if len(rate_history) >= RATE_LIMIT:
        raise HTTPException(status_code=429, detail="Rate limit exceeded")
    rate_history.append(now)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global orchestrator
    orchestrator = AgentOrchestrator()
    await orchestrator.connect(NATS_URL)
    await orchestrator.start_listener()
    logger.info("API started, connected to NATS at %s", NATS_URL)
    yield
    await orchestrator.disconnect()
    logger.info("API shut down")


app = FastAPI(title="Fraud Detection API", version="0.1.0", lifespan=lifespan)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = str(uuid.uuid4())[:8]
    start = time.time()
    response = await call_next(request)
    duration = time.time() - start
    logger.info("[%s] %s %s - %d (%.3fs)", request_id, request.method, request.url.path, response.status_code, duration)
    return response


@app.get("/health", response_model=HealthResponse)
async def health():
    return HealthResponse()


@app.post("/assess", response_model=AssessResponse)
async def assess(request: AssessRequest, fastapi_request: Request):
    check_rate_limit(fastapi_request.client.host)
    payload = {
        "markers": [m.model_dump() for m in request.markers]
    }
    try:
        result = await orchestrator.send_task(payload, timeout=30)
        return AssessResponse(
            risk_score=result["risk_score"],
            verdict=result["verdict"],
            reason=result["reason"],
        )
    except asyncio.TimeoutError:
        raise HTTPException(status_code=504, detail="Task timed out")
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error("Request failed: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.post("/assess/async", response_model=TaskStatus)
async def assess_async(request: AssessRequest, fastapi_request: Request):
    check_rate_limit(fastapi_request.client.host)
    task_id = str(uuid.uuid4())
    background_tasks[task_id] = {"status": "pending"}
    payload = {
        "markers": [m.model_dump() for m in request.markers]
    }
    asyncio.create_task(process_background(task_id, payload))
    return TaskStatus(task_id=task_id, status="pending")


@app.get("/status/{task_id}", response_model=TaskStatus)
async def get_status(task_id: str):
    entry = background_tasks.get(task_id)
    if entry is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return TaskStatus(task_id=task_id, **entry)


@app.post("/assess/batch", response_model=BatchResponse)
async def assess_batch(request: BatchRequest, fastapi_request: Request):
    check_rate_limit(fastapi_request.client.host)
    results = []
    for i, task in enumerate(request.tasks):
        payload = {
            "markers": [m.model_dump() for m in task.markers]
        }
        try:
            result = await orchestrator.send_task(payload, timeout=60)
            results.append(AssessResponse(
                risk_score=result["risk_score"],
                verdict=result["verdict"],
                reason=result["reason"],
            ))
        except Exception as e:
            logger.error("Batch task %d failed: %s", i, e)
            raise HTTPException(status_code=500, detail=f"Batch task {i} failed: {str(e)}")
    return BatchResponse(results=results)
