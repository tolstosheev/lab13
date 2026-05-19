import asyncio
import json
import logging
import uuid
from typing import cast, Dict, Optional, Any, List, TypedDict
import nats
from nats.aio.msg import Msg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("orchestrator.log", mode="w", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

SUBJECT_RISK_ASSESSMENT = "tasks.risk_assessment"
SUBJECT_COMPLETED = "tasks.completed"
MAX_RETRIES = 3

class RiskRequest(TypedDict):
    transaction_id: str
    markers: List[Dict[str, Any]]

class RiskResponse(TypedDict):
    transaction_id: str
    risk_score: int
    verdict: str
    reason: str

class AgentOrchestrator:
    def __init__(self) -> None:
        self.nc: Optional[nats.NATS] = None
        self.results: Dict[str, asyncio.Future] = {}
        self.processed: int = 0

    async def connect(self, url: str = "nats://localhost:4222") -> None:
        self.nc = await nats.connect(url)
        logger.info("Connected to NATS at %s", url)

    async def start_listener(self) -> None:
        await self.nc.subscribe(SUBJECT_COMPLETED, cb=self.on_result)
        logger.info("Listening on %s", SUBJECT_COMPLETED)

    async def on_result(self, msg: Msg) -> None:
        try:
            data = json.loads(msg.data.decode())
            if not isinstance(data, dict):
                return
            task_id = data.get("transaction_id")
            if task_id and task_id in self.results:
                future = self.results[task_id]
                if not future.done():
                    future.set_result(data)
                del self.results[task_id]
            elif task_id:
                logger.debug("Unknown task result: %s", task_id)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.debug("Failed to decode result message: %s", e)

    async def send_task(self, payload: Dict[str, Any], timeout: int = 30) -> RiskResponse:
        if not self.nc or not self.nc.is_connected:
            raise ConnectionError("Not connected to NATS server")

        markers = payload.get("markers")
        if not isinstance(markers, list):
            raise ValueError("Payload 'markers' must be a list")

        last_error: Optional[Exception] = None
        for attempt in range(1, MAX_RETRIES + 1):
            task_data: RiskRequest = {
                "transaction_id": str(uuid.uuid4()),
                "markers": markers
            }
            task_id = task_data["transaction_id"]
            logger.info("Sending task %s with %d markers (attempt %d/%d)",
                        task_id, len(markers), attempt, MAX_RETRIES)

            future = asyncio.get_running_loop().create_future()
            self.results[task_id] = future

            try:
                await self.nc.publish(SUBJECT_RISK_ASSESSMENT, json.dumps(task_data).encode())
                result = await asyncio.wait_for(future, timeout=timeout)
                self.processed += 1
                logger.info("Task %s completed: score %d, verdict %s (processed: %d)",
                            task_id, result["risk_score"], result["verdict"], self.processed)
                return cast(RiskResponse, result)
            except asyncio.TimeoutError:
                last_error = TimeoutError(f"Task {task_id} timed out after {timeout} seconds")
                logger.warning("Task %s timed out (attempt %d/%d)", task_id, attempt, MAX_RETRIES)
            except Exception as e:
                last_error = RuntimeError(f"Task {task_id} failed: {str(e)}")
                logger.warning("Task %s failed (attempt %d/%d): %s", task_id, attempt, MAX_RETRIES, e)
            finally:
                if task_id in self.results:
                    del self.results[task_id]

        raise last_error

    async def disconnect(self) -> None:
        if self.nc:
            logger.info("Disconnecting from NATS. Total tasks processed: %d", self.processed)
            await self.nc.close()
