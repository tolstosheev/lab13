import asyncio
import json
import uuid
from typing import Dict, Optional, Any, List, TypedDict
import nats

SUBJECT_RISK_ASSESSMENT = "tasks.risk_assessment"
SUBJECT_COMPLETED = "tasks.completed"

class RiskRequest(TypedDict):
    transaction_id: str
    markers: List[Dict[str, Any]]

class RiskResponse(TypedDict):
    transaction_id: str
    risk_score: int
    verdict: str
    reason: str

class AgentOrchestrator:
    def __init__(self):
        self.nc: Optional[nats.NATS] = None
        self.results: Dict[str, asyncio.Future] = {}

    async def connect(self, url: str = "nats://localhost:4222") -> None:
        self.nc = await nats.connect(url)

    async def start_listener(self) -> None:
        await self.nc.subscribe(SUBJECT_COMPLETED, cb=self.on_result)

    async def on_result(self, msg: nats.Msg) -> None:
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
        except (json.JSONDecodeError, UnicodeDecodeError):
            pass

    async def send_task(self, payload: Dict[str, Any], timeout: int = 30) -> RiskResponse:
        if not self.nc or not self.nc.is_connected:
            raise ConnectionError("Not connected to NATS server")

        markers = payload.get("markers")
        if not isinstance(markers, list):
            raise ValueError("Payload 'markers' must be a list")

        task_id = str(uuid.uuid4())
        task_data: RiskRequest = {
            "transaction_id": task_id,
            "markers": markers
        }
        
        future = asyncio.get_running_loop().create_future()
        self.results[task_id] = future

        try:
            await self.nc.publish(SUBJECT_RISK_ASSESSMENT, json.dumps(task_data).encode())
            result = await asyncio.wait_for(future, timeout=timeout)
            return result # type: ignore
        except asyncio.TimeoutError:
            raise TimeoutError(f"Task {task_id} timed out after {timeout} seconds")
        except Exception as e:
            raise RuntimeError(f"Task {task_id} failed: {str(e)}")
        finally:
            if task_id in self.results:
                del self.results[task_id]

    async def disconnect(self) -> None:
        if self.nc:
            await self.nc.close()
