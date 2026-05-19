import asyncio
import logging
import os
import signal

from orchestrator import AgentOrchestrator

NATS_URL = os.getenv("NATS_URL", "nats://localhost:4222")
logger = logging.getLogger("main")
shutdown_event = asyncio.Event()


def handle_signal() -> None:
    logger.info("Shutdown signal received, finishing...")
    shutdown_event.set()


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler("orchestrator.log", mode="w", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_signal)
        except NotImplementedError:
            pass

    orchestrator = AgentOrchestrator()
    await orchestrator.connect(NATS_URL)
    await orchestrator.start_listener()

    test_scenarios = [
        {
            "name": "HIGH Risk (Blacklist)",
            "payload": {"markers": [{"id": "BLACKLIST_HIT", "confidence": 1.0}]}
        },
        {
            "name": "MEDIUM Risk (Travel + Velocity)",
            "payload": {"markers": [
                {"id": "IMPOSSIBLE_TRAVEL", "confidence": 0.8},
                {"id": "VELOCITY_ATTACK", "confidence": 0.5}
            ]}
        },
        {
            "name": "LOW Risk (Empty)",
            "payload": {"markers": []}
        },
        {
            "name": "Invalid Payload",
            "payload": {"markers": "not a list"}
        },
        {
            "name": "Retry Exhaustion (timeout 1ms)",
            "payload": {"markers": [{"id": "TEST", "confidence": 0.5}]},
            "timeout": 0.001
        }
    ]

    try:
        for scenario in test_scenarios:
            if shutdown_event.is_set():
                break
            logger.info("Running scenario: %s", scenario["name"])
            try:
                timeout = scenario.get("timeout", 30)
                result = await orchestrator.send_task(scenario["payload"], timeout=timeout)
                logger.info("Result: Score=%d, Verdict=%s, Reason=%s",
                            result["risk_score"], result["verdict"], result["reason"])
            except Exception as e:
                logger.error("Scenario '%s' failed: %s", scenario["name"], e)
    finally:
        await orchestrator.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
