import asyncio
import logging

from orchestrator import AgentOrchestrator

logger = logging.getLogger("main")

async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler("orchestrator.log", mode="w", encoding="utf-8"),
            logging.StreamHandler()
        ]
    )

    orchestrator = AgentOrchestrator()
    await orchestrator.connect()
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
        }
    ]

    for scenario in test_scenarios:
        logger.info("Running scenario: %s", scenario["name"])
        try:
            result = await orchestrator.send_task(scenario["payload"])
            logger.info("Result: Score=%d, Verdict=%s, Reason=%s",
                        result["risk_score"], result["verdict"], result["reason"])
        except Exception as e:
            logger.error("Scenario '%s' failed: %s", scenario["name"], e)

    await orchestrator.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
