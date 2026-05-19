import asyncio
from orchestrator import AgentOrchestrator

async def main() -> None:
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
        print(f"Running scenario: {scenario['name']}")
        try:
            result = await orchestrator.send_task(scenario['payload'])
            print(f"Result: Score={result['risk_score']}, Verdict={result['verdict']}, Reason={result['reason']}")
        except Exception as e:
            print(f"Error: {e}")
        print("-" * 20)

    await orchestrator.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
