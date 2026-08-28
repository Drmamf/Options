#!/usr/bin/env python3
"""Initialize/verify the configured ReyT paper account without scanning or trading."""
import asyncio
import ReyT_strategy_engine_unified_1b_execution_status_v2 as m

async def main() -> None:
    engine = m.PaperEngine()
    await engine.open()
    try:
        print("PAPER_ACCOUNT_INITIALIZED")
        print(f"ACCOUNT_NAME={m.ACCOUNT_NAME}")
        print(f"INITIAL_CAPITAL_TOMAN={m.INITIAL_CAPITAL_TOMAN}")
        print(f"MAX_RISK_PER_POSITION_TOMAN={m.FIXED_RISK_PER_TRADE_TOMAN}")
        print(f"MAX_STRATEGY_ALLOCATION_PCT={m.MAX_STRATEGY_ALLOCATION_PCT}")
    finally:
        await engine.close()

if __name__ == '__main__':
    asyncio.run(main())
