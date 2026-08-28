#!/usr/bin/env python3
"""Read-only ReyT portfolio dry run with the configured per-strategy allocation cap."""
import asyncio
from collections import Counter
from decimal import Decimal

import aiomysql
import ReyT_strategy_engine_unified_1b_execution_status_v2 as m


async def main():
    engine = m.PaperEngine()
    pool = await aiomysql.create_pool(
        minsize=1,
        maxsize=max(2, m.MYSQL_POOL_MAX),
        **m.connection_kwargs(),
    )
    engine.pool = pool
    try:
        async with pool.acquire() as raw:
            db = m.DB(raw)
            now = m.tehran_now().replace(microsecond=0)
            account = m.Account(
                account_id=0,
                account_name="DRY_RUN",
                initial_equity=m.INITIAL_CAPITAL_RIAL,
                equity=m.INITIAL_CAPITAL_RIAL,
                realized=m.D0,
                unrealized=m.D0,
                reserved_risk=m.D0,
                allocated_capital=m.D0,
                open_count=0,
                high_watermark=m.INITIAL_CAPITAL_RIAL,
            )
            revision = await db.fetchval(
                f"SELECT state_value FROM {m.qname('collector_state')} WHERE state_key='live_source_revision'"
            )
            if not revision:
                raise RuntimeError("Collector live_source_revision is missing.")

            quotes, _, _, books = await engine._load_market_snapshot(db)
            hist_vol = await engine._load_historical_volatility(db)
            await engine._load_expected_return_history(db, now)
            engine._er_iv_multiplier_cache.clear()
            candidates = engine._build_candidates(
                account, quotes, books, hist_vol, str(revision), now
            )
            engine._apply_expected_return_hurdle(candidates)
            all_candidates = [c for group in candidates.values() for c in group]
            actionable = [
                c for c in all_candidates
                if c.final_signal in {"CANDIDATE", "STRONG_CANDIDATE"}
            ]
            ranked = sorted(actionable, key=lambda c: c.priority_tuple, reverse=True)
            execution_books = engine._execution_books(books)
            available_cash = account.available_cash
            strategy_cap = account.initial_equity * m.MAX_STRATEGY_ALLOCATION_PCT / m.D100
            strategy_allocated = Counter()
            executed = []
            rejects = Counter()
            by_strategy = Counter()

            for c in ranked:
                remaining_cap = max(m.D0, strategy_cap - strategy_allocated[c.strategy])
                d = engine._plan_execution(
                    c,
                    execution_books,
                    available_cash,
                    max_entry_capital=remaining_cap,
                )
                if d.plan is None or d.plan.units <= 0:
                    rejects[d.reason_code] += 1
                    continue
                plan = d.plan
                if any(
                    execution_books.get(leg.ins_code) is None
                    or execution_books[leg.ins_code].capacity_units(leg) < plan.units
                    for leg in c.legs
                ):
                    rejects["SHARED_DEPTH_CONSUMED"] += 1
                    continue
                for leg in c.legs:
                    if not execution_books[leg.ins_code].consume(leg, plan.units):
                        raise RuntimeError("In-memory order-book consumption mismatch.")
                capital = plan.metrics.unit_capital * Decimal(plan.units)
                available_cash -= capital
                strategy_allocated[c.strategy] += capital
                by_strategy[c.strategy] += 1
                executed.append((c, plan, capital))

            await raw.rollback()

            print("=" * 94)
            print("REYT READ-ONLY TRUE PORTFOLIO DRY RUN — PER-STRATEGY CAP ENABLED")
            print("=" * 94)
            print(f"Tehran time                  : {now}")
            print(f"Initial capital              : {m.INITIAL_CAPITAL_TOMAN:,.0f} toman")
            print(f"Min final max loss / entry   : {m.MIN_EXECUTION_RISK_TOMAN:,.0f} toman")
            print(f"Max loss / entry             : {m.FIXED_RISK_PER_TRADE_TOMAN:,.0f} toman")
            print(f"Max strategy allocation      : {m.MAX_STRATEGY_ALLOCATION_PCT}%")
            print(f"Max allocation / strategy    : {strategy_cap/m.TOMAN_TO_RIAL:,.0f} toman")
            print(f"Structures                   : {len(all_candidates)}")
            print(f"Economic actionable          : {len(actionable)}")
            print(f"Actually executable          : {len(executed)}")
            print(f"Capital allocated            : {(account.available_cash-available_cash)/m.TOMAN_TO_RIAL:,.0f} toman")
            print(f"Cash remaining               : {available_cash/m.TOMAN_TO_RIAL:,.0f} toman")
            print()
            print("BY STRATEGY")
            for strategy in [
                "COVERED_CALL", "PROTECTIVE_PUT", "BULL_CALL_SPREAD",
                "BEAR_PUT_SPREAD", "LONG_STRADDLE",
            ]:
                alloc = Decimal(strategy_allocated[strategy]) / m.TOMAN_TO_RIAL
                pct = (Decimal(strategy_allocated[strategy]) / account.initial_equity * 100) if account.initial_equity else m.D0
                print(f"  {strategy:<22} positions={by_strategy[strategy]:>4}  allocation={alloc:>15,.0f} toman  pct={pct:>7.3f}%")
            print()
            print("REJECTIONS")
            for reason, n in rejects.most_common():
                print(f"  {reason:<42} {n:>5}")
            print("=" * 94)
            print("DRY RUN COMPLETE — ZERO DATABASE WRITES")
            print("=" * 94)
    finally:
        pool.close()
        await pool.wait_closed()


if __name__ == "__main__":
    asyncio.run(main())
