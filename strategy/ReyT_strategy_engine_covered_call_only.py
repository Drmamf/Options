# -*- coding: utf-8 -*-
"""
ReyT temporary Covered-Call-only policy launcher.

This file loads the existing production strategy engine and overrides only the
temporary trading policy requested on 2026-08-17:

  * COVERED_CALL is the only generated/persisted/executed strategy.
  * Protective Put / Bull Call Spread / Bear Put Spread / Long Straddle are
    discarded before persistence and are never written by _upsert_signals.
  * Minimum Covered Call annualized_return_pct: 50%.
  * Annualized-return hurdle is checked both at signal quote and again after
    final five-level order-book VWAP.
  * Position sizing is based on Covered Call NET CAPITAL, not generic max-loss:
        stock buy cash cost (including buy fee)
      - option sale proceeds (after option-sale fee)
  * Maximum final net capital: 1,000,000 toman.
  * Minimum final net capital:  200,000 toman.
  * Integer contract granularity is preserved. A 1.2M candidate is reduced to the
    largest whole-contract size whose net capital is <= 1M. If one whole
    strategy unit itself exceeds 1M, it is not executed.
  * Gross underlying purchase still must be affordable before option premium is
    credited, preserving the base engine's cash-safety rule.
  * Old dual history/IV expected-return gate is replaced by the explicit
    Covered Call annualized-return gate for this temporary mode.
  * Score remains ranking/context only; it is not an entry gate.

Usage:
  OPTIONS_CONFIG_FILE=/opt/reyt/strategy/settings.ini \
  /opt/reyt/venv/bin/python \
  /opt/reyt/strategy/ReyT_strategy_engine_covered_call_only.py --watch

Optional settings.ini values (under [strategy] or [paper]):
  PAPER_CC_MIN_ANNUALIZED_RETURN_PCT = 50
  PAPER_CC_MAX_NET_CAPITAL_TOMAN = 1000000
  PAPER_CC_MIN_NET_CAPITAL_TOMAN = 200000
  REYT_BASE_ENGINE = /opt/reyt/strategy/ReyT_strategy_engine_unified_1b_execution_status_v2.py

The schema is deliberately NOT changed. Legacy table/column names such as
risk_budget_rial remain for compatibility; in this mode the 1M cap represented
there is the Covered Call net-capital cap.
"""
from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path
from decimal import Decimal
from typing import Any, Dict, List, Mapping, Optional

HERE = Path(__file__).resolve().parent


def _resolve_base_engine() -> Path:
    env = os.getenv("REYT_BASE_ENGINE", "").strip()
    candidates = []
    if env:
        candidates.append(Path(env))
    candidates.extend(
        [
            HERE / "ReyT_strategy_engine_unified_1b_execution_status_v2.py",
            HERE / "ReyT_strategy_engine_unified_1b_execution_status_v2(1).py",
        ]
    )
    for p in candidates:
        if p.is_file() and p.resolve() != Path(__file__).resolve():
            return p.resolve()
    raise RuntimeError(
        "Base ReyT engine not found. Set REYT_BASE_ENGINE to the production "
        "ReyT_strategy_engine_unified_1b_execution_status_v2.py path."
    )


BASE_PATH = _resolve_base_engine()
_spec = importlib.util.spec_from_file_location("reyt_base_engine_cc_only", BASE_PATH)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"Cannot load base engine: {BASE_PATH}")
base = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = base
_spec.loader.exec_module(base)


# ---------------------------------------------------------------------------
# Temporary policy settings
# ---------------------------------------------------------------------------

MIN_ANNUALIZED_RETURN_PCT = base._decimal_setting(
    "PAPER_CC_MIN_ANNUALIZED_RETURN_PCT", "50"
)
MAX_NET_CAPITAL_TOMAN = base._decimal_setting(
    "PAPER_CC_MAX_NET_CAPITAL_TOMAN", "1000000"
)
MIN_NET_CAPITAL_TOMAN = base._decimal_setting(
    "PAPER_CC_MIN_NET_CAPITAL_TOMAN", "200000"
)
MIN_ITM_PCT = base._decimal_setting(
    "PAPER_CC_MIN_ITM_PCT", "10"
)
if MIN_ITM_PCT < 0 or MIN_ITM_PCT >= 100:
    raise RuntimeError("PAPER_CC_MIN_ITM_PCT must be between 0 and 100.")
def _normalize_symbol(value: Any) -> str:
    return (
        str(value or "")
        .strip()
        .replace("\u200c", "")
        .replace("ي", "ی")
        .replace("ك", "ک")
    )


PAPER_ALLOWED_UNDERLYINGS = {
    _normalize_symbol(x)
    for x in base._setting(
        "PAPER_ALLOWED_UNDERLYINGS",
        "اهرم,وبملت,شپنا,فملی,شستا",
    ).split(",")
    if _normalize_symbol(x)
}
MAX_NET_CAPITAL_RIAL = MAX_NET_CAPITAL_TOMAN * base.TOMAN_TO_RIAL
MIN_NET_CAPITAL_RIAL = MIN_NET_CAPITAL_TOMAN * base.TOMAN_TO_RIAL

if MIN_ANNUALIZED_RETURN_PCT < 0:
    raise RuntimeError("PAPER_CC_MIN_ANNUALIZED_RETURN_PCT cannot be negative.")
if MIN_NET_CAPITAL_TOMAN <= 0:
    raise RuntimeError("PAPER_CC_MIN_NET_CAPITAL_TOMAN must be positive.")
if MAX_NET_CAPITAL_TOMAN < MIN_NET_CAPITAL_TOMAN:
    raise RuntimeError("PAPER_CC_MAX_NET_CAPITAL_TOMAN must be >= minimum.")
if MAX_NET_CAPITAL_RIAL > base.INITIAL_CAPITAL_RIAL:
    raise RuntimeError("Covered Call max net capital cannot exceed initial account capital.")

DISABLED_STRATEGIES = {
    "PROTECTIVE_PUT",
    "BULL_CALL_SPREAD",
    "BEAR_PUT_SPREAD",
    "LONG_STRADDLE",
}


def _opt_dec(value: Any) -> Optional[Decimal]:
    if value is None:
        return None
    try:
        x = Decimal(str(value))
        return x if x.is_finite() else None
    except Exception:
        return None

# Keep legacy names aligned with the new cap so existing DB columns/reports stay
# compatible. Covered Call unit_loss == unit_capital in the base payoff model,
# but sizing below explicitly uses unit_capital.
base.FIXED_RISK_PER_TRADE_TOMAN = MAX_NET_CAPITAL_TOMAN
base.FIXED_RISK_PER_TRADE_RIAL = MAX_NET_CAPITAL_RIAL
base.INITIAL_RISK_PCT = (
    MAX_NET_CAPITAL_RIAL / base.INITIAL_CAPITAL_RIAL * Decimal("100")
)


def _annualized_return_pct_from_metrics(metrics: Any, dte: int) -> Decimal:
    ret_to_expiry_pct = base.dec(metrics.extras.get("return_to_expiry_pct"))
    return ret_to_expiry_pct * Decimal("365") / Decimal(max(1, int(dte)))


# ---------------------------------------------------------------------------
# 1) Build: retain Covered Call only.
# ---------------------------------------------------------------------------

_original_build_candidates = base.PaperEngine._build_candidates


def _cc_only_build_candidates(
    self: Any,
    account: Any,
    quotes: Any,
    books: Mapping[str, Any],
    hist_vol: Mapping[str, Any],
    revision: str,
    now: Any,
) -> Dict[str, List[Any]]:
    out = _original_build_candidates(
        self, account, quotes, books, hist_vol, revision, now
    )
    for strategy in DISABLED_STRATEGIES:
        out[strategy] = []
    return out


base.PaperEngine._build_candidates = _cc_only_build_candidates


# ---------------------------------------------------------------------------
# 2) Signal hurdle: replace old history/IV 40% gate with explicit CC 50% EAR.
# ---------------------------------------------------------------------------

def _cc_only_apply_expected_return_hurdle(
    self: Any, candidates: Dict[str, List[Any]]
) -> None:
    for strategy in DISABLED_STRATEGIES:
        candidates[strategy] = []

    for c in candidates.get("COVERED_CALL", []):
        if c.final_signal not in {"CANDIDATE", "STRONG_CANDIDATE"}:
            c.details["covered_call_annualized_filter"] = "STRATEGY_INVALID"
            continue

        annual = _opt_dec(c.details.get("annualized_return_pct"))
        if annual is None:
            c.final_signal = "REJECT"
            c.details["covered_call_annualized_filter"] = "NO_ANNUALIZED_RETURN"
            c.reason += " | Annualized-return filter: unavailable."
            continue

        c.details["covered_call_min_annualized_return_pct"] = (
            MIN_ANNUALIZED_RETURN_PCT
        )
        c.details["covered_call_min_itm_pct"] = MIN_ITM_PCT
        c.details["covered_call_min_net_capital_toman"] = MIN_NET_CAPITAL_TOMAN
        c.details["covered_call_max_net_capital_toman"] = MAX_NET_CAPITAL_TOMAN
        if annual < MIN_ANNUALIZED_RETURN_PCT:
            c.final_signal = "REJECT"
            c.details["covered_call_annualized_filter"] = "FAIL"
            c.reason += (
                f" | Annualized return {annual}% < "
                f"{MIN_ANNUALIZED_RETURN_PCT}% minimum."
            )
        else:
            c.details["covered_call_annualized_filter"] = "PASS"


base.PaperEngine._apply_expected_return_hurdle = (
    _cc_only_apply_expected_return_hurdle
)


# ---------------------------------------------------------------------------
# 3) Persist: write ONLY covered_call_signals. Disabled tables are untouched.
# ---------------------------------------------------------------------------

async def _cc_only_upsert_signals(
    self: Any,
    db: Any,
    account: Any,
    candidates: Dict[str, List[Any]],
) -> int:
    strategy = "COVERED_CALL"
    items = candidates.get(strategy, [])
    table = base.qname(base.STRATEGY_TABLES[strategy])

    await db.execute(
        f"UPDATE {table} SET is_current=0 WHERE account_id=%s AND is_current=1",
        (account.account_id,),
    )

    common_cols = self._common_signal_columns()
    extra_cols = self._strategy_extra_columns(strategy)
    cols = common_cols + list(extra_cols) + ["details_json", "is_current"]

    if items:
        placeholders = ",".join(["%s"] * len(cols))
        update_cols = [
            x
            for x in cols
            if x
            not in {
                "signal_key",
                "signal_date",
                "account_id",
                "strategy_code",
                "is_current",
            }
        ]
        update_parts = []
        for x in update_cols:
            if x == "details_json":
                update_parts.append(
                    "details_json=IF(opened_position_id IS NULL,"
                    "VALUES(details_json),details_json)"
                )
            else:
                update_parts.append(f"{x}=VALUES({x})")
        update_sql = ",".join(update_parts)
        sql = f"""
            INSERT INTO {table} ({','.join(cols)})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE
              {update_sql},is_current=1,updated_at=CURRENT_TIMESTAMP
        """
        rows = [
            self._signal_row(account.account_id, c, extra_cols)
            for c in items
        ]
        await db.executemany(sql, rows)

    id_rows = await db.fetch(
        f"""
        SELECT signal_id,signal_key,opened_position_id
        FROM {table}
        WHERE account_id=%s AND is_current=1
        """,
        (account.account_id,),
    )
    idmap = {
        str(r["signal_key"]): (
            int(r["signal_id"]),
            int(r["opened_position_id"])
            if r["opened_position_id"] is not None
            else None,
        )
        for r in id_rows
    }
    for c in items:
        if c.key in idmap:
            c.signal_id, c.opened_position_id = idmap[c.key]

    return len(items)


base.PaperEngine._upsert_signals = _cc_only_upsert_signals


# ---------------------------------------------------------------------------
# 4) Explicit rejection reason for the 50% gate.
# ---------------------------------------------------------------------------

def _cc_only_non_entry_reason(c: Any):
    if c.strategy != "COVERED_CALL":
        return (
            "STRATEGY_TEMPORARILY_DISABLED",
            f"{c.strategy} is temporarily disabled; Covered Call only.",
        )
    annual = _opt_dec(c.details.get("annualized_return_pct"))
    if annual is None:
        return (
            "ANNUALIZED_RETURN_UNAVAILABLE",
            "Covered Call annualized return is unavailable.",
        )
    if annual < MIN_ANNUALIZED_RETURN_PCT:
        return (
            "ANNUALIZED_RETURN_BELOW_50PCT",
            f"Annualized return {annual}% is below "
            f"{MIN_ANNUALIZED_RETURN_PCT}% minimum.",
        )
    return (
        "STRATEGY_FILTER_REJECTED",
        "Covered Call structural validity filters rejected this candidate.",
    )


base.PaperEngine._non_entry_reason = staticmethod(_cc_only_non_entry_reason)


# ---------------------------------------------------------------------------
# 5) Execution plan: size by NET CAPITAL [200K, 1M], then re-check 50% at VWAP.
# ---------------------------------------------------------------------------

def _cc_only_plan_execution(
    self: Any,
    c: Any,
    books: Mapping[str, Any],
    available_cash: Decimal,
):
    if c.strategy != "COVERED_CALL":
        return base.ExecutionDecision(
            None,
            "STRATEGY_TEMPORARILY_DISABLED",
            f"{c.strategy} is temporarily disabled.",
        )

    if c.unit_capital <= base.D0:
        return base.ExecutionDecision(
            None,
            "INVALID_NET_CAPITAL",
            "Covered Call unit net capital is not positive.",
        )
    if c.unit_capital > MAX_NET_CAPITAL_RIAL:
        return base.ExecutionDecision(
            None,
            "ONE_UNIT_EXCEEDS_MAX_NET_CAPITAL",
            "One whole Covered Call unit exceeds the 1M toman net-capital cap.",
        )

    for leg in c.legs:
        book = books.get(leg.ins_code)
        if book is None:
            return base.ExecutionDecision(
                None,
                "ORDER_BOOK_MISSING",
                f"No order book is available for {leg.symbol}.",
            )
        if not book.is_fresh(c.scan_time, base.MAX_ENTRY_BOOK_AGE_SECONDS):
            return base.ExecutionDecision(
                None,
                "STALE_ORDER_BOOK",
                f"Order book for {leg.symbol} is too old for entry.",
            )
        if book.capacity_units(leg) <= 0:
            return base.ExecutionDecision(
                None,
                "INSUFFICIENT_ORDER_BOOK_DEPTH",
                f"No executable depth is available for {leg.symbol}.",
            )

    live_exec = base.executable_units_for_legs(c.legs, books, c.scan_time)
    if live_exec <= 0:
        return base.ExecutionDecision(
            None,
            "INSUFFICIENT_ORDER_BOOK_DEPTH",
            "Combined leg depth cannot execute one Covered Call unit.",
        )

    capital_units = base.floor_int(MAX_NET_CAPITAL_RIAL / c.unit_capital)
    if capital_units <= 0:
        return base.ExecutionDecision(
            None,
            "MAX_NET_CAPITAL_EXCEEDED",
            "1M toman net-capital cap cannot fund one whole unit.",
        )

    cash_units = live_exec
    cash_units = min(
        cash_units, base.floor_int(available_cash / c.unit_capital)
    )
    # Covered Call buys stock before crediting option premium; preserve this
    # stricter gross-funding check from the production engine.
    if c.pretrade_cash > base.D0:
        cash_units = min(
            cash_units, base.floor_int(available_cash / c.pretrade_cash)
        )
    if cash_units <= 0:
        return base.ExecutionDecision(
            None,
            "INSUFFICIENT_CASH",
            f"Available cash "
            f"{base.money(available_cash/base.TOMAN_TO_RIAL):,.0f} toman "
            "cannot fund one Covered Call unit.",
        )

    target = min(live_exec, capital_units, cash_units)
    if target <= 0:
        return base.ExecutionDecision(
            None,
            "NO_EXECUTABLE_UNITS",
            "Capital, cash, and order-book constraints leave zero units.",
        )

    plan = None
    for _ in range(6):
        prices = []
        for leg in c.legs:
            book = books.get(leg.ins_code)
            if book is None or not book.is_fresh(
                c.scan_time, base.MAX_ENTRY_BOOK_AGE_SECONDS
            ):
                return base.ExecutionDecision(
                    None,
                    "STALE_ORDER_BOOK",
                    f"Fresh depth disappeared for {leg.symbol}.",
                )
            px = book.quote_vwap(leg, target)
            if px is None or px <= base.D0:
                return base.ExecutionDecision(
                    None,
                    "INSUFFICIENT_ORDER_BOOK_DEPTH",
                    f"Five-level depth cannot fill {target} unit(s) "
                    f"for {leg.symbol}.",
                )
            prices.append(px)

        metrics = base.strategy_metrics(
            "COVERED_CALL", c.spot, c.legs, prices
        )
        if metrics.unit_capital <= base.D0:
            return base.ExecutionDecision(
                None,
                "INVALID_EXECUTION_NET_CAPITAL",
                "VWAP execution produced invalid Covered Call net capital.",
            )

        new_target = live_exec
        new_target = min(
            new_target,
            base.floor_int(MAX_NET_CAPITAL_RIAL / metrics.unit_capital),
        )
        new_target = min(
            new_target,
            base.floor_int(available_cash / metrics.unit_capital),
        )
        if metrics.pretrade_cash > base.D0:
            new_target = min(
                new_target,
                base.floor_int(available_cash / metrics.pretrade_cash),
            )

        if new_target <= 0:
            if base.floor_int(
                MAX_NET_CAPITAL_RIAL / metrics.unit_capital
            ) <= 0:
                return base.ExecutionDecision(
                    None,
                    "VWAP_MAX_NET_CAPITAL_EXCEEDED",
                    "VWAP/slippage pushes one whole unit above the 1M "
                    "toman net-capital cap.",
                )
            return base.ExecutionDecision(
                None,
                "INSUFFICIENT_CASH",
                "VWAP execution cost exceeds available paper-account cash.",
            )

        plan = base.ExecutionPlan(
            new_target, tuple(prices), metrics
        )
        if new_target == target:
            break
        target = new_target

    if plan is None:
        return base.ExecutionDecision(
            None,
            "EXECUTION_PLAN_FAILED",
            "No stable Covered Call execution plan could be produced.",
        )

    # Final quote at the final integer size.
    final_prices = []
    for leg in c.legs:
        book = books.get(leg.ins_code)
        if book is None:
            return base.ExecutionDecision(
                None,
                "ORDER_BOOK_MISSING",
                f"Order book missing for {leg.symbol} at final quote.",
            )
        px = book.quote_vwap(leg, plan.units)
        if px is None:
            return base.ExecutionDecision(
                None,
                "INSUFFICIENT_ORDER_BOOK_DEPTH",
                f"Final depth cannot fill {plan.units} unit(s) "
                f"for {leg.symbol}.",
            )
        final_prices.append(px)

    final_metrics = base.strategy_metrics(
        "COVERED_CALL", c.spot, c.legs, final_prices
    )
    final_net_capital = (
        final_metrics.unit_capital * Decimal(plan.units)
    )

    if final_net_capital > MAX_NET_CAPITAL_RIAL + Decimal("0.01"):
        return base.ExecutionDecision(
            None,
            "MAX_NET_CAPITAL_EXCEEDED",
            f"Final net capital "
            f"{base.money(final_net_capital/base.TOMAN_TO_RIAL):,.0f} toman "
            "exceeds the 1M toman cap.",
        )

    if final_net_capital < MIN_NET_CAPITAL_RIAL - Decimal("0.01"):
        return base.ExecutionDecision(
            None,
            "MIN_NET_CAPITAL_NOT_MET",
            f"Final net capital "
            f"{base.money(final_net_capital/base.TOMAN_TO_RIAL):,.0f} toman "
            "is below the 200K toman minimum.",
        )

    if final_net_capital > available_cash + Decimal("0.01"):
        return base.ExecutionDecision(
            None,
            "INSUFFICIENT_CASH",
            "Final net capital exceeds available paper-account cash.",
        )

    if (
        final_metrics.pretrade_cash * Decimal(plan.units)
        > available_cash + Decimal("0.01")
    ):
        return base.ExecutionDecision(
            None,
            "INSUFFICIENT_CASH",
            "Gross stock purchase before option-premium credit exceeds "
            "available paper-account cash.",
        )

    execution_annual = _annualized_return_pct_from_metrics(
        final_metrics, c.days_to_expiry
    )
    c.details.update(
        {
            "execution_annualized_return_pct": base.pct(execution_annual),
            "covered_call_min_annualized_return_pct": (
                MIN_ANNUALIZED_RETURN_PCT
            ),
            "execution_net_capital_rial": base.money(final_net_capital),
            "execution_net_capital_toman": base.money(
                final_net_capital / base.TOMAN_TO_RIAL
            ),
        }
    )
    if execution_annual < MIN_ANNUALIZED_RETURN_PCT:
        return base.ExecutionDecision(
            None,
            "VWAP_ANNUALIZED_RETURN_BELOW_50PCT",
            f"Final VWAP annualized return {base.pct(execution_annual)}% "
            f"is below {MIN_ANNUALIZED_RETURN_PCT}% minimum.",
        )

    return base.ExecutionDecision(
        base.ExecutionPlan(
            plan.units, tuple(final_prices), final_metrics
        ),
        "EXECUTABLE",
        f"Executable for {plan.units} unit(s); net capital "
        f"{base.money(final_net_capital/base.TOMAN_TO_RIAL):,.0f} toman; "
        f"annualized return {base.pct(execution_annual)}%.",
    )


base.PaperEngine._plan_execution = _cc_only_plan_execution


# ---------------------------------------------------------------------------
# 6) Auto-open: Covered Call only; score is NOT a gate.
# ---------------------------------------------------------------------------

async def _cc_only_auto_open(
    self: Any,
    db: Any,
    account: Any,
    candidates: Dict[str, List[Any]],
    books: Mapping[str, Any],
    now: Any,
    enabled: bool = True,
) -> List[int]:
    all_candidates = candidates.get("COVERED_CALL", [])

    if not base.AUTO_TRADE or not enabled:
        for c in all_candidates:
            if c.opened_position_id is not None:
                # Preserve immutable execution-time details already stored.
                continue
            else:
                await self._set_signal_execution_status(
                    db,
                    c,
                    "NOT_EXECUTED",
                    now,
                    "AUTO_TRADE_DISABLED",
                    "Automatic paper-account execution is disabled.",
                )
        return []

    open_signatures = await self._open_signatures(
        db, account.account_id
    )
    eligible = []

    for c in all_candidates:
        if c.opened_position_id is not None:
            # _open_position already persisted the exact entry-time details.
            # Never overwrite those immutable execution facts on later scans.
            continue

        if c.final_signal not in {
            "CANDIDATE",
            "STRONG_CANDIDATE",
        }:
            code, reason = self._non_entry_reason(c)
            await self._set_signal_execution_status(
                db, c, "NOT_EXECUTED", now, code, reason
            )
            continue
        if _normalize_symbol(c.underlying_symbol) not in PAPER_ALLOWED_UNDERLYINGS:
            await self._set_signal_execution_status(
                db,
                c,
                "NOT_EXECUTED",
                now,
                "PAPER_UNDERLYING_NOT_ALLOWED",
                (
                    f"Underlying {c.underlying_symbol} is outside the allowed "
                    "paper-account universe."
                ),
            )
            continue
        # No score threshold here. 50% annualized is the economic gate.
        short_call = next(
            (
                leg for leg in c.legs
                if leg.kind == "OPTION"
                and leg.side == "SHORT"
                and leg.option_type == "CALL"
            ),
            None,
        )

        max_allowed_strike = (
            c.spot * (Decimal("1") - MIN_ITM_PCT / Decimal("100"))
        )

        if (
            short_call is None
            or short_call.strike is None
            or short_call.strike > max_allowed_strike
        ):
            await self._set_signal_execution_status(
                db,
                c,
                "NOT_EXECUTED",
                now,
                "CC_MIN_ITM_PCT_NOT_MET",
                (
                    f"Covered Call strike must be at least "
                    f"{MIN_ITM_PCT}% below spot for paper execution."
                ),
            )
            continue
        eligible.append(c)

    # Rank only after all hard gates. Prefer higher annualized return, then
    # liquidity and score as tie-break/context.
    def _priority(c: Any):
        annual = _opt_dec(
            c.details.get("annualized_return_pct")
        ) or Decimal("-1")
        return (annual, c.liquidity, c.score)

    eligible.sort(key=_priority, reverse=True)

    execution_books = self._execution_books(books)
    available_cash = account.available_cash
    opened = []

    for c in eligible:
        if c.signature in open_signatures:
            await self._set_signal_execution_status(
                db,
                c,
                "NOT_EXECUTED",
                now,
                "DUPLICATE_OPEN_SIGNATURE",
                "The exact same Covered Call structure already has an "
                "OPEN paper position.",
            )
            continue

        decision = self._plan_execution(
            c, execution_books, available_cash
        )
        if decision.plan is None or decision.plan.units <= 0:
            await self._set_signal_execution_status(
                db,
                c,
                "NOT_EXECUTED",
                now,
                decision.reason_code,
                decision.reason,
            )
            continue
        plan = decision.plan

        can_fill = True
        for leg in c.legs:
            book = execution_books.get(leg.ins_code)
            if (
                book is None
                or book.capacity_units(leg) < plan.units
            ):
                can_fill = False
                break
        if not can_fill:
            await self._set_signal_execution_status(
                db,
                c,
                "NOT_EXECUTED",
                now,
                "INSUFFICIENT_ORDER_BOOK_DEPTH",
                "Displayed five-level depth was consumed by a "
                "higher-priority Covered Call in the same scan.",
            )
            continue

        pid = await self._open_position(
            db, account, c, plan, now
        )

        for leg in c.legs:
            ok = execution_books[leg.ins_code].consume(
                leg, plan.units
            )
            if not ok:
                raise RuntimeError(
                    "In-memory order-book consumption mismatch."
                )

        available_cash -= (
            plan.metrics.unit_capital * Decimal(plan.units)
        )
        open_signatures.add(c.signature)
        c.opened_position_id = pid
        opened.append(pid)

        total_cap = (
            plan.metrics.unit_capital * Decimal(plan.units)
        )
        annual = _annualized_return_pct_from_metrics(
            plan.metrics, c.days_to_expiry
        )
        print(
            f"[{now:%H:%M:%S}] 🧪 Paper OPEN #{pid} | COVERED_CALL | "
            f"{c.underlying_symbol} | units={plan.units} | "
            f"net_capital="
            f"{base.money(total_cap/base.TOMAN_TO_RIAL):,.0f} toman | "
            f"annualized={base.pct(annual)}%"
        )

    return opened


base.PaperEngine._auto_open = _cc_only_auto_open


# ---------------------------------------------------------------------------
# Startup banner to make the temporary mode unmistakable in journalctl.
# ---------------------------------------------------------------------------

_original_open = base.PaperEngine.open


async def _cc_only_open(self: Any) -> None:
    await _original_open(self)
    print(
        "   🟦 TEMP POLICY: COVERED_CALL ONLY | "
        f"annualized >= {MIN_ANNUALIZED_RETURN_PCT}% | "
        f"min ITM depth >= {MIN_ITM_PCT}% | "
        f"net capital {MIN_NET_CAPITAL_TOMAN:,.0f}.."
        f"{MAX_NET_CAPITAL_TOMAN:,.0f} toman"
    )
    print(
        "   Disabled/no-save: PROTECTIVE_PUT, BULL_CALL_SPREAD, "
        "BEAR_PUT_SPREAD, LONG_STRADDLE"
    )


base.PaperEngine.open = _cc_only_open


if __name__ == "__main__":
    base.main()
