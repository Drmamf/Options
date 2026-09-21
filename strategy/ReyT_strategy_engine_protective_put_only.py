# -*- coding: utf-8 -*-
"""
ReyT Protective-Put-only paper-trading policy launcher.

This launcher keeps the production unified engine as the shared implementation,
but isolates Protective Put into its own paper account and applies a policy that
mirrors the current Covered Call paper mode as closely as the payoff permits:

  * PROTECTIVE_PUT is the only generated/persisted/executed strategy.
  * Put strike must be at least 10% ITM: K >= S * 1.10.
  * DTE uses the same configured 5..180 day window as Covered Call.
  * History AND IV expected return must each beat a 50% effective-annual hurdle.
  * The expected-return hurdle is checked at signal quote and again after final
    five-level order-book VWAP.
  * Position sizing is based on TOTAL Protective Put capital:
        stock buy cash cost (including buy fee)
      + put buy cash cost (including option-buy fee)
  * Maximum final position capital: 1,000,000 toman.
  * Minimum final position capital:   200,000 toman.
  * Integer contract granularity is preserved.
  * The same allowed-underlying universe and order-book freshness/depth rules as
    the Covered Call paper mode are used.
  * Score remains ranking/context only; it is not an entry gate.

The schema is not changed. Run this launcher in a separate process/account from
Covered Call by overriding PAPER_ACCOUNT_NAME in systemd.
"""

from __future__ import annotations

import importlib.util
import math
import os
import sys
from decimal import Decimal
from pathlib import Path
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
_spec = importlib.util.spec_from_file_location("reyt_base_engine_pp_only", BASE_PATH)
if _spec is None or _spec.loader is None:
    raise RuntimeError(f"Cannot load base engine: {BASE_PATH}")
base = importlib.util.module_from_spec(_spec)
sys.modules[_spec.name] = base
_spec.loader.exec_module(base)


# ---------------------------------------------------------------------------
# Protective Put policy settings
# ---------------------------------------------------------------------------

MIN_EXPECTED_ANNUALIZED_RETURN_PCT = base._decimal_setting(
    "PAPER_PP_MIN_EXPECTED_ANNUALIZED_RETURN_PCT", "50"
)
MAX_CAPITAL_TOMAN = base._decimal_setting(
    "PAPER_PP_MAX_CAPITAL_TOMAN", "1000000"
)
MIN_CAPITAL_TOMAN = base._decimal_setting(
    "PAPER_PP_MIN_CAPITAL_TOMAN", "200000"
)
MIN_ITM_PCT = base._decimal_setting(
    "PAPER_PP_MIN_ITM_PCT", "10"
)


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

MAX_CAPITAL_RIAL = MAX_CAPITAL_TOMAN * base.TOMAN_TO_RIAL
MIN_CAPITAL_RIAL = MIN_CAPITAL_TOMAN * base.TOMAN_TO_RIAL

if MIN_EXPECTED_ANNUALIZED_RETURN_PCT < 0:
    raise RuntimeError(
        "PAPER_PP_MIN_EXPECTED_ANNUALIZED_RETURN_PCT cannot be negative."
    )
if MIN_ITM_PCT < 0:
    raise RuntimeError("PAPER_PP_MIN_ITM_PCT cannot be negative.")
if MIN_CAPITAL_TOMAN <= 0:
    raise RuntimeError("PAPER_PP_MIN_CAPITAL_TOMAN must be positive.")
if MAX_CAPITAL_TOMAN < MIN_CAPITAL_TOMAN:
    raise RuntimeError(
        "PAPER_PP_MAX_CAPITAL_TOMAN must be >= PAPER_PP_MIN_CAPITAL_TOMAN."
    )
if MAX_CAPITAL_RIAL > base.INITIAL_CAPITAL_RIAL:
    raise RuntimeError(
        "Protective Put max capital cannot exceed initial account capital."
    )

# Make the base expected-return model use the PP-specific 50% effective-annual
# hurdle. Both History and IV must pass, matching the policy agreed for PP.
base.EXPECTED_RETURN_HURDLE_EAR = (
    MIN_EXPECTED_ANNUALIZED_RETURN_PCT / Decimal("100")
)
base.EXPECTED_RETURN_REQUIRE_BOTH = True

DISABLED_STRATEGIES = {
    "COVERED_CALL",
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


def _annualize_expected_return_pct(
    return_to_expiry_pct: Any,
    dte: int,
) -> Optional[Decimal]:
    value = _opt_dec(return_to_expiry_pct)
    if value is None:
        return None
    r = float(value) / 100.0
    if r <= -1.0:
        return None
    days = max(1, int(dte))
    annual = (math.pow(1.0 + r, 365.0 / days) - 1.0) * 100.0
    if not math.isfinite(annual):
        return None
    return base.pct(Decimal(str(annual)))


def _put_leg(candidate: Any) -> Optional[Any]:
    return next(
        (
            leg
            for leg in candidate.legs
            if leg.kind == "OPTION"
            and leg.side == "LONG"
            and leg.option_type == "PUT"
        ),
        None,
    )


def _itm_depth_pct(candidate: Any) -> Optional[Decimal]:
    leg = _put_leg(candidate)
    if (
        leg is None
        or leg.strike is None
        or candidate.spot is None
        or candidate.spot <= base.D0
    ):
        return None
    return base.pct(
        base.safe_div(base.dec(leg.strike) - candidate.spot, candidate.spot)
        * base.D100
    )


# ---------------------------------------------------------------------------
# 1) Build: keep PP only and replace legacy strike-validity with >=10% ITM.
# ---------------------------------------------------------------------------

_original_build_candidates = base.PaperEngine._build_candidates


def _pp_only_build_candidates(
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

    items = out.get("PROTECTIVE_PUT", [])
    for c in items:
        itm = _itm_depth_pct(c)
        c.details["protective_put_itm_depth_pct"] = itm
        c.details["protective_put_min_itm_pct"] = MIN_ITM_PCT
        c.details["protective_put_min_capital_toman"] = MIN_CAPITAL_TOMAN
        c.details["protective_put_max_capital_toman"] = MAX_CAPITAL_TOMAN
        c.details["protective_put_min_expected_annualized_return_pct"] = (
            MIN_EXPECTED_ANNUALIZED_RETURN_PCT
        )

        leg = _put_leg(c)
        strike_ok = (
            leg is not None
            and leg.strike is not None
            and c.spot > base.D0
            and base.dec(leg.strike)
            >= c.spot * (
                Decimal("1") + MIN_ITM_PCT / Decimal("100")
            )
        )
        structural_ok = (
            strike_ok
            and c.unit_capital > base.D0
            and c.unit_loss > base.D0
            and c.days_to_expiry >= base.MIN_DTE
            and c.days_to_expiry <= base.MAX_DTE
        )

        if structural_ok:
            c.final_signal = (
                "STRONG_CANDIDATE"
                if c.score >= Decimal("75")
                else "CANDIDATE"
            )
        else:
            c.final_signal = "REJECT"
            if not strike_ok:
                c.reason += (
                    f" | REJECT: Protective Put strike must be at least "
                    f"{MIN_ITM_PCT}% above spot."
                )

        # Recommended size is capital-based for this paper policy, not based on
        # generic max-loss sizing from the unified engine.
        executable = base.executable_units_for_legs(c.legs, books, now)
        capital_units = (
            base.floor_int(MAX_CAPITAL_RIAL / c.unit_capital)
            if c.unit_capital > base.D0
            else 0
        )
        cash_units = (
            base.floor_int(account.available_cash / c.unit_capital)
            if c.unit_capital > base.D0
            else 0
        )
        recommended = max(0, min(executable, capital_units, cash_units))
        c.recommended_units = recommended
        c.recommended_capital = base.money(
            c.unit_capital * Decimal(recommended)
        )
        c.recommended_risk = base.money(
            c.unit_loss * Decimal(recommended)
        )

    items.sort(key=lambda c: (c.priority_tuple, c.key), reverse=True)
    return out


base.PaperEngine._build_candidates = _pp_only_build_candidates


# ---------------------------------------------------------------------------
# 2) Expected return: History AND IV > 50% effective annual.
# ---------------------------------------------------------------------------

_original_apply_expected_return_hurdle = (
    base.PaperEngine._apply_expected_return_hurdle
)


def _pp_only_apply_expected_return_hurdle(
    self: Any,
    candidates: Dict[str, List[Any]],
) -> None:
    for strategy in DISABLED_STRATEGIES:
        candidates[strategy] = []

    _original_apply_expected_return_hurdle(self, candidates)

    for c in candidates.get("PROTECTIVE_PUT", []):
        c.details["protective_put_min_expected_annualized_return_pct"] = (
            MIN_EXPECTED_ANNUALIZED_RETURN_PCT
        )
        c.details["history_expected_annualized_return_pct"] = (
            _annualize_expected_return_pct(
                c.details.get("history_expected_return_to_expiry_pct"),
                c.days_to_expiry,
            )
        )
        c.details["iv_expected_annualized_return_pct"] = (
            _annualize_expected_return_pct(
                c.details.get("iv_expected_return_to_expiry_pct"),
                c.days_to_expiry,
            )
        )


base.PaperEngine._apply_expected_return_hurdle = (
    _pp_only_apply_expected_return_hurdle
)


# ---------------------------------------------------------------------------
# 3) Persist: write ONLY protective_put_signals.
# ---------------------------------------------------------------------------

async def _pp_only_upsert_signals(
    self: Any,
    db: Any,
    account: Any,
    candidates: Dict[str, List[Any]],
) -> int:
    strategy = "PROTECTIVE_PUT"
    items = candidates.get(strategy, [])
    table = base.qname(base.STRATEGY_TABLES[strategy])

    await db.execute(
        f"UPDATE {table} SET is_current=0 "
        "WHERE account_id=%s AND is_current=1",
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
        sql = f"""
            INSERT INTO {table} ({','.join(cols)})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE
              {','.join(update_parts)},
              is_current=1,
              updated_at=CURRENT_TIMESTAMP
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


base.PaperEngine._upsert_signals = _pp_only_upsert_signals


# ---------------------------------------------------------------------------
# 4) Protective-Put-specific non-entry reasons.
# ---------------------------------------------------------------------------

def _pp_only_non_entry_reason(c: Any):
    if c.strategy != "PROTECTIVE_PUT":
        return (
            "STRATEGY_TEMPORARILY_DISABLED",
            f"{c.strategy} is disabled in Protective-Put-only mode.",
        )

    itm = _itm_depth_pct(c)
    if itm is None or itm < MIN_ITM_PCT:
        return (
            "PP_MIN_ITM_PCT_NOT_MET",
            f"Protective Put strike must be at least "
            f"{MIN_ITM_PCT}% above spot.",
        )

    state = str(c.details.get("expected_return_filter") or "")
    if state == "NO_USABLE_HISTORY":
        return (
            "PP_EXPECTED_RETURN_NO_HISTORY",
            "No usable retained history for Protective Put expected return.",
        )
    if state == "NO_USABLE_IV":
        return (
            "PP_EXPECTED_RETURN_NO_IV",
            "No usable IV for Protective Put expected return.",
        )
    if state == "FAIL":
        return (
            "PP_EXPECTED_RETURN_BELOW_50PCT",
            f"History and IV expected return did not both beat "
            f"{MIN_EXPECTED_ANNUALIZED_RETURN_PCT}% effective annual.",
        )

    return (
        "STRATEGY_FILTER_REJECTED",
        "Protective Put structural filters rejected this candidate.",
    )


base.PaperEngine._non_entry_reason = staticmethod(_pp_only_non_entry_reason)


# ---------------------------------------------------------------------------
# 5) Execution: size by total capital [200K, 1M], then re-check 50% at VWAP.
# ---------------------------------------------------------------------------

def _pp_only_plan_execution(
    self: Any,
    c: Any,
    books: Mapping[str, Any],
    available_cash: Decimal,
):
    if c.strategy != "PROTECTIVE_PUT":
        return base.ExecutionDecision(
            None,
            "STRATEGY_TEMPORARILY_DISABLED",
            f"{c.strategy} is disabled.",
        )

    if c.unit_capital <= base.D0:
        return base.ExecutionDecision(
            None,
            "INVALID_POSITION_CAPITAL",
            "Protective Put unit capital is not positive.",
        )
    if c.unit_capital > MAX_CAPITAL_RIAL:
        return base.ExecutionDecision(
            None,
            "PP_ONE_UNIT_EXCEEDS_MAX_CAPITAL",
            "One whole Protective Put unit exceeds the 1M toman capital cap.",
        )

    for leg in c.legs:
        book = books.get(leg.ins_code)
        if book is None:
            return base.ExecutionDecision(
                None,
                "ORDER_BOOK_MISSING",
                f"No order book is available for {leg.symbol}.",
            )
        if not book.is_fresh(
            c.scan_time, base.MAX_ENTRY_BOOK_AGE_SECONDS
        ):
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

    live_exec = base.executable_units_for_legs(
        c.legs, books, c.scan_time
    )
    if live_exec <= 0:
        return base.ExecutionDecision(
            None,
            "INSUFFICIENT_ORDER_BOOK_DEPTH",
            "Combined leg depth cannot execute one Protective Put unit.",
        )

    capital_units = base.floor_int(
        MAX_CAPITAL_RIAL / c.unit_capital
    )
    cash_units = base.floor_int(
        available_cash / c.unit_capital
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
            "PROTECTIVE_PUT", c.spot, c.legs, prices
        )
        if metrics.unit_capital <= base.D0:
            return base.ExecutionDecision(
                None,
                "INVALID_EXECUTION_CAPITAL",
                "VWAP execution produced invalid Protective Put capital.",
            )

        new_target = live_exec
        new_target = min(
            new_target,
            base.floor_int(MAX_CAPITAL_RIAL / metrics.unit_capital),
            base.floor_int(available_cash / metrics.unit_capital),
        )

        if new_target <= 0:
            if base.floor_int(
                MAX_CAPITAL_RIAL / metrics.unit_capital
            ) <= 0:
                return base.ExecutionDecision(
                    None,
                    "PP_VWAP_MAX_CAPITAL_EXCEEDED",
                    "VWAP/slippage pushes one whole Protective Put unit "
                    "above the 1M toman capital cap.",
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
            "No stable Protective Put execution plan could be produced.",
        )

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
        "PROTECTIVE_PUT", c.spot, c.legs, final_prices
    )
    final_capital = (
        final_metrics.unit_capital * Decimal(plan.units)
    )

    if final_capital > MAX_CAPITAL_RIAL + Decimal("0.01"):
        return base.ExecutionDecision(
            None,
            "PP_MAX_CAPITAL_EXCEEDED",
            f"Final Protective Put capital "
            f"{base.money(final_capital/base.TOMAN_TO_RIAL):,.0f} toman "
            "exceeds the 1M toman cap.",
        )

    if final_capital < MIN_CAPITAL_RIAL - Decimal("0.01"):
        return base.ExecutionDecision(
            None,
            "PP_MIN_CAPITAL_NOT_MET",
            f"Final Protective Put capital "
            f"{base.money(final_capital/base.TOMAN_TO_RIAL):,.0f} toman "
            "is below the 200K toman minimum.",
        )

    if final_capital > available_cash + Decimal("0.01"):
        return base.ExecutionDecision(
            None,
            "INSUFFICIENT_CASH",
            "Final Protective Put capital exceeds available cash.",
        )

    history_exec, iv_exec, iv = self._expected_return_pair(
        c, final_metrics.unit_capital
    )
    if history_exec is None:
        return base.ExecutionDecision(
            None,
            "PP_VWAP_HISTORY_UNAVAILABLE",
            "History expected return could not be evaluated at final VWAP.",
        )
    if iv_exec is None or iv is None:
        return base.ExecutionDecision(
            None,
            "PP_VWAP_IV_UNAVAILABLE",
            "IV expected return could not be evaluated at final VWAP.",
        )

    hist_to_expiry_pct = history_exec["expected_return"] * 100.0
    iv_to_expiry_pct = iv_exec["expected_return"] * 100.0
    hist_annual = _annualize_expected_return_pct(
        hist_to_expiry_pct, c.days_to_expiry
    )
    iv_annual = _annualize_expected_return_pct(
        iv_to_expiry_pct, c.days_to_expiry
    )

    c.details.update(
        {
            "execution_history_expected_return_to_expiry_pct":
                hist_to_expiry_pct,
            "execution_iv_expected_return_to_expiry_pct":
                iv_to_expiry_pct,
            "execution_history_expected_annualized_return_pct":
                hist_annual,
            "execution_iv_expected_annualized_return_pct":
                iv_annual,
            "execution_effective_iv_pct": iv * 100.0,
            "protective_put_min_expected_annualized_return_pct":
                MIN_EXPECTED_ANNUALIZED_RETURN_PCT,
            "execution_position_capital_rial":
                base.money(final_capital),
            "execution_position_capital_toman":
                base.money(final_capital / base.TOMAN_TO_RIAL),
        }
    )

    if not (
        bool(history_exec["passes"])
        and bool(iv_exec["passes"])
    ):
        return base.ExecutionDecision(
            None,
            "PP_VWAP_EXPECTED_RETURN_BELOW_50PCT",
            f"Final VWAP expected return did not pass "
            f"{MIN_EXPECTED_ANNUALIZED_RETURN_PCT}% effective annual "
            "in both History and IV models.",
        )

    return base.ExecutionDecision(
        base.ExecutionPlan(
            plan.units, tuple(final_prices), final_metrics
        ),
        "EXECUTABLE",
        f"Executable for {plan.units} unit(s); position capital "
        f"{base.money(final_capital/base.TOMAN_TO_RIAL):,.0f} toman; "
        f"History annualized={hist_annual}%; IV annualized={iv_annual}%.",
    )


base.PaperEngine._plan_execution = _pp_only_plan_execution


# ---------------------------------------------------------------------------
# 6) Auto-open: Protective Put only; score is ranking/context only.
# ---------------------------------------------------------------------------

async def _pp_only_auto_open(
    self: Any,
    db: Any,
    account: Any,
    candidates: Dict[str, List[Any]],
    books: Mapping[str, Any],
    now: Any,
    enabled: bool = True,
) -> List[int]:
    all_candidates = candidates.get("PROTECTIVE_PUT", [])

    if not base.AUTO_TRADE or not enabled:
        for c in all_candidates:
            if c.opened_position_id is None:
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

        if (
            _normalize_symbol(c.underlying_symbol)
            not in PAPER_ALLOWED_UNDERLYINGS
        ):
            await self._set_signal_execution_status(
                db,
                c,
                "NOT_EXECUTED",
                now,
                "PAPER_UNDERLYING_NOT_ALLOWED",
                (
                    f"Underlying {c.underlying_symbol} is outside the "
                    "allowed paper-account universe."
                ),
            )
            continue

        itm = _itm_depth_pct(c)
        if itm is None or itm < MIN_ITM_PCT:
            await self._set_signal_execution_status(
                db,
                c,
                "NOT_EXECUTED",
                now,
                "PP_MIN_ITM_PCT_NOT_MET",
                (
                    f"Protective Put strike must be at least "
                    f"{MIN_ITM_PCT}% above spot."
                ),
            )
            continue

        eligible.append(c)

    # Prefer the strongest conservative expected return first, then liquidity
    # and score only as tie-break/context.
    def _priority(c: Any):
        h = _opt_dec(
            c.details.get("history_expected_annualized_return_pct")
        ) or Decimal("-1")
        i = _opt_dec(
            c.details.get("iv_expected_annualized_return_pct")
        ) or Decimal("-1")
        return (min(h, i), c.liquidity, c.score)

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
                "The exact same Protective Put structure already has "
                "an OPEN paper position.",
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
                "higher-priority Protective Put in the same scan.",
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

        opened_capital = (
            plan.metrics.unit_capital * Decimal(plan.units)
        )
        available_cash -= opened_capital
        open_signatures.add(c.signature)
        c.opened_position_id = pid
        opened.append(pid)

        h = c.details.get(
            "execution_history_expected_annualized_return_pct"
        )
        i = c.details.get(
            "execution_iv_expected_annualized_return_pct"
        )
        print(
            f"[{now:%H:%M:%S}] 🧪 Paper OPEN #{pid} | PROTECTIVE_PUT | "
            f"{c.underlying_symbol} | units={plan.units} | "
            f"capital="
            f"{base.money(opened_capital/base.TOMAN_TO_RIAL):,.0f} toman | "
            f"HistoryAnn={h}% | IVAnn={i}%"
        )

    return opened


base.PaperEngine._auto_open = _pp_only_auto_open


# ---------------------------------------------------------------------------
# Startup banner
# ---------------------------------------------------------------------------

_original_open = base.PaperEngine.open


async def _pp_only_open(self: Any) -> None:
    await _original_open(self)
    print(
        "   🛡️ POLICY: PROTECTIVE_PUT ONLY | "
        f"History+IV annualized >= "
        f"{MIN_EXPECTED_ANNUALIZED_RETURN_PCT}% | "
        f"min ITM depth >= {MIN_ITM_PCT}% | "
        f"capital {MIN_CAPITAL_TOMAN:,.0f}.."
        f"{MAX_CAPITAL_TOMAN:,.0f} toman | "
        f"DTE {base.MIN_DTE}..{base.MAX_DTE}"
    )
    print(
        "   Disabled/no-save: COVERED_CALL, BULL_CALL_SPREAD, "
        "BEAR_PUT_SPREAD, LONG_STRADDLE"
    )


base.PaperEngine.open = _pp_only_open


if __name__ == "__main__":
    base.main()
