-- ============================================================================
-- ReyT Options - REBUILT MySQL 8 schema v2 (signal execution tracking)
-- Database: ghazali1_ReyTOption
--
-- Clean architecture for the unified ReyT pipeline:
--   Phase 1  : market-watch / contract / underlying snapshots
--   Phase 2  : normalized 5-level order book
--   Phase 3  : daily history for volatility and settlement
--   Phase 4  : option Greeks + derived features
--   Phase 5+ : five option strategies
--   Runtime  : paper account, positions, legs, valuations, equity history
--   Notify   : per-signal paper-account execution state + unified signal export view
--
-- IMPORTANT
--   * This is a fresh schema definition, not a patch/migration script.
--   * It is NON-DESTRUCTIVE by default: no user table is dropped.
--   * CREATE TABLE IF NOT EXISTS will not modify an already-existing old table.
--     For a truly clean replacement, use an empty database/schema or migrate first.
--
-- Designed for MySQL 8.x. utf8mb4 is used everywhere.
-- ============================================================================

USE `ghazali1_ReyTOption`;

SET NAMES utf8mb4;
SET time_zone = '+03:30';
SET FOREIGN_KEY_CHECKS = 1;

-- ============================================================================
-- SECTION 1 - Core market data
-- ============================================================================

-- One current row per underlying asset.
CREATE TABLE IF NOT EXISTS `underlying_assets` (
    `ua_ins_code` VARCHAR(30) NOT NULL,
    `symbol` VARCHAR(100) NULL,
    `full_name` VARCHAR(255) NULL,
    `status_code` VARCHAR(20) NULL,
    `status_title` VARCHAR(100) NULL,
    `under_supervision` TINYINT(1) NOT NULL DEFAULT 0,
    `closing_price` DECIMAL(20,4) NULL,
    `last_trade_price` DECIMAL(20,4) NULL,
    `previous_closing_price` DECIMAL(20,4) NULL,
    `last_update` DATETIME NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`ua_ins_code`),
    KEY `ix_underlying_symbol` (`symbol`),
    KEY `ix_underlying_updated_at` (`updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Static / slowly-changing contract metadata.
CREATE TABLE IF NOT EXISTS `option_contracts` (
    `ins_code` VARCHAR(30) NOT NULL,
    `ua_ins_code` VARCHAR(30) NOT NULL,
    `contract_type` ENUM('CALL','PUT') NOT NULL,
    `short_symbol` VARCHAR(100) NULL,
    `full_symbol` VARCHAR(255) NULL,
    `strike_price` DECIMAL(20,4) NOT NULL,
    `contract_size` INT UNSIGNED NOT NULL,
    `begin_date` DATE NULL,
    `end_date` DATE NOT NULL,
    `begin_date_shamsi` VARCHAR(10) NULL,
    `end_date_shamsi` VARCHAR(10) NULL,
    `remained_days` INT NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`ins_code`),
    KEY `ix_option_contracts_chain` (`ua_ins_code`, `end_date`, `contract_type`, `strike_price`),
    KEY `ix_option_contracts_expiry` (`end_date`),
    CONSTRAINT `fk_option_contract_underlying`
        FOREIGN KEY (`ua_ins_code`) REFERENCES `underlying_assets` (`ua_ins_code`)
        ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- One current live snapshot per instrument (option or underlying).
-- Contains every live field used by 1.ipynb and the strategy engine.
CREATE TABLE IF NOT EXISTS `market_data_ticks` (
    `ins_code` VARCHAR(30) NOT NULL,
    `tick_time` DATETIME NOT NULL,
    `last_price` DECIMAL(20,4) NULL,
    `closing_price` DECIMAL(20,4) NULL,
    `previous_close` DECIMAL(20,4) NULL,
    `bid_price` DECIMAL(20,4) NULL,
    `bid_volume` BIGINT UNSIGNED NULL,
    `ask_price` DECIMAL(20,4) NULL,
    `ask_volume` BIGINT UNSIGNED NULL,
    `open_interest` BIGINT UNSIGNED NULL,
    `previous_open_interest` BIGINT UNSIGNED NULL,
    `trade_count_today` BIGINT UNSIGNED NULL,
    `volume_today` BIGINT UNSIGNED NULL,
    `volume_5d` DECIMAL(26,4) NULL,
    `value_today` DECIMAL(28,4) NULL,
    `notional_value` DECIMAL(28,4) NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`ins_code`),
    KEY `ix_market_ticks_tick_time` (`tick_time`),
    KEY `ix_market_ticks_updated_at` (`updated_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Normalized order book: one row per instrument + depth level (1..5).
-- This avoids 30 repeated columns and is cheaper to update/query.
CREATE TABLE IF NOT EXISTS `order_book_depth` (
    `ins_code` VARCHAR(30) NOT NULL,
    `level` TINYINT UNSIGNED NOT NULL,
    `snapshot_time` DATETIME NOT NULL,
    `bid_price` DECIMAL(20,4) NULL,
    `bid_volume` BIGINT UNSIGNED NULL,
    `bid_order_count` INT UNSIGNED NULL,
    `ask_price` DECIMAL(20,4) NULL,
    `ask_volume` BIGINT UNSIGNED NULL,
    `ask_order_count` INT UNSIGNED NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`ins_code`, `level`),
    KEY `ix_order_book_snapshot` (`snapshot_time`),
    KEY `ix_order_book_level` (`level`, `snapshot_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Historical EOD data. Used by volatility calculations and expiry settlement.
CREATE TABLE IF NOT EXISTS `daily_market_data` (
    `id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `ins_code` VARCHAR(30) NOT NULL,
    `trade_date` DATE NOT NULL,
    `open_price` DECIMAL(20,4) NULL,
    `high_price` DECIMAL(20,4) NULL,
    `low_price` DECIMAL(20,4) NULL,
    `close_price` DECIMAL(20,4) NULL,
    `last_price` DECIMAL(20,4) NULL,
    `previous_close` DECIMAL(20,4) NULL,
    `price_change` DECIMAL(20,4) NULL,
    `volume` BIGINT UNSIGNED NULL,
    `value` DECIMAL(28,4) NULL,
    `trade_count` BIGINT UNSIGNED NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`),
    UNIQUE KEY `uq_daily_market_data_ins_date` (`ins_code`, `trade_date`),
    KEY `ix_daily_market_data_date_ins` (`trade_date`, `ins_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Current Greeks/features snapshot. This is the database equivalent of stage 4 in 1.ipynb.
CREATE TABLE IF NOT EXISTS `option_greeks` (
    `ins_code` VARCHAR(30) NOT NULL,
    `fetch_datetime` DATETIME NOT NULL,
    `ua_ins_code` VARCHAR(30) NOT NULL,
    `option_type` ENUM('CALL','PUT') NOT NULL,
    `symbol` VARCHAR(100) NULL,
    `ua_symbol` VARCHAR(100) NULL,
    `expiry_date` DATE NOT NULL,
    `strike` DECIMAL(20,4) NOT NULL,
    `days_to_expiry` INT NOT NULL,
    `underlying_price` DECIMAL(20,4) NOT NULL,
    `option_price` DECIMAL(20,4) NULL,
    `bid` DECIMAL(20,4) NULL,
    `ask` DECIMAL(20,4) NULL,
    `spread` DECIMAL(20,4) NULL,
    `spread_percent` DECIMAL(18,8) NULL,
    `intrinsic_value` DECIMAL(20,4) NULL,
    `time_value` DECIMAL(20,4) NULL,
    `moneyness` DECIMAL(18,8) NULL,
    `distance_to_strike` DECIMAL(18,8) NULL,
    `implied_volatility` DECIMAL(20,10) NULL,
    `delta` DECIMAL(20,10) NULL,
    `gamma` DECIMAL(20,10) NULL,
    `theta` DECIMAL(20,10) NULL,
    `vega` DECIMAL(20,10) NULL,
    `rho` DECIMAL(20,10) NULL,
    `leverage` DECIMAL(20,10) NULL,
    `elasticity` DECIMAL(20,10) NULL,
    `break_even` DECIMAL(20,4) NULL,
    `open_interest` BIGINT UNSIGNED NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`ins_code`),
    KEY `ix_option_greeks_chain` (`ua_ins_code`, `expiry_date`, `option_type`, `strike`),
    KEY `ix_option_greeks_fetch_time` (`fetch_datetime`),
    CONSTRAINT `fk_option_greeks_contract`
        FOREIGN KEY (`ins_code`) REFERENCES `option_contracts` (`ins_code`)
        ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT `fk_option_greeks_underlying`
        FOREIGN KEY (`ua_ins_code`) REFERENCES `underlying_assets` (`ua_ins_code`)
        ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Small persistent collector/runtime state tables.
CREATE TABLE IF NOT EXISTS `collector_state` (
    `state_key` VARCHAR(100) NOT NULL,
    `state_value` VARCHAR(1000) NULL,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`state_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `history_sync_state` (
    `ins_code` VARCHAR(30) NOT NULL,
    `initial_90_day_complete` TINYINT(1) NOT NULL DEFAULT 0,
    `last_trade_date` DATE NULL,
    `last_success_at` DATETIME NULL,
    `last_error` VARCHAR(1000) NULL,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`ins_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `engine_runs` (
    `run_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `started_at` DATETIME NOT NULL,
    `finished_at` DATETIME NULL,
    `run_mode` VARCHAR(30) NOT NULL DEFAULT 'SCAN',
    `source_revision` VARCHAR(255) NULL,
    `status` ENUM('RUNNING','SUCCESS','FAILED','SKIPPED') NOT NULL DEFAULT 'RUNNING',
    `quotes_count` INT UNSIGNED NOT NULL DEFAULT 0,
    `signals_count` INT UNSIGNED NOT NULL DEFAULT 0,
    `opened_positions_count` INT UNSIGNED NOT NULL DEFAULT 0,
    `closed_positions_count` INT UNSIGNED NOT NULL DEFAULT 0,
    `error_message` VARCHAR(2000) NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`run_id`),
    KEY `ix_engine_runs_started_status` (`started_at`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- SECTION 2 - Paper trading account and position lifecycle
-- ============================================================================

CREATE TABLE IF NOT EXISTS `paper_strategy_account` (
    `account_id` INT UNSIGNED NOT NULL AUTO_INCREMENT,
    `account_name` VARCHAR(100) NOT NULL,
    `currency` VARCHAR(10) NOT NULL DEFAULT 'IRR',
    `initial_equity_rial` DECIMAL(26,4) NOT NULL,
    `current_equity_rial` DECIMAL(26,4) NOT NULL,
    `realized_pnl_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `unrealized_pnl_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `reserved_risk_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `allocated_capital_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `open_positions_count` INT UNSIGNED NOT NULL DEFAULT 0,
    `risk_per_trade_pct` DECIMAL(10,4) NOT NULL,
    `capital_per_position_pct` DECIMAL(10,4) NOT NULL DEFAULT 2,
    `max_total_open_risk_pct` DECIMAL(10,4) NOT NULL,
    `max_capital_usage_pct` DECIMAL(10,4) NOT NULL,
    `max_open_positions` INT UNSIGNED NOT NULL,
    `high_watermark_rial` DECIMAL(26,4) NOT NULL,
    `drawdown_pct` DECIMAL(18,6) NOT NULL DEFAULT 0,
    `last_scan_at` DATETIME NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`account_id`),
    UNIQUE KEY `uq_paper_strategy_account_name` (`account_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ============================================================================
-- SECTION 3 - Strategy signal tables (5 strategies)
-- ============================================================================
--
-- Paper-account execution state semantics (shared by all five signal tables):
--   executed_on_paper_account = 1  -> a real paper position was opened.
--   executed_on_paper_account = 0  -> no paper position was opened.
--   paper_execution_status:
--       PENDING      = signal persisted; account execution not decided yet.
--       EXECUTED     = position opened on the shared paper account.
--       NOT_EXECUTED = evaluated but not opened (e.g. insufficient cash/depth).
--   paper_execution_reason_code / reason explain why a signal was not executed.
--


CREATE TABLE IF NOT EXISTS `covered_call_signals` (
`signal_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `signal_key` VARCHAR(191) NOT NULL,
    `signal_date` DATE NOT NULL,
    `scan_time` DATETIME NOT NULL,
    `last_seen_at` DATETIME NOT NULL,
    `account_id` INT UNSIGNED NOT NULL,
    `strategy_code` VARCHAR(40) NOT NULL,
    `ua_ins_code` VARCHAR(30) NOT NULL,
    `underlying_symbol` VARCHAR(100) NULL,
    `expiry_date` DATE NOT NULL,
    `days_to_expiry` INT NOT NULL,
    `spot_price_rial` DECIMAL(20,4) NOT NULL,

    `leg1_kind` ENUM('UNDERLYING','OPTION') NOT NULL,
    `leg1_ins_code` VARCHAR(30) NOT NULL,
    `leg1_symbol` VARCHAR(100) NULL,
    `leg1_side` ENUM('LONG','SHORT') NOT NULL,
    `leg1_option_type` ENUM('CALL','PUT') NULL,
    `leg1_strike_rial` DECIMAL(20,4) NULL,
    `leg1_entry_price_rial` DECIMAL(20,4) NOT NULL,
    `leg1_contract_size` INT UNSIGNED NOT NULL,

    `leg2_kind` ENUM('UNDERLYING','OPTION') NOT NULL,
    `leg2_ins_code` VARCHAR(30) NOT NULL,
    `leg2_symbol` VARCHAR(100) NULL,
    `leg2_side` ENUM('LONG','SHORT') NOT NULL,
    `leg2_option_type` ENUM('CALL','PUT') NULL,
    `leg2_strike_rial` DECIMAL(20,4) NULL,
    `leg2_entry_price_rial` DECIMAL(20,4) NOT NULL,
    `leg2_contract_size` INT UNSIGNED NOT NULL,

    `unit_capital_rial` DECIMAL(26,4) NOT NULL,
    `unit_max_loss_rial` DECIMAL(26,4) NOT NULL,
    `unit_max_profit_rial` DECIMAL(26,4) NULL,
    `breakeven_low_rial` DECIMAL(20,4) NULL,
    `breakeven_high_rial` DECIMAL(20,4) NULL,
    `reward_risk_ratio` DECIMAL(18,6) NULL,
    `executable_units` INT UNSIGNED NOT NULL DEFAULT 0,
    `risk_budget_rial` DECIMAL(26,4) NOT NULL,
    `recommended_units` INT UNSIGNED NOT NULL DEFAULT 0,
    `recommended_risk_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `recommended_capital_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `liquidity_score` DECIMAL(10,4) NOT NULL DEFAULT 0,
    `strategy_score` DECIMAL(10,4) NOT NULL DEFAULT 0,
    `final_signal` VARCHAR(30) NOT NULL,
    `signal_reason` VARCHAR(1000) NULL,
    `is_current` TINYINT(1) NOT NULL DEFAULT 1,
    `opened_position_id` BIGINT UNSIGNED NULL,

    `executed_on_paper_account` TINYINT(1) NOT NULL DEFAULT 0,
    `paper_execution_status` ENUM('PENDING','EXECUTED','NOT_EXECUTED') NOT NULL DEFAULT 'PENDING',
    `paper_execution_reason_code` VARCHAR(64) NULL,
    `paper_execution_reason` VARCHAR(1000) NULL,
    `paper_execution_checked_at` DATETIME NULL,
    `paper_executed_at` DATETIME NULL,

`premium_income_rial` DECIMAL(26,4) NULL,
    `return_to_expiry_pct` DECIMAL(18,6) NULL,
    `annualized_return_pct` DECIMAL(18,6) NULL,
    `moneyness_pct` DECIMAL(18,6) NULL,

    `details_json` JSON NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`signal_id`),
    KEY `ix_covered_call_paper_execution` (`account_id`, `executed_on_paper_account`, `paper_execution_status`, `signal_date`),
    UNIQUE KEY `uq_covered_call_signal_key` (`signal_key`),
    KEY `ix_covered_call_current_score` (`account_id`, `is_current`, `strategy_score`),
    KEY `ix_covered_call_signal_state` (`account_id`, `final_signal`, `is_current`),
    KEY `ix_covered_call_underlying_expiry` (`ua_ins_code`, `expiry_date`),
    KEY `ix_covered_call_opened_position` (`opened_position_id`),
    CONSTRAINT `fk_covered_call_account`
        FOREIGN KEY (`account_id`) REFERENCES `paper_strategy_account` (`account_id`)
        ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `protective_put_signals` (
`signal_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `signal_key` VARCHAR(191) NOT NULL,
    `signal_date` DATE NOT NULL,
    `scan_time` DATETIME NOT NULL,
    `last_seen_at` DATETIME NOT NULL,
    `account_id` INT UNSIGNED NOT NULL,
    `strategy_code` VARCHAR(40) NOT NULL,
    `ua_ins_code` VARCHAR(30) NOT NULL,
    `underlying_symbol` VARCHAR(100) NULL,
    `expiry_date` DATE NOT NULL,
    `days_to_expiry` INT NOT NULL,
    `spot_price_rial` DECIMAL(20,4) NOT NULL,

    `leg1_kind` ENUM('UNDERLYING','OPTION') NOT NULL,
    `leg1_ins_code` VARCHAR(30) NOT NULL,
    `leg1_symbol` VARCHAR(100) NULL,
    `leg1_side` ENUM('LONG','SHORT') NOT NULL,
    `leg1_option_type` ENUM('CALL','PUT') NULL,
    `leg1_strike_rial` DECIMAL(20,4) NULL,
    `leg1_entry_price_rial` DECIMAL(20,4) NOT NULL,
    `leg1_contract_size` INT UNSIGNED NOT NULL,

    `leg2_kind` ENUM('UNDERLYING','OPTION') NOT NULL,
    `leg2_ins_code` VARCHAR(30) NOT NULL,
    `leg2_symbol` VARCHAR(100) NULL,
    `leg2_side` ENUM('LONG','SHORT') NOT NULL,
    `leg2_option_type` ENUM('CALL','PUT') NULL,
    `leg2_strike_rial` DECIMAL(20,4) NULL,
    `leg2_entry_price_rial` DECIMAL(20,4) NOT NULL,
    `leg2_contract_size` INT UNSIGNED NOT NULL,

    `unit_capital_rial` DECIMAL(26,4) NOT NULL,
    `unit_max_loss_rial` DECIMAL(26,4) NOT NULL,
    `unit_max_profit_rial` DECIMAL(26,4) NULL,
    `breakeven_low_rial` DECIMAL(20,4) NULL,
    `breakeven_high_rial` DECIMAL(20,4) NULL,
    `reward_risk_ratio` DECIMAL(18,6) NULL,
    `executable_units` INT UNSIGNED NOT NULL DEFAULT 0,
    `risk_budget_rial` DECIMAL(26,4) NOT NULL,
    `recommended_units` INT UNSIGNED NOT NULL DEFAULT 0,
    `recommended_risk_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `recommended_capital_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `liquidity_score` DECIMAL(10,4) NOT NULL DEFAULT 0,
    `strategy_score` DECIMAL(10,4) NOT NULL DEFAULT 0,
    `final_signal` VARCHAR(30) NOT NULL,
    `signal_reason` VARCHAR(1000) NULL,
    `is_current` TINYINT(1) NOT NULL DEFAULT 1,
    `opened_position_id` BIGINT UNSIGNED NULL,

    `executed_on_paper_account` TINYINT(1) NOT NULL DEFAULT 0,
    `paper_execution_status` ENUM('PENDING','EXECUTED','NOT_EXECUTED') NOT NULL DEFAULT 'PENDING',
    `paper_execution_reason_code` VARCHAR(64) NULL,
    `paper_execution_reason` VARCHAR(1000) NULL,
    `paper_execution_checked_at` DATETIME NULL,
    `paper_executed_at` DATETIME NULL,

`insurance_cost_rial` DECIMAL(26,4) NULL,
    `insurance_cost_pct` DECIMAL(18,6) NULL,
    `protection_gap_pct` DECIMAL(18,6) NULL,

    `details_json` JSON NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`signal_id`),
    KEY `ix_protective_put_paper_execution` (`account_id`, `executed_on_paper_account`, `paper_execution_status`, `signal_date`),
    UNIQUE KEY `uq_protective_put_signal_key` (`signal_key`),
    KEY `ix_protective_put_current_score` (`account_id`, `is_current`, `strategy_score`),
    KEY `ix_protective_put_signal_state` (`account_id`, `final_signal`, `is_current`),
    KEY `ix_protective_put_underlying_expiry` (`ua_ins_code`, `expiry_date`),
    KEY `ix_protective_put_opened_position` (`opened_position_id`),
    CONSTRAINT `fk_protective_put_account`
        FOREIGN KEY (`account_id`) REFERENCES `paper_strategy_account` (`account_id`)
        ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `bull_call_spread_signals` (
`signal_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `signal_key` VARCHAR(191) NOT NULL,
    `signal_date` DATE NOT NULL,
    `scan_time` DATETIME NOT NULL,
    `last_seen_at` DATETIME NOT NULL,
    `account_id` INT UNSIGNED NOT NULL,
    `strategy_code` VARCHAR(40) NOT NULL,
    `ua_ins_code` VARCHAR(30) NOT NULL,
    `underlying_symbol` VARCHAR(100) NULL,
    `expiry_date` DATE NOT NULL,
    `days_to_expiry` INT NOT NULL,
    `spot_price_rial` DECIMAL(20,4) NOT NULL,

    `leg1_kind` ENUM('UNDERLYING','OPTION') NOT NULL,
    `leg1_ins_code` VARCHAR(30) NOT NULL,
    `leg1_symbol` VARCHAR(100) NULL,
    `leg1_side` ENUM('LONG','SHORT') NOT NULL,
    `leg1_option_type` ENUM('CALL','PUT') NULL,
    `leg1_strike_rial` DECIMAL(20,4) NULL,
    `leg1_entry_price_rial` DECIMAL(20,4) NOT NULL,
    `leg1_contract_size` INT UNSIGNED NOT NULL,

    `leg2_kind` ENUM('UNDERLYING','OPTION') NOT NULL,
    `leg2_ins_code` VARCHAR(30) NOT NULL,
    `leg2_symbol` VARCHAR(100) NULL,
    `leg2_side` ENUM('LONG','SHORT') NOT NULL,
    `leg2_option_type` ENUM('CALL','PUT') NULL,
    `leg2_strike_rial` DECIMAL(20,4) NULL,
    `leg2_entry_price_rial` DECIMAL(20,4) NOT NULL,
    `leg2_contract_size` INT UNSIGNED NOT NULL,

    `unit_capital_rial` DECIMAL(26,4) NOT NULL,
    `unit_max_loss_rial` DECIMAL(26,4) NOT NULL,
    `unit_max_profit_rial` DECIMAL(26,4) NULL,
    `breakeven_low_rial` DECIMAL(20,4) NULL,
    `breakeven_high_rial` DECIMAL(20,4) NULL,
    `reward_risk_ratio` DECIMAL(18,6) NULL,
    `executable_units` INT UNSIGNED NOT NULL DEFAULT 0,
    `risk_budget_rial` DECIMAL(26,4) NOT NULL,
    `recommended_units` INT UNSIGNED NOT NULL DEFAULT 0,
    `recommended_risk_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `recommended_capital_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `liquidity_score` DECIMAL(10,4) NOT NULL DEFAULT 0,
    `strategy_score` DECIMAL(10,4) NOT NULL DEFAULT 0,
    `final_signal` VARCHAR(30) NOT NULL,
    `signal_reason` VARCHAR(1000) NULL,
    `is_current` TINYINT(1) NOT NULL DEFAULT 1,
    `opened_position_id` BIGINT UNSIGNED NULL,

    `executed_on_paper_account` TINYINT(1) NOT NULL DEFAULT 0,
    `paper_execution_status` ENUM('PENDING','EXECUTED','NOT_EXECUTED') NOT NULL DEFAULT 'PENDING',
    `paper_execution_reason_code` VARCHAR(64) NULL,
    `paper_execution_reason` VARCHAR(1000) NULL,
    `paper_execution_checked_at` DATETIME NULL,
    `paper_executed_at` DATETIME NULL,

`net_debit_per_unit_rial` DECIMAL(20,4) NULL,
    `strike_width_rial` DECIMAL(20,4) NULL,
    `required_spot_move_pct` DECIMAL(18,6) NULL,

    `details_json` JSON NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`signal_id`),
    KEY `ix_bull_call_spread_paper_execution` (`account_id`, `executed_on_paper_account`, `paper_execution_status`, `signal_date`),
    UNIQUE KEY `uq_bull_call_spread_signal_key` (`signal_key`),
    KEY `ix_bull_call_spread_current_score` (`account_id`, `is_current`, `strategy_score`),
    KEY `ix_bull_call_spread_signal_state` (`account_id`, `final_signal`, `is_current`),
    KEY `ix_bull_call_spread_underlying_expiry` (`ua_ins_code`, `expiry_date`),
    KEY `ix_bull_call_spread_opened_position` (`opened_position_id`),
    CONSTRAINT `fk_bull_call_spread_account`
        FOREIGN KEY (`account_id`) REFERENCES `paper_strategy_account` (`account_id`)
        ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `bear_put_spread_signals` (
`signal_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `signal_key` VARCHAR(191) NOT NULL,
    `signal_date` DATE NOT NULL,
    `scan_time` DATETIME NOT NULL,
    `last_seen_at` DATETIME NOT NULL,
    `account_id` INT UNSIGNED NOT NULL,
    `strategy_code` VARCHAR(40) NOT NULL,
    `ua_ins_code` VARCHAR(30) NOT NULL,
    `underlying_symbol` VARCHAR(100) NULL,
    `expiry_date` DATE NOT NULL,
    `days_to_expiry` INT NOT NULL,
    `spot_price_rial` DECIMAL(20,4) NOT NULL,

    `leg1_kind` ENUM('UNDERLYING','OPTION') NOT NULL,
    `leg1_ins_code` VARCHAR(30) NOT NULL,
    `leg1_symbol` VARCHAR(100) NULL,
    `leg1_side` ENUM('LONG','SHORT') NOT NULL,
    `leg1_option_type` ENUM('CALL','PUT') NULL,
    `leg1_strike_rial` DECIMAL(20,4) NULL,
    `leg1_entry_price_rial` DECIMAL(20,4) NOT NULL,
    `leg1_contract_size` INT UNSIGNED NOT NULL,

    `leg2_kind` ENUM('UNDERLYING','OPTION') NOT NULL,
    `leg2_ins_code` VARCHAR(30) NOT NULL,
    `leg2_symbol` VARCHAR(100) NULL,
    `leg2_side` ENUM('LONG','SHORT') NOT NULL,
    `leg2_option_type` ENUM('CALL','PUT') NULL,
    `leg2_strike_rial` DECIMAL(20,4) NULL,
    `leg2_entry_price_rial` DECIMAL(20,4) NOT NULL,
    `leg2_contract_size` INT UNSIGNED NOT NULL,

    `unit_capital_rial` DECIMAL(26,4) NOT NULL,
    `unit_max_loss_rial` DECIMAL(26,4) NOT NULL,
    `unit_max_profit_rial` DECIMAL(26,4) NULL,
    `breakeven_low_rial` DECIMAL(20,4) NULL,
    `breakeven_high_rial` DECIMAL(20,4) NULL,
    `reward_risk_ratio` DECIMAL(18,6) NULL,
    `executable_units` INT UNSIGNED NOT NULL DEFAULT 0,
    `risk_budget_rial` DECIMAL(26,4) NOT NULL,
    `recommended_units` INT UNSIGNED NOT NULL DEFAULT 0,
    `recommended_risk_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `recommended_capital_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `liquidity_score` DECIMAL(10,4) NOT NULL DEFAULT 0,
    `strategy_score` DECIMAL(10,4) NOT NULL DEFAULT 0,
    `final_signal` VARCHAR(30) NOT NULL,
    `signal_reason` VARCHAR(1000) NULL,
    `is_current` TINYINT(1) NOT NULL DEFAULT 1,
    `opened_position_id` BIGINT UNSIGNED NULL,

    `executed_on_paper_account` TINYINT(1) NOT NULL DEFAULT 0,
    `paper_execution_status` ENUM('PENDING','EXECUTED','NOT_EXECUTED') NOT NULL DEFAULT 'PENDING',
    `paper_execution_reason_code` VARCHAR(64) NULL,
    `paper_execution_reason` VARCHAR(1000) NULL,
    `paper_execution_checked_at` DATETIME NULL,
    `paper_executed_at` DATETIME NULL,

`net_debit_per_unit_rial` DECIMAL(20,4) NULL,
    `strike_width_rial` DECIMAL(20,4) NULL,
    `required_spot_drop_pct` DECIMAL(18,6) NULL,

    `details_json` JSON NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`signal_id`),
    KEY `ix_bear_put_spread_paper_execution` (`account_id`, `executed_on_paper_account`, `paper_execution_status`, `signal_date`),
    UNIQUE KEY `uq_bear_put_spread_signal_key` (`signal_key`),
    KEY `ix_bear_put_spread_current_score` (`account_id`, `is_current`, `strategy_score`),
    KEY `ix_bear_put_spread_signal_state` (`account_id`, `final_signal`, `is_current`),
    KEY `ix_bear_put_spread_underlying_expiry` (`ua_ins_code`, `expiry_date`),
    KEY `ix_bear_put_spread_opened_position` (`opened_position_id`),
    CONSTRAINT `fk_bear_put_spread_account`
        FOREIGN KEY (`account_id`) REFERENCES `paper_strategy_account` (`account_id`)
        ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


CREATE TABLE IF NOT EXISTS `long_straddle_signals` (
`signal_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `signal_key` VARCHAR(191) NOT NULL,
    `signal_date` DATE NOT NULL,
    `scan_time` DATETIME NOT NULL,
    `last_seen_at` DATETIME NOT NULL,
    `account_id` INT UNSIGNED NOT NULL,
    `strategy_code` VARCHAR(40) NOT NULL,
    `ua_ins_code` VARCHAR(30) NOT NULL,
    `underlying_symbol` VARCHAR(100) NULL,
    `expiry_date` DATE NOT NULL,
    `days_to_expiry` INT NOT NULL,
    `spot_price_rial` DECIMAL(20,4) NOT NULL,

    `leg1_kind` ENUM('UNDERLYING','OPTION') NOT NULL,
    `leg1_ins_code` VARCHAR(30) NOT NULL,
    `leg1_symbol` VARCHAR(100) NULL,
    `leg1_side` ENUM('LONG','SHORT') NOT NULL,
    `leg1_option_type` ENUM('CALL','PUT') NULL,
    `leg1_strike_rial` DECIMAL(20,4) NULL,
    `leg1_entry_price_rial` DECIMAL(20,4) NOT NULL,
    `leg1_contract_size` INT UNSIGNED NOT NULL,

    `leg2_kind` ENUM('UNDERLYING','OPTION') NOT NULL,
    `leg2_ins_code` VARCHAR(30) NOT NULL,
    `leg2_symbol` VARCHAR(100) NULL,
    `leg2_side` ENUM('LONG','SHORT') NOT NULL,
    `leg2_option_type` ENUM('CALL','PUT') NULL,
    `leg2_strike_rial` DECIMAL(20,4) NULL,
    `leg2_entry_price_rial` DECIMAL(20,4) NOT NULL,
    `leg2_contract_size` INT UNSIGNED NOT NULL,

    `unit_capital_rial` DECIMAL(26,4) NOT NULL,
    `unit_max_loss_rial` DECIMAL(26,4) NOT NULL,
    `unit_max_profit_rial` DECIMAL(26,4) NULL,
    `breakeven_low_rial` DECIMAL(20,4) NULL,
    `breakeven_high_rial` DECIMAL(20,4) NULL,
    `reward_risk_ratio` DECIMAL(18,6) NULL,
    `executable_units` INT UNSIGNED NOT NULL DEFAULT 0,
    `risk_budget_rial` DECIMAL(26,4) NOT NULL,
    `recommended_units` INT UNSIGNED NOT NULL DEFAULT 0,
    `recommended_risk_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `recommended_capital_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `liquidity_score` DECIMAL(10,4) NOT NULL DEFAULT 0,
    `strategy_score` DECIMAL(10,4) NOT NULL DEFAULT 0,
    `final_signal` VARCHAR(30) NOT NULL,
    `signal_reason` VARCHAR(1000) NULL,
    `is_current` TINYINT(1) NOT NULL DEFAULT 1,
    `opened_position_id` BIGINT UNSIGNED NULL,

    `executed_on_paper_account` TINYINT(1) NOT NULL DEFAULT 0,
    `paper_execution_status` ENUM('PENDING','EXECUTED','NOT_EXECUTED') NOT NULL DEFAULT 'PENDING',
    `paper_execution_reason_code` VARCHAR(64) NULL,
    `paper_execution_reason` VARCHAR(1000) NULL,
    `paper_execution_checked_at` DATETIME NULL,
    `paper_executed_at` DATETIME NULL,

`total_premium_per_unit_rial` DECIMAL(20,4) NULL,
    `required_move_pct` DECIMAL(18,6) NULL,
    `annualized_volatility_pct` DECIMAL(18,6) NULL,
    `expected_move_to_expiry_pct` DECIMAL(18,6) NULL,
    `volatility_observations` INT UNSIGNED NULL,

    `details_json` JSON NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (`signal_id`),
    KEY `ix_long_straddle_paper_execution` (`account_id`, `executed_on_paper_account`, `paper_execution_status`, `signal_date`),
    UNIQUE KEY `uq_long_straddle_signal_key` (`signal_key`),
    KEY `ix_long_straddle_current_score` (`account_id`, `is_current`, `strategy_score`),
    KEY `ix_long_straddle_signal_state` (`account_id`, `final_signal`, `is_current`),
    KEY `ix_long_straddle_underlying_expiry` (`ua_ins_code`, `expiry_date`),
    KEY `ix_long_straddle_opened_position` (`opened_position_id`),
    CONSTRAINT `fk_long_straddle_account`
        FOREIGN KEY (`account_id`) REFERENCES `paper_strategy_account` (`account_id`)
        ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;


-- ============================================================================
-- SECTION 4 - Paper positions, legs, valuations, and account equity history
-- ============================================================================

CREATE TABLE IF NOT EXISTS `paper_positions` (
    `position_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `account_id` INT UNSIGNED NOT NULL,
    `signal_table` VARCHAR(64) NOT NULL,
    `signal_id` BIGINT UNSIGNED NOT NULL,
    `signal_key` VARCHAR(191) NOT NULL,
    `position_signature` VARCHAR(700) NOT NULL,
    `strategy_code` VARCHAR(40) NOT NULL,
    `ua_ins_code` VARCHAR(30) NOT NULL,
    `underlying_symbol` VARCHAR(100) NULL,
    `expiry_date` DATE NOT NULL,
    `status` ENUM('OPEN','CLOSED','EXPIRED') NOT NULL DEFAULT 'OPEN',
    `units` INT UNSIGNED NOT NULL,
    `entry_risk_budget_rial` DECIMAL(26,4) NOT NULL,
    `entry_unit_risk_rial` DECIMAL(26,4) NOT NULL,
    `entry_max_loss_rial` DECIMAL(26,4) NOT NULL,
    `entry_max_profit_rial` DECIMAL(26,4) NULL,
    `entry_capital_rial` DECIMAL(26,4) NOT NULL,
    `entry_net_value_rial` DECIMAL(26,4) NOT NULL,
    `current_position_value_rial` DECIMAL(26,4) NOT NULL,
    `current_underlying_price_rial` DECIMAL(20,4) NULL,
    `unrealized_pnl_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `realized_pnl_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `return_on_risk_pct` DECIMAL(18,6) NOT NULL DEFAULT 0,
    `max_favorable_pnl_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `max_adverse_pnl_rial` DECIMAL(26,4) NOT NULL DEFAULT 0,
    `opened_at` DATETIME NOT NULL,
    `last_valued_at` DATETIME NULL,
    `closed_at` DATETIME NULL,
    `close_reason` VARCHAR(1000) NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`position_id`),
    UNIQUE KEY `uq_paper_position_source` (`account_id`, `signal_table`, `signal_id`),
    KEY `ix_paper_positions_account_status` (`account_id`, `status`, `opened_at`),
    KEY `ix_paper_positions_underlying_status` (`ua_ins_code`, `status`),
    KEY `ix_paper_positions_strategy_status` (`strategy_code`, `status`),
    KEY `ix_paper_positions_signature` (`account_id`, `status`, `position_signature`(191)),
    CONSTRAINT `fk_paper_positions_account`
        FOREIGN KEY (`account_id`) REFERENCES `paper_strategy_account` (`account_id`)
        ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `paper_position_legs` (
    `leg_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `position_id` BIGINT UNSIGNED NOT NULL,
    `leg_no` SMALLINT UNSIGNED NOT NULL,
    `instrument_kind` ENUM('UNDERLYING','OPTION') NOT NULL,
    `ins_code` VARCHAR(30) NOT NULL,
    `symbol` VARCHAR(100) NULL,
    `side` ENUM('LONG','SHORT') NOT NULL,
    `option_type` ENUM('CALL','PUT') NULL,
    `strike_price_rial` DECIMAL(20,4) NULL,
    `expiry_date` DATE NULL,
    `contract_size` INT UNSIGNED NOT NULL,
    `contract_count` INT UNSIGNED NOT NULL,
    `quantity_units` BIGINT UNSIGNED NOT NULL,
    `entry_price_rial` DECIMAL(20,4) NOT NULL,
    `latest_price_rial` DECIMAL(20,4) NOT NULL,
    `exit_price_rial` DECIMAL(20,4) NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`leg_id`),
    UNIQUE KEY `uq_paper_position_leg` (`position_id`, `leg_no`),
    KEY `ix_paper_position_legs_ins_code` (`ins_code`),
    CONSTRAINT `fk_paper_position_legs_position`
        FOREIGN KEY (`position_id`) REFERENCES `paper_positions` (`position_id`)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `paper_position_valuations` (
    `valuation_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `position_id` BIGINT UNSIGNED NOT NULL,
    `valuation_time` DATETIME NOT NULL,
    `position_value_rial` DECIMAL(26,4) NOT NULL,
    `unrealized_pnl_rial` DECIMAL(26,4) NOT NULL,
    `return_on_risk_pct` DECIMAL(18,6) NOT NULL,
    `underlying_price_rial` DECIMAL(20,4) NULL,
    `max_favorable_pnl_rial` DECIMAL(26,4) NOT NULL,
    `max_adverse_pnl_rial` DECIMAL(26,4) NOT NULL,
    `quote_is_stale` TINYINT(1) NOT NULL DEFAULT 0,
    `valuation_reason` VARCHAR(500) NULL,
    `leg_marks_json` JSON NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`valuation_id`),
    UNIQUE KEY `uq_paper_valuation_time` (`position_id`, `valuation_time`),
    KEY `ix_paper_valuations_time` (`valuation_time`),
    CONSTRAINT `fk_paper_valuations_position`
        FOREIGN KEY (`position_id`) REFERENCES `paper_positions` (`position_id`)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS `paper_account_equity_history` (
    `equity_history_id` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    `account_id` INT UNSIGNED NOT NULL,
    `snapshot_time` DATETIME NOT NULL,
    `current_equity_rial` DECIMAL(26,4) NOT NULL,
    `realized_pnl_rial` DECIMAL(26,4) NOT NULL,
    `unrealized_pnl_rial` DECIMAL(26,4) NOT NULL,
    `reserved_risk_rial` DECIMAL(26,4) NOT NULL,
    `allocated_capital_rial` DECIMAL(26,4) NOT NULL,
    `open_positions_count` INT UNSIGNED NOT NULL,
    `drawdown_pct` DECIMAL(18,6) NOT NULL,
    `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (`equity_history_id`),
    UNIQUE KEY `uq_paper_equity_snapshot` (`account_id`, `snapshot_time`),
    KEY `ix_paper_equity_history_time` (`snapshot_time`),
    CONSTRAINT `fk_paper_equity_account`
        FOREIGN KEY (`account_id`) REFERENCES `paper_strategy_account` (`account_id`)
        ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- ============================================================================
-- SECTION 5 - Compatibility / reporting views
-- ============================================================================

-- Reconstructs the stage-2 shape from 1.ipynb without duplicating storage.
DROP VIEW IF EXISTS `vw_option_market_watch`;
CREATE VIEW `vw_option_market_watch` AS
SELECT
    c.`ins_code` AS `ins_code`,
    CASE c.`contract_type` WHEN 'CALL' THEN 'Call' ELSE 'Put' END AS `option_type`,
    c.`short_symbol` AS `symbol`,
    c.`full_symbol` AS `name`,
    c.`ua_ins_code` AS `ua_ins_code`,
    u.`symbol` AS `ua_symbol`,
    u.`closing_price` AS `ua_close`,
    u.`last_trade_price` AS `ua_last`,
    u.`previous_closing_price` AS `ua_prev_close`,
    c.`contract_size` AS `contract_size`,
    c.`strike_price` AS `strike`,
    c.`begin_date` AS `begin_date`,
    c.`end_date` AS `end_date`,
    c.`begin_date_shamsi` AS `begin_date_shamsi`,
    c.`end_date_shamsi` AS `end_date_shamsi`,
    c.`remained_days` AS `days_to_expiry`,
    t.`last_price` AS `last`,
    t.`closing_price` AS `close`,
    t.`previous_close` AS `prev_close`,
    t.`open_interest` AS `open_interest`,
    t.`previous_open_interest` AS `prev_open_interest`,
    t.`trade_count_today` AS `trades_count`,
    t.`volume_5d` AS `volume_5d`,
    t.`value_today` AS `trade_value`,
    t.`notional_value` AS `notional_value`,
    t.`bid_price` AS `bid`,
    t.`bid_volume` AS `bid_volume`,
    t.`ask_price` AS `ask`,
    t.`ask_volume` AS `ask_volume`,
    t.`tick_time` AS `fetch_datetime`
FROM `option_contracts` c
JOIN `underlying_assets` u ON u.`ua_ins_code` = c.`ua_ins_code`
LEFT JOIN `market_data_ticks` t ON t.`ins_code` = c.`ins_code`;

-- Reconstructs the wide 5-level order-book table used in 1.ipynb.
DROP VIEW IF EXISTS `vw_order_book_depth_5`;
CREATE VIEW `vw_order_book_depth_5` AS
SELECT
    `ins_code`,
    MAX(`snapshot_time`) AS `fetch_datetime`,
    MAX(CASE WHEN `level`=1 THEN `bid_price` END) AS `bid_price_1`,
    MAX(CASE WHEN `level`=1 THEN `bid_volume` END) AS `bid_volume_1`,
    MAX(CASE WHEN `level`=1 THEN `bid_order_count` END) AS `bid_orders_1`,
    MAX(CASE WHEN `level`=1 THEN `ask_price` END) AS `ask_price_1`,
    MAX(CASE WHEN `level`=1 THEN `ask_volume` END) AS `ask_volume_1`,
    MAX(CASE WHEN `level`=1 THEN `ask_order_count` END) AS `ask_orders_1`,
    MAX(CASE WHEN `level`=2 THEN `bid_price` END) AS `bid_price_2`,
    MAX(CASE WHEN `level`=2 THEN `bid_volume` END) AS `bid_volume_2`,
    MAX(CASE WHEN `level`=2 THEN `bid_order_count` END) AS `bid_orders_2`,
    MAX(CASE WHEN `level`=2 THEN `ask_price` END) AS `ask_price_2`,
    MAX(CASE WHEN `level`=2 THEN `ask_volume` END) AS `ask_volume_2`,
    MAX(CASE WHEN `level`=2 THEN `ask_order_count` END) AS `ask_orders_2`,
    MAX(CASE WHEN `level`=3 THEN `bid_price` END) AS `bid_price_3`,
    MAX(CASE WHEN `level`=3 THEN `bid_volume` END) AS `bid_volume_3`,
    MAX(CASE WHEN `level`=3 THEN `bid_order_count` END) AS `bid_orders_3`,
    MAX(CASE WHEN `level`=3 THEN `ask_price` END) AS `ask_price_3`,
    MAX(CASE WHEN `level`=3 THEN `ask_volume` END) AS `ask_volume_3`,
    MAX(CASE WHEN `level`=3 THEN `ask_order_count` END) AS `ask_orders_3`,
    MAX(CASE WHEN `level`=4 THEN `bid_price` END) AS `bid_price_4`,
    MAX(CASE WHEN `level`=4 THEN `bid_volume` END) AS `bid_volume_4`,
    MAX(CASE WHEN `level`=4 THEN `bid_order_count` END) AS `bid_orders_4`,
    MAX(CASE WHEN `level`=4 THEN `ask_price` END) AS `ask_price_4`,
    MAX(CASE WHEN `level`=4 THEN `ask_volume` END) AS `ask_volume_4`,
    MAX(CASE WHEN `level`=4 THEN `ask_order_count` END) AS `ask_orders_4`,
    MAX(CASE WHEN `level`=5 THEN `bid_price` END) AS `bid_price_5`,
    MAX(CASE WHEN `level`=5 THEN `bid_volume` END) AS `bid_volume_5`,
    MAX(CASE WHEN `level`=5 THEN `bid_order_count` END) AS `bid_orders_5`,
    MAX(CASE WHEN `level`=5 THEN `ask_price` END) AS `ask_price_5`,
    MAX(CASE WHEN `level`=5 THEN `ask_volume` END) AS `ask_volume_5`,
    MAX(CASE WHEN `level`=5 THEN `ask_order_count` END) AS `ask_orders_5`
FROM `order_book_depth`
GROUP BY `ins_code`;

DROP VIEW IF EXISTS `vw_current_strategy_signals`;
CREATE VIEW `vw_current_strategy_signals` AS
SELECT `signal_id`,`signal_key`,`scan_time`,`account_id`,`strategy_code`,`ua_ins_code`,`underlying_symbol`,`expiry_date`,`days_to_expiry`,`spot_price_rial`,`recommended_units`,`recommended_risk_rial`,`recommended_capital_rial`,`liquidity_score`,`strategy_score`,`final_signal`,`signal_reason`,`executed_on_paper_account`,`paper_execution_status`,`paper_execution_reason_code`,`paper_execution_reason`,`paper_execution_checked_at`,`paper_executed_at`,`opened_position_id`
FROM `covered_call_signals` WHERE `is_current`=1
UNION ALL
SELECT `signal_id`,`signal_key`,`scan_time`,`account_id`,`strategy_code`,`ua_ins_code`,`underlying_symbol`,`expiry_date`,`days_to_expiry`,`spot_price_rial`,`recommended_units`,`recommended_risk_rial`,`recommended_capital_rial`,`liquidity_score`,`strategy_score`,`final_signal`,`signal_reason`,`executed_on_paper_account`,`paper_execution_status`,`paper_execution_reason_code`,`paper_execution_reason`,`paper_execution_checked_at`,`paper_executed_at`,`opened_position_id`
FROM `protective_put_signals` WHERE `is_current`=1
UNION ALL
SELECT `signal_id`,`signal_key`,`scan_time`,`account_id`,`strategy_code`,`ua_ins_code`,`underlying_symbol`,`expiry_date`,`days_to_expiry`,`spot_price_rial`,`recommended_units`,`recommended_risk_rial`,`recommended_capital_rial`,`liquidity_score`,`strategy_score`,`final_signal`,`signal_reason`,`executed_on_paper_account`,`paper_execution_status`,`paper_execution_reason_code`,`paper_execution_reason`,`paper_execution_checked_at`,`paper_executed_at`,`opened_position_id`
FROM `bull_call_spread_signals` WHERE `is_current`=1
UNION ALL
SELECT `signal_id`,`signal_key`,`scan_time`,`account_id`,`strategy_code`,`ua_ins_code`,`underlying_symbol`,`expiry_date`,`days_to_expiry`,`spot_price_rial`,`recommended_units`,`recommended_risk_rial`,`recommended_capital_rial`,`liquidity_score`,`strategy_score`,`final_signal`,`signal_reason`,`executed_on_paper_account`,`paper_execution_status`,`paper_execution_reason_code`,`paper_execution_reason`,`paper_execution_checked_at`,`paper_executed_at`,`opened_position_id`
FROM `bear_put_spread_signals` WHERE `is_current`=1
UNION ALL
SELECT `signal_id`,`signal_key`,`scan_time`,`account_id`,`strategy_code`,`ua_ins_code`,`underlying_symbol`,`expiry_date`,`days_to_expiry`,`spot_price_rial`,`recommended_units`,`recommended_risk_rial`,`recommended_capital_rial`,`liquidity_score`,`strategy_score`,`final_signal`,`signal_reason`,`executed_on_paper_account`,`paper_execution_status`,`paper_execution_reason_code`,`paper_execution_reason`,`paper_execution_checked_at`,`paper_executed_at`,`opened_position_id`
FROM `long_straddle_signals` WHERE `is_current`=1;

-- Unified append-history view used by Bale and the end-of-day CSV export.
-- One row = one persisted strategy signal. Execution columns explicitly
-- distinguish signals that did/did not enter the shared paper account.
DROP VIEW IF EXISTS `vw_all_strategy_signals`;
CREATE VIEW `vw_all_strategy_signals` AS
SELECT 'covered_call_signals' AS `signal_source_table`,
    s.`signal_id`, s.`signal_key`, s.`signal_date`, s.`scan_time`, s.`last_seen_at`,
    s.`account_id`, s.`strategy_code`, s.`ua_ins_code`, s.`underlying_symbol`,
    s.`expiry_date`, s.`days_to_expiry`, s.`spot_price_rial`,
    s.`leg1_kind`, s.`leg1_ins_code`, s.`leg1_symbol`, s.`leg1_side`, s.`leg1_option_type`,
    s.`leg1_strike_rial`, s.`leg1_entry_price_rial`, s.`leg1_contract_size`,
    s.`leg2_kind`, s.`leg2_ins_code`, s.`leg2_symbol`, s.`leg2_side`, s.`leg2_option_type`,
    s.`leg2_strike_rial`, s.`leg2_entry_price_rial`, s.`leg2_contract_size`,
    s.`unit_capital_rial`, s.`unit_max_loss_rial`, s.`unit_max_profit_rial`,
    s.`breakeven_low_rial`, s.`breakeven_high_rial`, s.`reward_risk_ratio`,
    s.`executable_units`, s.`risk_budget_rial`, s.`recommended_units`,
    s.`recommended_risk_rial`, s.`recommended_capital_rial`, s.`liquidity_score`,
    s.`strategy_score`, s.`final_signal`, s.`signal_reason`, s.`is_current`,
    s.`executed_on_paper_account`, s.`paper_execution_status`,
    s.`paper_execution_reason_code`, s.`paper_execution_reason`,
    s.`paper_execution_checked_at`, s.`paper_executed_at`, s.`opened_position_id`,
    p.`status` AS `paper_position_status`,
    p.`units` AS `paper_executed_units`,
    p.`entry_capital_rial` AS `paper_entry_capital_rial`,
    p.`entry_max_loss_rial` AS `paper_entry_max_loss_rial`,
    p.`opened_at` AS `paper_opened_at`,
    p.`closed_at` AS `paper_closed_at`,
    p.`realized_pnl_rial` AS `paper_realized_pnl_rial`,
    p.`unrealized_pnl_rial` AS `paper_unrealized_pnl_rial`,
    s.`details_json`, s.`created_at`, s.`updated_at`
FROM `covered_call_signals` s
LEFT JOIN `paper_positions` p ON p.`position_id` = s.`opened_position_id`
UNION ALL
SELECT 'protective_put_signals' AS `signal_source_table`,
    s.`signal_id`, s.`signal_key`, s.`signal_date`, s.`scan_time`, s.`last_seen_at`,
    s.`account_id`, s.`strategy_code`, s.`ua_ins_code`, s.`underlying_symbol`,
    s.`expiry_date`, s.`days_to_expiry`, s.`spot_price_rial`,
    s.`leg1_kind`, s.`leg1_ins_code`, s.`leg1_symbol`, s.`leg1_side`, s.`leg1_option_type`,
    s.`leg1_strike_rial`, s.`leg1_entry_price_rial`, s.`leg1_contract_size`,
    s.`leg2_kind`, s.`leg2_ins_code`, s.`leg2_symbol`, s.`leg2_side`, s.`leg2_option_type`,
    s.`leg2_strike_rial`, s.`leg2_entry_price_rial`, s.`leg2_contract_size`,
    s.`unit_capital_rial`, s.`unit_max_loss_rial`, s.`unit_max_profit_rial`,
    s.`breakeven_low_rial`, s.`breakeven_high_rial`, s.`reward_risk_ratio`,
    s.`executable_units`, s.`risk_budget_rial`, s.`recommended_units`,
    s.`recommended_risk_rial`, s.`recommended_capital_rial`, s.`liquidity_score`,
    s.`strategy_score`, s.`final_signal`, s.`signal_reason`, s.`is_current`,
    s.`executed_on_paper_account`, s.`paper_execution_status`,
    s.`paper_execution_reason_code`, s.`paper_execution_reason`,
    s.`paper_execution_checked_at`, s.`paper_executed_at`, s.`opened_position_id`,
    p.`status` AS `paper_position_status`,
    p.`units` AS `paper_executed_units`,
    p.`entry_capital_rial` AS `paper_entry_capital_rial`,
    p.`entry_max_loss_rial` AS `paper_entry_max_loss_rial`,
    p.`opened_at` AS `paper_opened_at`,
    p.`closed_at` AS `paper_closed_at`,
    p.`realized_pnl_rial` AS `paper_realized_pnl_rial`,
    p.`unrealized_pnl_rial` AS `paper_unrealized_pnl_rial`,
    s.`details_json`, s.`created_at`, s.`updated_at`
FROM `protective_put_signals` s
LEFT JOIN `paper_positions` p ON p.`position_id` = s.`opened_position_id`
UNION ALL
SELECT 'bull_call_spread_signals' AS `signal_source_table`,
    s.`signal_id`, s.`signal_key`, s.`signal_date`, s.`scan_time`, s.`last_seen_at`,
    s.`account_id`, s.`strategy_code`, s.`ua_ins_code`, s.`underlying_symbol`,
    s.`expiry_date`, s.`days_to_expiry`, s.`spot_price_rial`,
    s.`leg1_kind`, s.`leg1_ins_code`, s.`leg1_symbol`, s.`leg1_side`, s.`leg1_option_type`,
    s.`leg1_strike_rial`, s.`leg1_entry_price_rial`, s.`leg1_contract_size`,
    s.`leg2_kind`, s.`leg2_ins_code`, s.`leg2_symbol`, s.`leg2_side`, s.`leg2_option_type`,
    s.`leg2_strike_rial`, s.`leg2_entry_price_rial`, s.`leg2_contract_size`,
    s.`unit_capital_rial`, s.`unit_max_loss_rial`, s.`unit_max_profit_rial`,
    s.`breakeven_low_rial`, s.`breakeven_high_rial`, s.`reward_risk_ratio`,
    s.`executable_units`, s.`risk_budget_rial`, s.`recommended_units`,
    s.`recommended_risk_rial`, s.`recommended_capital_rial`, s.`liquidity_score`,
    s.`strategy_score`, s.`final_signal`, s.`signal_reason`, s.`is_current`,
    s.`executed_on_paper_account`, s.`paper_execution_status`,
    s.`paper_execution_reason_code`, s.`paper_execution_reason`,
    s.`paper_execution_checked_at`, s.`paper_executed_at`, s.`opened_position_id`,
    p.`status` AS `paper_position_status`,
    p.`units` AS `paper_executed_units`,
    p.`entry_capital_rial` AS `paper_entry_capital_rial`,
    p.`entry_max_loss_rial` AS `paper_entry_max_loss_rial`,
    p.`opened_at` AS `paper_opened_at`,
    p.`closed_at` AS `paper_closed_at`,
    p.`realized_pnl_rial` AS `paper_realized_pnl_rial`,
    p.`unrealized_pnl_rial` AS `paper_unrealized_pnl_rial`,
    s.`details_json`, s.`created_at`, s.`updated_at`
FROM `bull_call_spread_signals` s
LEFT JOIN `paper_positions` p ON p.`position_id` = s.`opened_position_id`
UNION ALL
SELECT 'bear_put_spread_signals' AS `signal_source_table`,
    s.`signal_id`, s.`signal_key`, s.`signal_date`, s.`scan_time`, s.`last_seen_at`,
    s.`account_id`, s.`strategy_code`, s.`ua_ins_code`, s.`underlying_symbol`,
    s.`expiry_date`, s.`days_to_expiry`, s.`spot_price_rial`,
    s.`leg1_kind`, s.`leg1_ins_code`, s.`leg1_symbol`, s.`leg1_side`, s.`leg1_option_type`,
    s.`leg1_strike_rial`, s.`leg1_entry_price_rial`, s.`leg1_contract_size`,
    s.`leg2_kind`, s.`leg2_ins_code`, s.`leg2_symbol`, s.`leg2_side`, s.`leg2_option_type`,
    s.`leg2_strike_rial`, s.`leg2_entry_price_rial`, s.`leg2_contract_size`,
    s.`unit_capital_rial`, s.`unit_max_loss_rial`, s.`unit_max_profit_rial`,
    s.`breakeven_low_rial`, s.`breakeven_high_rial`, s.`reward_risk_ratio`,
    s.`executable_units`, s.`risk_budget_rial`, s.`recommended_units`,
    s.`recommended_risk_rial`, s.`recommended_capital_rial`, s.`liquidity_score`,
    s.`strategy_score`, s.`final_signal`, s.`signal_reason`, s.`is_current`,
    s.`executed_on_paper_account`, s.`paper_execution_status`,
    s.`paper_execution_reason_code`, s.`paper_execution_reason`,
    s.`paper_execution_checked_at`, s.`paper_executed_at`, s.`opened_position_id`,
    p.`status` AS `paper_position_status`,
    p.`units` AS `paper_executed_units`,
    p.`entry_capital_rial` AS `paper_entry_capital_rial`,
    p.`entry_max_loss_rial` AS `paper_entry_max_loss_rial`,
    p.`opened_at` AS `paper_opened_at`,
    p.`closed_at` AS `paper_closed_at`,
    p.`realized_pnl_rial` AS `paper_realized_pnl_rial`,
    p.`unrealized_pnl_rial` AS `paper_unrealized_pnl_rial`,
    s.`details_json`, s.`created_at`, s.`updated_at`
FROM `bear_put_spread_signals` s
LEFT JOIN `paper_positions` p ON p.`position_id` = s.`opened_position_id`
UNION ALL
SELECT 'long_straddle_signals' AS `signal_source_table`,
    s.`signal_id`, s.`signal_key`, s.`signal_date`, s.`scan_time`, s.`last_seen_at`,
    s.`account_id`, s.`strategy_code`, s.`ua_ins_code`, s.`underlying_symbol`,
    s.`expiry_date`, s.`days_to_expiry`, s.`spot_price_rial`,
    s.`leg1_kind`, s.`leg1_ins_code`, s.`leg1_symbol`, s.`leg1_side`, s.`leg1_option_type`,
    s.`leg1_strike_rial`, s.`leg1_entry_price_rial`, s.`leg1_contract_size`,
    s.`leg2_kind`, s.`leg2_ins_code`, s.`leg2_symbol`, s.`leg2_side`, s.`leg2_option_type`,
    s.`leg2_strike_rial`, s.`leg2_entry_price_rial`, s.`leg2_contract_size`,
    s.`unit_capital_rial`, s.`unit_max_loss_rial`, s.`unit_max_profit_rial`,
    s.`breakeven_low_rial`, s.`breakeven_high_rial`, s.`reward_risk_ratio`,
    s.`executable_units`, s.`risk_budget_rial`, s.`recommended_units`,
    s.`recommended_risk_rial`, s.`recommended_capital_rial`, s.`liquidity_score`,
    s.`strategy_score`, s.`final_signal`, s.`signal_reason`, s.`is_current`,
    s.`executed_on_paper_account`, s.`paper_execution_status`,
    s.`paper_execution_reason_code`, s.`paper_execution_reason`,
    s.`paper_execution_checked_at`, s.`paper_executed_at`, s.`opened_position_id`,
    p.`status` AS `paper_position_status`,
    p.`units` AS `paper_executed_units`,
    p.`entry_capital_rial` AS `paper_entry_capital_rial`,
    p.`entry_max_loss_rial` AS `paper_entry_max_loss_rial`,
    p.`opened_at` AS `paper_opened_at`,
    p.`closed_at` AS `paper_closed_at`,
    p.`realized_pnl_rial` AS `paper_realized_pnl_rial`,
    p.`unrealized_pnl_rial` AS `paper_unrealized_pnl_rial`,
    s.`details_json`, s.`created_at`, s.`updated_at`
FROM `long_straddle_signals` s
LEFT JOIN `paper_positions` p ON p.`position_id` = s.`opened_position_id`;

DROP VIEW IF EXISTS `vw_paper_trade_monitor`;
CREATE VIEW `vw_paper_trade_monitor` AS
SELECT
    `position_id`, `strategy_code`, `underlying_symbol`, `status`, `units`,
    `opened_at`, `expiry_date`,
    CAST(`entry_risk_budget_rial` / 10.0 AS DECIMAL(26,2)) AS `risk_budget_toman`,
    CAST(`entry_max_loss_rial` / 10.0 AS DECIMAL(26,2)) AS `entry_max_loss_toman`,
    CAST(`entry_capital_rial` / 10.0 AS DECIMAL(26,2)) AS `capital_involved_toman`,
    CAST(`unrealized_pnl_rial` / 10.0 AS DECIMAL(26,2)) AS `unrealized_pnl_toman`,
    CAST(`realized_pnl_rial` / 10.0 AS DECIMAL(26,2)) AS `realized_pnl_toman`,
    `return_on_risk_pct`, `current_underlying_price_rial`,
    CAST(`max_favorable_pnl_rial` / 10.0 AS DECIMAL(26,2)) AS `max_favorable_pnl_toman`,
    CAST(`max_adverse_pnl_rial` / 10.0 AS DECIMAL(26,2)) AS `max_adverse_pnl_toman`,
    `last_valued_at`, `closed_at`, `close_reason`
FROM `paper_positions`;

DROP VIEW IF EXISTS `vw_paper_strategy_performance`;
CREATE VIEW `vw_paper_strategy_performance` AS
SELECT
    `strategy_code`,
    COUNT(*) AS `total_positions`,
    SUM(CASE WHEN `status`='OPEN' THEN 1 ELSE 0 END) AS `open_positions`,
    SUM(CASE WHEN `status`<>'OPEN' THEN 1 ELSE 0 END) AS `closed_positions`,
    SUM(CASE WHEN `status`<>'OPEN' AND `realized_pnl_rial`>0 THEN 1 ELSE 0 END) AS `winning_positions`,
    CAST(COALESCE(SUM(CASE WHEN `status`<>'OPEN' THEN `realized_pnl_rial` ELSE 0 END),0)/10.0 AS DECIMAL(26,2)) AS `realized_pnl_toman`,
    CAST(COALESCE(SUM(CASE WHEN `status`='OPEN' THEN `unrealized_pnl_rial` ELSE 0 END),0)/10.0 AS DECIMAL(26,2)) AS `unrealized_pnl_toman`,
    CAST(AVG(CASE WHEN `status`<>'OPEN' THEN `return_on_risk_pct` END) AS DECIMAL(18,6)) AS `avg_closed_return_on_risk_pct`
FROM `paper_positions`
GROUP BY `strategy_code`;

-- ============================================================================
-- SECTION 6 - Verification
-- ============================================================================

SELECT 'ReyT schema bootstrap completed' AS `message`;
SHOW FULL TABLES FROM `ghazali1_ReyTOption`;
