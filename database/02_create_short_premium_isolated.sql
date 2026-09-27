-- ============================================================================
-- ReyT Short Premium - isolated additive schema
-- Strategies: SHORT_STRADDLE / SHORT_STRANGLE
--
-- IMPORTANT:
--   * Additive migration only.
--   * Does NOT ALTER/DROP any existing ReyT table or view.
--   * Existing Covered Call / Protective Put runtime remains untouched.
-- ============================================================================

USE ghazali1_ReyTOption;
SET NAMES utf8mb4;
SET time_zone = '+03:30';

CREATE TABLE IF NOT EXISTS short_premium_accounts (
    account_id INT UNSIGNED NOT NULL AUTO_INCREMENT,
    account_name VARCHAR(100) NOT NULL,
    strategy_code ENUM('SHORT_STRADDLE','SHORT_STRANGLE') NOT NULL,
    currency VARCHAR(10) NOT NULL DEFAULT 'IRR',
    initial_equity_rial DECIMAL(26,4) NOT NULL,
    realized_pnl_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    unrealized_pnl_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    current_equity_rial DECIMAL(26,4) NOT NULL,
    entry_bucket_target_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    adjustment_bucket_target_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    entry_capital_used_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    adjustment_capital_used_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    open_positions_count INT UNSIGNED NOT NULL DEFAULT 0,
    high_watermark_rial DECIMAL(26,4) NOT NULL,
    drawdown_pct DECIMAL(18,6) NOT NULL DEFAULT 0,
    last_scan_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (account_id),
    UNIQUE KEY uq_short_premium_account_name (account_name),
    UNIQUE KEY uq_short_premium_strategy_account (strategy_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS short_premium_positions (
    position_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    account_id INT UNSIGNED NOT NULL,
    strategy_code ENUM('SHORT_STRADDLE','SHORT_STRANGLE') NOT NULL,
    ua_ins_code VARCHAR(30) NOT NULL,
    underlying_symbol VARCHAR(100) NOT NULL,
    expiry_date DATE NOT NULL,
    status ENUM('OPEN','EXITING','CLOSED') NOT NULL DEFAULT 'OPEN',
    exit_mode ENUM('FORCED','SCHEDULED') NULL,
    initial_spot_rial DECIMAL(20,4) NOT NULL,
    initial_qty INT UNSIGNED NOT NULL,
    initial_gross_premium_rial DECIMAL(26,4) NOT NULL,
    initial_net_premium_rial DECIMAL(26,4) NOT NULL,
    initial_fees_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    entry_margin_allocated_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    adjustment_margin_allocated_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    current_margin_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    cumulative_sell_gross_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    cumulative_buy_gross_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    total_fees_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    cumulative_net_cashflow_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    lower_breakeven_rial DECIMAL(20,4) NULL,
    upper_breakeven_rial DECIMAL(20,4) NULL,
    current_underlying_price_rial DECIMAL(20,4) NULL,
    current_close_cost_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    gross_pnl_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    net_pnl_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    opened_at DATETIME NOT NULL,
    last_valued_at DATETIME NULL,
    exit_started_at DATETIME NULL,
    forced_exit_date DATE NULL,
    closed_at DATETIME NULL,
    close_reason VARCHAR(1000) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (position_id),
    KEY ix_short_premium_position_account_status (account_id,status,opened_at),
    KEY ix_short_premium_position_underlying_expiry (account_id,ua_ins_code,expiry_date,status),
    KEY ix_short_premium_position_strategy_status (strategy_code,status),
    CONSTRAINT fk_short_premium_position_account
      FOREIGN KEY (account_id) REFERENCES short_premium_accounts(account_id)
      ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS short_premium_legs (
    leg_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    position_id BIGINT UNSIGNED NOT NULL,
    role ENUM('INITIAL','ADJUSTMENT') NOT NULL,
    option_type ENUM('CALL','PUT') NOT NULL,
    ins_code VARCHAR(30) NOT NULL,
    symbol VARCHAR(100) NULL,
    strike_price_rial DECIMAL(20,4) NOT NULL,
    expiry_date DATE NOT NULL,
    contract_size INT UNSIGNED NOT NULL,
    opened_contracts INT UNSIGNED NOT NULL,
    remaining_contracts INT UNSIGNED NOT NULL,
    entry_price_rial DECIMAL(20,4) NOT NULL,
    entry_gross_rial DECIMAL(26,4) NOT NULL,
    entry_fee_rial DECIMAL(26,4) NOT NULL,
    entry_net_credit_rial DECIMAL(26,4) NOT NULL,
    margin_allocated_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    opened_at DATETIME NOT NULL,
    closed_at DATETIME NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (leg_id),
    KEY ix_short_premium_leg_position_open (position_id,remaining_contracts),
    KEY ix_short_premium_leg_instrument (ins_code),
    CONSTRAINT fk_short_premium_leg_position
      FOREIGN KEY (position_id) REFERENCES short_premium_positions(position_id)
      ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS short_premium_fills (
    fill_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    position_id BIGINT UNSIGNED NOT NULL,
    leg_id BIGINT UNSIGNED NULL,
    action ENUM('SELL','BUY_TO_CLOSE') NOT NULL,
    reason ENUM('ENTRY','ADJUSTMENT','FORCED_EXIT','SCHEDULED_EXIT') NOT NULL,
    book_level TINYINT UNSIGNED NOT NULL DEFAULT 1,
    contract_count INT UNSIGNED NOT NULL,
    price_rial DECIMAL(20,4) NOT NULL,
    gross_value_rial DECIMAL(26,4) NOT NULL,
    fee_rial DECIMAL(26,4) NOT NULL,
    net_cashflow_rial DECIMAL(26,4) NOT NULL,
    fill_time DATETIME NOT NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fill_id),
    KEY ix_short_premium_fill_position_time (position_id,fill_time),
    KEY ix_short_premium_fill_leg (leg_id),
    CONSTRAINT fk_short_premium_fill_position
      FOREIGN KEY (position_id) REFERENCES short_premium_positions(position_id)
      ON UPDATE CASCADE ON DELETE CASCADE,
    CONSTRAINT fk_short_premium_fill_leg
      FOREIGN KEY (leg_id) REFERENCES short_premium_legs(leg_id)
      ON UPDATE CASCADE ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS short_premium_valuations (
    valuation_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    position_id BIGINT UNSIGNED NOT NULL,
    valuation_time DATETIME NOT NULL,
    spot_rial DECIMAL(20,4) NOT NULL,
    lower_breakeven_rial DECIMAL(20,4) NULL,
    upper_breakeven_rial DECIMAL(20,4) NULL,
    loss_distance_pct DECIMAL(18,6) NOT NULL DEFAULT 0,
    current_margin_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    gross_pnl_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    net_pnl_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    close_cost_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    entry_capital_used_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    adjustment_capital_used_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    state_json JSON NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (valuation_id),
    UNIQUE KEY uq_short_premium_valuation_time (position_id,valuation_time),
    KEY ix_short_premium_valuation_time (valuation_time),
    CONSTRAINT fk_short_premium_valuation_position
      FOREIGN KEY (position_id) REFERENCES short_premium_positions(position_id)
      ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS short_premium_events (
    event_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    account_id INT UNSIGNED NOT NULL,
    position_id BIGINT UNSIGNED NULL,
    strategy_code ENUM('SHORT_STRADDLE','SHORT_STRANGLE') NOT NULL,
    event_type ENUM(
      'ENTRY','ADJUSTMENT','FORCED_EXIT_START','SCHEDULED_EXIT_START',
      'PARTIAL_EXIT','POSITION_CLOSED'
    ) NOT NULL,
    event_time DATETIME NOT NULL,
    details_json JSON NOT NULL,
    notified_at DATETIME NULL,
    notification_error VARCHAR(1000) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (event_id),
    KEY ix_short_premium_events_notify (notified_at,event_id),
    KEY ix_short_premium_events_position (position_id,event_time),
    CONSTRAINT fk_short_premium_event_account
      FOREIGN KEY (account_id) REFERENCES short_premium_accounts(account_id)
      ON UPDATE CASCADE ON DELETE RESTRICT,
    CONSTRAINT fk_short_premium_event_position
      FOREIGN KEY (position_id) REFERENCES short_premium_positions(position_id)
      ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS short_premium_engine_runs (
    run_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    started_at DATETIME NOT NULL,
    finished_at DATETIME NULL,
    status ENUM('RUNNING','SUCCESS','FAILED','SKIPPED') NOT NULL DEFAULT 'RUNNING',
    positions_opened INT UNSIGNED NOT NULL DEFAULT 0,
    adjustments_executed INT UNSIGNED NOT NULL DEFAULT 0,
    positions_closed INT UNSIGNED NOT NULL DEFAULT 0,
    error_message VARCHAR(2000) NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id),
    KEY ix_short_premium_run_time (started_at,status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS short_straddle_signals (
    signal_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    scan_time DATETIME NOT NULL,
    account_id INT UNSIGNED NOT NULL,
    ua_ins_code VARCHAR(30) NOT NULL,
    underlying_symbol VARCHAR(100) NOT NULL,
    expiry_date DATE NOT NULL,
    days_to_expiry INT NOT NULL,
    spot_rial DECIMAL(20,4) NOT NULL,
    put_ins_code VARCHAR(30) NOT NULL,
    put_symbol VARCHAR(100) NULL,
    put_strike_rial DECIMAL(20,4) NOT NULL,
    put_bid_rial DECIMAL(20,4) NULL,
    put_bid_volume BIGINT UNSIGNED NULL,
    call_ins_code VARCHAR(30) NOT NULL,
    call_symbol VARCHAR(100) NULL,
    call_strike_rial DECIMAL(20,4) NOT NULL,
    call_bid_rial DECIMAL(20,4) NULL,
    call_bid_volume BIGINT UNSIGNED NULL,
    requested_qty INT UNSIGNED NOT NULL DEFAULT 0,
    executable_qty INT UNSIGNED NOT NULL DEFAULT 0,
    gross_premium_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    sell_fees_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    net_premium_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    required_margin_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    net_premium_margin_pct DECIMAL(18,6) NOT NULL DEFAULT 0,
    position_value_rial DECIMAL(26,4) NOT NULL DEFAULT 0,
    stress_down_spot_rial DECIMAL(20,4) NULL,
    stress_up_spot_rial DECIMAL(20,4) NULL,
    stress_down_pnl_rial DECIMAL(26,4) NULL,
    stress_up_pnl_rial DECIMAL(26,4) NULL,
    lower_breakeven_rial DECIMAL(20,4) NULL,
    upper_breakeven_rial DECIMAL(20,4) NULL,
    decision ENUM('EXECUTED','REJECTED') NOT NULL,
    reason_code VARCHAR(64) NULL,
    opened_position_id BIGINT UNSIGNED NULL,
    details_json JSON NULL,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (signal_id),
    UNIQUE KEY uq_short_straddle_scan_structure (account_id,scan_time,ua_ins_code,expiry_date),
    KEY ix_short_straddle_signal_time (scan_time,decision),
    CONSTRAINT fk_short_straddle_signal_account
      FOREIGN KEY (account_id) REFERENCES short_premium_accounts(account_id)
      ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS short_strangle_signals LIKE short_straddle_signals;

-- LIKE copies indexes but not foreign keys. Add the isolated account FK explicitly.
SET @fk_exists = (
    SELECT COUNT(*)
    FROM information_schema.REFERENTIAL_CONSTRAINTS
    WHERE CONSTRAINT_SCHEMA = DATABASE()
      AND TABLE_NAME = 'short_strangle_signals'
      AND CONSTRAINT_NAME = 'fk_short_strangle_signal_account'
);
SET @sql = IF(
    @fk_exists = 0,
    'ALTER TABLE short_strangle_signals ADD CONSTRAINT fk_short_strangle_signal_account FOREIGN KEY (account_id) REFERENCES short_premium_accounts(account_id) ON UPDATE CASCADE ON DELETE RESTRICT',
    'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Rename indexes inherited through LIKE so operational diagnostics stay obvious.
-- MySQL allows duplicate index names across tables, so this is intentionally optional.

INSERT INTO short_premium_accounts (
    account_name,strategy_code,initial_equity_rial,current_equity_rial,
    entry_bucket_target_rial,adjustment_bucket_target_rial,high_watermark_rial
)
VALUES
    ('paper_100m_short_straddle','SHORT_STRADDLE',1000000000,1000000000,700000000,300000000,1000000000),
    ('paper_100m_short_strangle','SHORT_STRANGLE',1000000000,1000000000,700000000,300000000,1000000000)
ON DUPLICATE KEY UPDATE
    account_name=VALUES(account_name);

SELECT 'SHORT_PREMIUM_SCHEMA_READY' AS status;
