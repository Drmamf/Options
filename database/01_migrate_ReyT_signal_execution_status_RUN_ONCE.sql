-- ReyT Options - RUN ONCE migration: signal paper-account execution tracking
-- Use this only if the previous REBUILT schema already exists in the database.
-- For a clean/new database, use 00_create_all_ReyT_mysql_tables_REBUILT_v2_execution_status.sql instead.

USE `ghazali1_ReyTOption`;
SET NAMES utf8mb4;
SET time_zone = '+03:30';

ALTER TABLE `covered_call_signals`
    ADD COLUMN `executed_on_paper_account` TINYINT(1) NOT NULL DEFAULT 0 AFTER `opened_position_id`,
    ADD COLUMN `paper_execution_status` ENUM('PENDING','EXECUTED','NOT_EXECUTED') NOT NULL DEFAULT 'PENDING' AFTER `executed_on_paper_account`,
    ADD COLUMN `paper_execution_reason_code` VARCHAR(64) NULL AFTER `paper_execution_status`,
    ADD COLUMN `paper_execution_reason` VARCHAR(1000) NULL AFTER `paper_execution_reason_code`,
    ADD COLUMN `paper_execution_checked_at` DATETIME NULL AFTER `paper_execution_reason`,
    ADD COLUMN `paper_executed_at` DATETIME NULL AFTER `paper_execution_checked_at`,
    ADD KEY `ix_covered_call_paper_execution` (`account_id`, `executed_on_paper_account`, `paper_execution_status`, `signal_date`);

ALTER TABLE `protective_put_signals`
    ADD COLUMN `executed_on_paper_account` TINYINT(1) NOT NULL DEFAULT 0 AFTER `opened_position_id`,
    ADD COLUMN `paper_execution_status` ENUM('PENDING','EXECUTED','NOT_EXECUTED') NOT NULL DEFAULT 'PENDING' AFTER `executed_on_paper_account`,
    ADD COLUMN `paper_execution_reason_code` VARCHAR(64) NULL AFTER `paper_execution_status`,
    ADD COLUMN `paper_execution_reason` VARCHAR(1000) NULL AFTER `paper_execution_reason_code`,
    ADD COLUMN `paper_execution_checked_at` DATETIME NULL AFTER `paper_execution_reason`,
    ADD COLUMN `paper_executed_at` DATETIME NULL AFTER `paper_execution_checked_at`,
    ADD KEY `ix_protective_put_paper_execution` (`account_id`, `executed_on_paper_account`, `paper_execution_status`, `signal_date`);

ALTER TABLE `bull_call_spread_signals`
    ADD COLUMN `executed_on_paper_account` TINYINT(1) NOT NULL DEFAULT 0 AFTER `opened_position_id`,
    ADD COLUMN `paper_execution_status` ENUM('PENDING','EXECUTED','NOT_EXECUTED') NOT NULL DEFAULT 'PENDING' AFTER `executed_on_paper_account`,
    ADD COLUMN `paper_execution_reason_code` VARCHAR(64) NULL AFTER `paper_execution_status`,
    ADD COLUMN `paper_execution_reason` VARCHAR(1000) NULL AFTER `paper_execution_reason_code`,
    ADD COLUMN `paper_execution_checked_at` DATETIME NULL AFTER `paper_execution_reason`,
    ADD COLUMN `paper_executed_at` DATETIME NULL AFTER `paper_execution_checked_at`,
    ADD KEY `ix_bull_call_spread_paper_execution` (`account_id`, `executed_on_paper_account`, `paper_execution_status`, `signal_date`);

ALTER TABLE `bear_put_spread_signals`
    ADD COLUMN `executed_on_paper_account` TINYINT(1) NOT NULL DEFAULT 0 AFTER `opened_position_id`,
    ADD COLUMN `paper_execution_status` ENUM('PENDING','EXECUTED','NOT_EXECUTED') NOT NULL DEFAULT 'PENDING' AFTER `executed_on_paper_account`,
    ADD COLUMN `paper_execution_reason_code` VARCHAR(64) NULL AFTER `paper_execution_status`,
    ADD COLUMN `paper_execution_reason` VARCHAR(1000) NULL AFTER `paper_execution_reason_code`,
    ADD COLUMN `paper_execution_checked_at` DATETIME NULL AFTER `paper_execution_reason`,
    ADD COLUMN `paper_executed_at` DATETIME NULL AFTER `paper_execution_checked_at`,
    ADD KEY `ix_bear_put_spread_paper_execution` (`account_id`, `executed_on_paper_account`, `paper_execution_status`, `signal_date`);

ALTER TABLE `long_straddle_signals`
    ADD COLUMN `executed_on_paper_account` TINYINT(1) NOT NULL DEFAULT 0 AFTER `opened_position_id`,
    ADD COLUMN `paper_execution_status` ENUM('PENDING','EXECUTED','NOT_EXECUTED') NOT NULL DEFAULT 'PENDING' AFTER `executed_on_paper_account`,
    ADD COLUMN `paper_execution_reason_code` VARCHAR(64) NULL AFTER `paper_execution_status`,
    ADD COLUMN `paper_execution_reason` VARCHAR(1000) NULL AFTER `paper_execution_reason_code`,
    ADD COLUMN `paper_execution_checked_at` DATETIME NULL AFTER `paper_execution_reason`,
    ADD COLUMN `paper_executed_at` DATETIME NULL AFTER `paper_execution_checked_at`,
    ADD KEY `ix_long_straddle_paper_execution` (`account_id`, `executed_on_paper_account`, `paper_execution_status`, `signal_date`);

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


SELECT 'ReyT signal execution tracking migration completed' AS `message`;
