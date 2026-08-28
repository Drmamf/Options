#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

systemctl stop reyt-bale.service || true
systemctl stop reyt-strategy.service || true
systemctl stop reyt-collector.service || true

install -o reyt -g reyt -m 0644 "$ROOT_DIR/collector/ReyT_collector_unified_optimized.py" \
  /opt/reyt/collector/ReyT_collector_unified_optimized.py
install -o reyt -g reyt -m 0644 "$ROOT_DIR/strategy/ReyT_strategy_engine_unified_1b_execution_status_v2.py" \
  /opt/reyt/strategy/ReyT_strategy_engine_unified_1b_execution_status_v2.py
install -o reyt -g reyt -m 0644 "$ROOT_DIR/bale/ReyT_bale_notifier_unified.py" \
  /opt/reyt/bale/ReyT_bale_notifier_unified.py


install -d -o root -g reyt -m 0750 /opt/reyt/tools
install -o reyt -g reyt -m 0750 "$ROOT_DIR/tools/initialize_paper_account.py" /opt/reyt/strategy/initialize_paper_account.py
for tool in reset_trading_state_preserve_market.sh health_check.sh test_bale.sh send_bale_eod_now.sh; do
  install -o root -g reyt -m 0750 "$ROOT_DIR/tools/$tool" "/opt/reyt/tools/$tool"
done

install -o root -g root -m 0644 "$ROOT_DIR/systemd/reyt-collector.service" /etc/systemd/system/reyt-collector.service
install -o root -g root -m 0644 "$ROOT_DIR/systemd/reyt-strategy.service" /etc/systemd/system/reyt-strategy.service
install -o root -g root -m 0644 "$ROOT_DIR/systemd/reyt-bale.service" /etc/systemd/system/reyt-bale.service
systemctl daemon-reload

echo "FINAL_FILES_INSTALLED"
