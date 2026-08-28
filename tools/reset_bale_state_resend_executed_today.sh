#!/usr/bin/env bash
set -euo pipefail
STATE=/opt/reyt/bale/reyt_bale_notifier_state.sqlite3
systemctl stop reyt-bale.service
rm -f "$STATE" "$STATE-shm" "$STATE-wal"
systemctl reset-failed reyt-bale.service || true
systemctl start reyt-bale.service
sleep 3
echo "Bale = $(systemctl is-active reyt-bale.service)"
journalctl -u reyt-bale.service --since "2 minutes ago" --no-pager | tail -n 80
