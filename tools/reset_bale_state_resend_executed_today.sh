#!/usr/bin/env bash
set -euo pipefail

systemctl stop reyt-notifier.service

rm -f   /var/lib/reyt/bale/reyt_bale_notifier_state.sqlite3   /var/lib/reyt/bale/reyt_bale_notifier_state.sqlite3-shm   /var/lib/reyt/bale/reyt_bale_notifier_state.sqlite3-wal   /opt/reyt/bale/reyt_bale_notifier_state.sqlite3   /opt/reyt/bale/reyt_bale_notifier_state.sqlite3-shm   /opt/reyt/bale/reyt_bale_notifier_state.sqlite3-wal

systemctl reset-failed reyt-notifier.service || true
systemctl start reyt-notifier.service
sleep 3

echo "Notifier = $(systemctl is-active reyt-notifier.service)"
journalctl -u reyt-notifier.service --since "2 minutes ago" --no-pager | tail -n 80
