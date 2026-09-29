#!/usr/bin/env bash
set -euo pipefail

cd /opt/reyt/bale

sudo -u reyt env   HOME=/var/lib/reyt   OPTIONS_CONFIG_FILE=/etc/reyt/bale/settings.ini   TELEGRAM_CONFIG_FILE=/etc/reyt/telegram/settings.ini   TELEGRAM_CFW_SESSION=/var/lib/reyt/telegram/cfw.session   PYTHONPATH=/opt/reyt/bale   PYTHONUNBUFFERED=1   /opt/reyt/venv/bin/python   /opt/reyt/bale/ReyT_telegram_primary_notifier.py --test-message
