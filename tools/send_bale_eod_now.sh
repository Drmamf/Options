#!/usr/bin/env bash
set -euo pipefail
cd /opt/reyt/bale
sudo -u reyt env OPTIONS_CONFIG_FILE=/opt/reyt/bale/settings.ini PYTHONPATH=/opt/reyt/bale PYTHONUNBUFFERED=1 \
  /opt/reyt/venv/bin/python /opt/reyt/bale/ReyT_bale_notifier_unified.py --eod-report
