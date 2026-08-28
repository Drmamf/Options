#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

bash "$ROOT_DIR/tools/install_final_files.sh"
/opt/reyt/venv/bin/python "$ROOT_DIR/tools/apply_final_settings.py"
bash "$ROOT_DIR/tools/verify_final_build.sh"

systemctl enable reyt-collector.service reyt-strategy.service reyt-bale.service
systemctl restart reyt-collector.service reyt-strategy.service reyt-bale.service
sleep 3

echo "DEPLOY_FINAL_OK"
for s in reyt-collector reyt-strategy reyt-bale; do
  echo "$s: active=$(systemctl is-active "$s.service" || true), enabled=$(systemctl is-enabled "$s.service" || true)"
done
