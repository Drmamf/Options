ReyT FINAL LOCKED — 2026-08-16
===============================

این آرشیو مرجع نهایی Deployment پروژه ReyT است و تنظیمات توافق‌شده نهایی را در خود نگه می‌دارد.
برای مشاهده یکجا‌ی تمام مقادیر نهایی ابتدا فایل FINAL_RUNTIME_PROFILE.txt را بخوانید.

اجزای اصلی:
- collector/ReyT_collector_unified_optimized.py
- strategy/ReyT_strategy_engine_unified_1b_execution_status_v2.py
- bale/ReyT_bale_notifier_unified.py
- database/ اسکریپت ساخت جداول و migration
- systemd/ سه سرویس دائمی
- config/FINAL_SETTINGS_REFERENCE.ini مرجع تنظیمات بدون Secret
- tools/ نصب، deploy، verify، reset، health-check، dry-run و تست Bale

تنظیمات قفل‌شده نهایی:
- Paper capital: 1,000,000,000 تومان
- Min final Max Loss هر معامله جدید: 2,000,000 تومان
- Max final Max Loss هر معامله جدید: 10,000,000 تومان
- Max OPEN allocation per strategy: 30% = 300,000,000 تومان
- DTE filter: 5..180
- Score floor: OFF
- Expected Return: History AND IV > hurdle معادل 40% effective annual
- Post-VWAP History/IV recheck: ON
- 5-level VWAP execution
- Entry book max age: 150s
- Mark book max age: 900s
- History context: 252 rows; minimum returns: 90; no pruning
- Greeks risk-free: 40%
- Bale immediate notifications: EXECUTED only
- NOT_EXECUTED: فقط DB/CSV/EOD، بدون Push فوری
- 08:30 snapshot و 13:00 snapshot + CSV

Deploy روی VPS موجود:
  cd <extracted_package>
  chmod +x tools/*.sh
  bash tools/deploy_final.sh

این Deploy:
- سورس‌ها و systemd را نصب می‌کند
- Secretهای موجود settings.ini را حفظ می‌کند
- تمام تنظیمات نهایی را اعمال می‌کند
- verify را اجرا می‌کند
- هر سه سرویس را enable + restart می‌کند

Reset پیشنهادی بدون حذف History/Market Data:
  sudo /opt/reyt/tools/reset_trading_state_preserve_market.sh

Health check:
  sudo /opt/reyt/tools/health_check.sh

Bale connectivity test:
  sudo /opt/reyt/tools/test_bale.sh

Secrets عمداً داخل ZIP نیستند:
MYSQL_PASSWORD / BALE_BOT_TOKEN / BALE_CHAT_ID
روی VPS موجود توسط apply_final_settings.py حفظ می‌شوند؛ روی VPS تازه باید دستی وارد شوند.
