ReyT FINAL CLEAN build v2 - 2026-08-15

این بسته شامل سورس کامل Collector / Strategy / Bale است؛ patch جداگانه لازم نیست.

تنظیمات کلیدی v2:
- Paper capital = 100M toman
- Max loss/entry = 10M; minimum final Max Loss = 2M
- DTE entry filter = 5..180 days
- Expected-return hurdle = 40% EAR; History AND IV required; recheck after VWAP
- History/realized-volatility lookback ~= one trading year; minimum returns = 90
- Greeks risk-free = 40%
- Entry order-book max age = 150s
- Fees enabled with configured standard schedule
- Collector history = one-year initial bootstrap + one EOD row/instrument at 13:00; pruning OFF

ترتیب پیشنهادی:
1) سرویس‌ها را stop کن.
2) سورس‌های کامل این بسته را جایگزین کن.
3) tools/apply_final_settings.py را اجرا کن تا settings فعلی normalize شوند و secrets حفظ شوند.
4) tools/verify_final_build.sh را اجرا کن.
5) tools/reset_all_data.sh را اجرا کن (همه داده‌ها حذف می‌شوند؛ backup نمی‌گیرد).
6) Collector را start کن و تا Initial one-year history bootstrap complete صبر کن.
7) Strategy را یک بار --once --no-auto-trade اجرا کن و counts را چک کن.
8) Strategy service را start کن.
9) پس از یک scan موفق، Bale service را start کن.


Build v5 portfolio rule:
- Paper capital: 1,000,000,000 toman.
- Max loss per entry: 10,000,000 toman.
- Max OPEN allocation per strategy: 30% of initial equity = 300,000,000 toman.
- Existing OPEN positions count toward the cap; closing them releases capacity.
- No database schema migration is required for this rule.

v6 note: EOD scheduler timezone mismatch fixed. Use v6 instead of v5 for final deployment.
