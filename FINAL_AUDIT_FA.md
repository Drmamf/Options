# ممیزی نهایی ReyT — FINAL LOCKED — 2026-08-16

## 1) حساب Paper و Sizing
- نام حساب: `paper_1b_toman`
- سرمایه اولیه: **1,000,000,000 تومان**
- سقف Max Loss هر ورود: **10,000,000 تومان** (حداکثر 1% سرمایه اولیه در هر ورود)
- کف Max Loss اجرای نهایی: **2,000,000 تومان**
- سقف تخصیص هر استراتژی: **30% سرمایه اولیه = 300,000,000 تومان** برای مجموع پوزیشن‌های OPEN همان strategy.
- سقف 30% بر اساس `entry_capital_rial` پوزیشن‌های OPEN اعمال می‌شود؛ با بسته‌شدن پوزیشن، ظرفیت آزاد می‌شود.
- اگر معامله بعدی از ظرفیت باقی‌مانده 30% بزرگ‌تر باشد، موتور تا حد ممکن تعداد units را کاهش می‌دهد؛ اگر حتی یک unit جا نشود، با `STRATEGY_ALLOCATION_CAP_REACHED` رد می‌شود.
- سقف تعداد پوزیشن / روز / نماد / ریسک کل: **خاموش**؛ محدودیت‌های تجمعی Cash آزاد + cap 30% هر strategy هستند.
- سایز نهایی: کمینه‌ی ظرفیت مشترک 5-level order book، سقف ریسک 10M، Cash قابل استفاده، و ظرفیت باقی‌مانده 30% strategy.
- Partial fill نامتقارن بین لگ‌ها ممنوع؛ نسبت لگ‌ها حفظ می‌شود.

## 2) Signal filters
- Score floor: **خاموش**؛ Score فقط ranking/context است.
- DTE filter: **روشن**؛ `5 <= DTE <= 180`.
- Expected-return hurdle: **40% effective annual**.
- History model و IV model هر دو باید PASS شوند.
- بعد از محاسبه VWAP واقعی 5-level، هر دو Expected Return دوباره باید PASS شوند.
- Order book ورود: حداکثر سن **150 ثانیه**.
- Order book mark/valuation: حداکثر سن **900 ثانیه**.
- Signal key: strategy + underlying + legs + Tehran trading date؛ refresh جدید رکورد روزانه جدید نمی‌سازد.

## 3) History / Volatility / IV
- Initial collector history: **252 trading-session rows requested once** (تقریباً یک سال معاملاتی).
- هیچ history pruning انجام نمی‌شود؛ آرشیو اولیه دائمی است و بعداً رشد می‌کند.
- هر روز بازار ساعت **13:00 تهران**، Collector فقط حداکثر **یک ردیف جدید روزانه برای هر instrument** از آخرین روز معاملاتی تکمیل‌شده append/UPSERT می‌کند.
- in-market 7-day history refresh حذف شده است.
- Expected-return history uses latest **252 daily price rows**.
- Minimum expected-return historical returns: **90**.
- Historical/realized volatility lookback: **252 returns/days context**.
- Minimum volatility returns: **90**.
- IV model uses **current option implied volatility**; IV is point-in-time and historical IV is not retroactively available in the current database.
- Trading days/year: 252; calendar days/year: 365.

## 4) Greeks / IV solver
- Greeks enabled.
- Risk-free rate: **40% = 0.40**.
- Dividend yield: 0%.
- IV range: 0.000001 to 5.0 (500%).
- Newton max iterations: 30; tolerance: 1e-7.
- Bisection iterations: 60.
- Spread threshold: 10%.

## 5) Fees — active in Capital / Risk / Expected Return / P&L
Configured standard rates (percentage points):
- Underlying buy: **0.3712%**
- Underlying sell: **0.88%**
- Option buy: **0.103%**
- Option sell: **0.103%**
- Option expiry/exercise: **0.05% of exercise value**, charged once per exercised option leg.

Implementation note: expiry fee is calculated on strike × contract size (exercise value), not intrinsic payoff, and is not double-counted as both settlement and exercise.

## 6) Collector live data
- API-1 realtime: every 5s.
- API-3 underlying: every 60s.
- API-4 order book: every 15s.
- Order-book batch: 300 instruments.
- Order-book concurrency: 30.
- HTTP connector limit: 60.
- Market hours: 09:00–12:30 Tehran.
- After 12:30 Collector is idle except the single EOD history append at 13:00.

## 7) Strategy-specific rules retained
- Covered Call: existing structural rule retained.
- Protective Put: existing strike/moneyness structural rule retained.
- Bull Call Spread / Bear Put Spread: `PAPER_MAX_STRIKE_STEPS = 4`.
- Long Straddle: existing moneyness rule retained; historical-volatility sample requirement now benefits from minimum 90 returns.

## 8) Bale
- 08:30 start-of-day snapshot on market days.
- Signal polling every 2s; query overlap 5s; batch 2000.
- Push فوری Bale فقط برای `EXECUTED` است؛ هر logical EXECUTED حداکثر یک‌بار در روز معاملاتی تهران ارسال می‌شود. `NOT_EXECUTED` در DB/CSV/EOD باقی می‌ماند ولی Push فوری ندارد.
- 13:00 end-of-day snapshot/CSV remains enabled.

## 9) Important interpretation
At 13:00, “one row append” means at most one newly completed `daily_market_data` row **per instrument**. No historical row is deleted. The strategy calculation still uses only the latest configured 252 rows, while the database archive grows over time.

## Shutdown / systemd hardening

- Collector scheduler sleeps are interruptible on SIGTERM/SIGINT.
- All three systemd units use `TimeoutStopSec=90` as a safety margin for in-flight HTTP/DB work.
- This addresses the observed old-service stop timeout where systemd sent SIGKILL after the previous 30-second limit.

## v6 — EOD scheduler datetime fix
- `tehran_now()` در Collector عمداً datetime تهران را به‌صورت naive برای سازگاری با MySQL DATETIME برمی‌گرداند.
- `wait_until_eod()` نیز target ساعت 13:00 را اکنون به‌صورت naive می‌سازد؛ بنابراین تفریق target-now بعد از بسته‌شدن بازار خطای offset-naive/offset-aware نمی‌دهد.
- این تغییر فقط scheduler بعد از بازار را اصلاح می‌کند و منطق Live, History, Greeks, Strategy, Bale و محدودیت 30% را تغییر نمی‌دهد.

## 11) Final operational hardening
- Strategy watch initializes/verifies the paper account once at service startup even outside market hours; this performs no strategy scan or paper trade.
- Recommended reset is `tools/reset_trading_state_preserve_market.sh`: it clears only paper/signal state, preserves validated 252-row market history, clears Bale SQLite state, and recreates the configured paper account.
- `tools/health_check.sh`, `tools/test_bale.sh`, `tools/send_bale_eod_now.sh`, and `tools/deploy_final.sh` are bundled for repeatable VPS operations.
- `tools/reset_all_data.sh` remains destructive and is only for an intentional full market/history bootstrap from zero.


Bale v8: immediate signal notifications are EXECUTED-only. NOT_EXECUTED rows remain in MySQL/CSV/EOD reports and are not sent as immediate messages. Resetting only the Bale SQLite state will resend today's EXECUTED signals once; never delete the Python source to reset notifications.
