# ReyT Short Premium — Short Straddle / Short Strangle

این ماژول به‌صورت مستقل کنار Runtime فعلی ReyT اضافه می‌شود و به ساختار Paper Trading قبلی دست نمی‌زند.

## جداسازی

- Collector همان Collector فعلی است و فقط داده بازار را تأمین می‌کند.
- Engine جدید: `strategy/ReyT_short_premium_engine.py`
- Notifier جدید: `bale/ReyT_short_premium_telegram_notifier.py`
- حساب‌ها:
  - `paper_100m_short_straddle`
  - `paper_100m_short_strangle`
- Signalها:
  - `short_straddle_signals`
  - `short_strangle_signals`
- سرویس‌ها:
  - `reyt-short-premium.service`
  - `reyt-short-premium-notifier.service`

هیچ جدول یا سرویس Covered Call / Protective Put توسط Migration جدید ALTER/DROP نمی‌شود.

## قواعد قفل‌شده

- Universe: شستا، شپنا، وبملت، اهرم، فملی
- DTE: ۵ تا ۳۰ روز تقویمی
- سرمایه هر Paper Account: ۱۰۰ میلیون تومان
- Target سرمایه: ۷۰٪ Entry / ۳۰٪ Adjustment بر مبنای Equity تحقق‌یافته
- حداقل ارزش Premium قابل اجرای Entry: ۱ میلیون تومان
- سقف Margin پوزیشن: ۵ میلیون تومان
- حداقل Net Premium / Margin: ۱۰٪
- Stress:
  - Short Straddle: ±۲۰٪
  - Short Strangle: ±۱۰٪
- Trigger Adjustment: ورود ۵٪ به ناحیه زیان نسبت به BE مربوط
- Entry/Adjustment: Best Bid Level 1
- Close عادی/Forced: Best Ask Level 1
- Scheduled Exit روز معاملاتی قبل از سررسید از ساعت ۱۲:۰۰:
  - Ask Level 1
  - در صورت کمبود Ask Level 2
  - اگر باز هم کامل نشد، Position در EXITING می‌ماند و روز سررسید از 09:15 ادامه می‌دهد.
- Engine: Snapshot پنج‌دقیقه‌ای 09:15 تا 12:30
- Entry جدید بعد از 12:15 ممنوع

## نصب روی VPS

از ریشه Repo:

    sudo bash tools/install_short_premium.sh

فایل زیر ساخته می‌شود ولی اگر قبلاً وجود داشته باشد overwrite نمی‌شود:

    /etc/reyt/short-premium/settings.ini

در آن Secretهای لازم را وارد کنید:

- MYSQL_PASSWORD
- BOT_TOKEN بات تلگرام جدید
- CHAT_ID مقصد بات جدید

سپس:

    sudo bash tools/verify_short_premium.sh

قبل از Start دائمی یک بار Dry Run:

    sudo -u reyt env SHORT_PREMIUM_CONFIG_FILE=/etc/reyt/short-premium/settings.ini \
      /opt/reyt-venv/bin/python \
      /opt/reyt-github/strategy/ReyT_short_premium_engine.py --once

تست بات:

    sudo -u reyt env SHORT_PREMIUM_CONFIG_FILE=/etc/reyt/short-premium/settings.ini \
      /opt/reyt-venv/bin/python \
      /opt/reyt-github/bale/ReyT_short_premium_telegram_notifier.py --test-message

بعد:

    sudo systemctl start reyt-short-premium.service
    sudo systemctl start reyt-short-premium-notifier.service

بررسی:

    systemctl status reyt-short-premium.service
    systemctl status reyt-short-premium-notifier.service
    journalctl -u reyt-short-premium.service -n 100 --no-pager
    journalctl -u reyt-short-premium-notifier.service -n 100 --no-pager

## نکته

تا قبل از اجرای database/02_create_short_premium_isolated.sql هیچ جدول جدیدی در VPS ساخته نمی‌شود. Migration فقط اشیای جدید Short Premium را ایجاد می‌کند.

## اعلان‌های Short Premium — Telegram + Bale

Notifier استرادل/استرانگل از Telegram و Bale پشتیبانی می‌کند.
Telegram طبق تنظیمات فعلی فعال است. Bale اختیاری است و تا زمانی که هر دو مقدار
`BOT_TOKEN` و `CHAT_ID` در بخش `[short_premium_bale]` تنظیم نشده باشند،
غیرفعال می‌ماند و روی ارسال Telegram اثری ندارد.

پس از تنظیم هر دو مقدار Bale، هر پیام Short Premium به هر دو کانال ارسال می‌شود.
خرابی یک کانال مانع تلاش کانال دیگر نمی‌شود.
