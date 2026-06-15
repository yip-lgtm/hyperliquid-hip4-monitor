---
name: ote_rr25_signal
description: "OTE RR≥2.5 signal generator with $100 daily kill-switch. TradingView webhook triggered. MNQ only."
metadata:
  timezone: US/Eastern
  kill_switch_usd: 100
  max_signals_per_day: 2
  min_rr: 2.5
  max_risk_usd: 80
---

# OTE RR25 Signal Bot

只出 RR ≥ 2.5 的 OTE signal，機械、冷靜、保護帳戶。

## Trigger

TradingView alert webhook → Flask endpoint → skill 邏輯

## Signal 條件（全部滿足先出）

- Daily Bias 明確（bullish / bearish）
- Price 進入 OTE zone（0.705–0.79）
- LTF rejection + MSS 確認
- RR ≥ 2.5
- 單筆 risk ≤ $80
- Kill zone（London/NY）
- 當日累計 loss < $90
- 今日 signal count < 2

## 輸出格式

```json
{
  "type": "OTE_RR25",
  "direction": "LONG/SHORT",
  "entry": 19845.50,
  "sl": 19812.00,
  "target1": 19912.00,
  "target2": 19945.00,
  "rr": 2.68,
  "risk_usd": 67.00,
  "size": 2,
  "note": "London open | Daily Bias Bullish | Only high RR OTE"
}
```

## 安全機制

- `$90+ loss` → 停止當日所有 signal
- `signal_count ≥ 2` → 停止當日
- `total_pnl <= -90` → kill-switch 觸發

## 使用方式

1. 確認 `artifacts/daily_pnl.json` 存在
2. TradingView alert 發 webhook 到 `/webhook/ote`
3. Flask server 接收 → 寫入 queue → OpenClaw cron 處理
4. 合格 signal 發 Telegram

## 測試模式

`TEST_MODE=true` 時只 log，不發送 Telegram，結果寫 `artifacts/ote_test_log.json`