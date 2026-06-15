---
name: hyperliquid_hip4_monitor
description: Hyperliquid HIP-4 Outcome Market Monitor + Validator L1 Vote Tracker + Polymarket Arb + LLM Semantic Matching + Performance Analytics. 追蹤 Canonical 預測市場（世界盃/體育/政治）、價格/隱含機率、結算事件、validator 投票狀態、arb 機會偵測、歷史績效分析。適用於交易信號、arb 機會偵測、事件結算追蹤、策略績效回測。
origin: custom
version: 1.0  # Phase D + 6 + 7 (REST-only)
---

# Hyperliquid HIP-4 Monitor + Vote Tracker + Polymarket Arb

## 技術說明

**本 skill 完全繞過 SDK，全部使用 REST API。**
SDK `info.info()` call 已知會失敗，已全面替換為 `requests.post()` 直接 call `https://api.hyperliquid.xyz/info`。

---

## When to Activate

- 用戶要查 **Hyperliquid HIP-4 預測市場**（價格/隱含機率/成交量）
- 用戶要追蹤 **Canonical Outcome Markets**（世界盃、體育賽事、政治事件）
- 用戶要監控 **Validator L1 投票**（新市場上架、結算投票、投票參與率）
- 用戶要做 **arb 分析**（Hyperliquid vs Polymarket 價格比較）
- 用戶要追蹤 **已結算市場**（結算結果、結算時間）
- 用戶要查 **策略績效**（勝率、平均价差、模擬 PnL）
- 用戶要查 **風險狀態**（circuit breaker、連續虧損、每日損失限額）
- 關鍵詞：HIP-4、Hyperliquid、預測市場、Canonical、outcome market、validator vote、World Cup、結算、arb、Polymarket

---

## API Endpoints

| 功能 | Endpoint | 說明 |
|------|----------|------|
| 活躍市場 + 投票 | `POST /info` `{type:"validatorL1Votes"}` | 所有 HIP-4 市場提案/結算投票 |
| 隱含機率/價格 | `POST /info` `{type:"allMids"}` | 所有資產 mid price（含 outcome tokens） |

> 主網：`https://api.hyperliquid.xyz/info`

---

## Skill Wrapper — HIP4Monitor Class

```python
from hyperliquid_hip4_monitor.monitor import HIP4Monitor

monitor = HIP4Monitor(
    arb_threshold=0.05,               # arb 價差閾值（預設 5%）
    workspace="/path/to/workspace",   # 事件/績效檔存放目錄
)
```

### Phase 6: 完整方法列表

| 方法 | 回傳 | 說明 |
|------|------|------|
| `execute(ws_enabled=False)` | `dict` | 執行完整監控（REST 或 WS+REST） |
| `get_summary(result?)` | `str` | Human-readable 摘要（含電路狀態） |
| `health_check()` | `dict` | 所有 endpoint + 內部狀態檢查 |
| `get_validator_l1_votes()` | `list[dict]` | 直接取用 validator votes |
| `get_outcome_prices()` | `dict` | 直接取用 outcome token 價格 |
| **`get_governance_events()`** | `dict` | Phase 6: Canonical 市場生命週期追蹤 |
| **`get_recent_arb_signals(limit=20)`** | `list[dict]` | Phase 6: 最近 arb signals 快取 |
| **`get_performance_metrics()`** | `dict` | Phase 6: 歷史勝率/平均价差/模擬 PnL |
| **`get_risk_status()`** | `dict` | Phase 6: Circuit breaker + 風險狀態 |
| **`get_trading_suggestion()`** | `dict` | Phase 6: 交易建議（無 SDK，純 advisory） |
| **`ingest_settlement_result()`** | `None` | Phase 6: 外部餵入結算結果 |
| **`reset_circuit_breaker()`** | `None` | Phase 6: 手動重置電路 |

---

### `execute()` return value

```json
{
  "timestamp": "2026-06-15T06:47:00+00:00",
  "total_markets": 19,
  "new_markets": [...],
  "newly_settled": [...],
  "arb_signals": [...],
  "outcome_prices_sample": { "#1040": 0.55, "#1041": 0.45 },
  "ws_prices_cached": 866,
  "ws_enabled": false
}
```

### `health_check()` return value

```json
{
  "hyperliquid_api":     { "ok": true, "status_code": 200, "response_ms": 120 },
  "hyperliquid_allMids": { "ok": true, "outcome_tokens": 276 },
  "polymarket_gamma":    { "ok": true, "status_code": 200 },
  "circuit_breaker":     { "ok": true, "broken": false, "consecutive_losses": 0 },
  "canonical_markets":  0,
  "last_heartbeat":      "2026-06-15T06:57:40+00:00",
  "_all_ok": true
}
```

### `get_performance_metrics()` return value

```json
{
  "total_tracked_signals": 12,
  "win_rate": 0.667,
  "average_edge": 0.072,
  "total_simulated_pnl": 3.45,
  "last_updated": "2026-06-15T06:50:00+00:00"
}
```

### `get_trading_suggestion()` return value

```json
{
  "action": "MONITOR",
  "signal": { "hl_market": "Outcome #1710", "diff": 0.06, ... },
  "suggested_size_usd": 12.5,
  "validator_participation": 82.3,
  "circuit_broken": false,
  "timestamp": "2026-06-15T06:47:00+00:00"
}
```

Action values: `EXECUTE_ARB` (≥8% diff + ≥80% participation + breaker OK) | `MONITOR` (≥5%) | `HOLD`

---

## Cron 設定範例

每 3 分鐘偵測新市場/投票變化 + arb 信號：

```json
{
  "name": "HIP-4 Monitor Phase 6",
  "schedule": { "kind": "cron", "expr": "*/3 * * * *" },
  "sessionTarget": "isolated",
  "payload": {
    "kind": "agentTurn",
    "message": "Run HIP-4 Phase 6 monitor. Execute, check summary, report governance events if any new markets or arb signals detected.",
  },
  "delivery": { "mode": "announce", "channel": "telegram" }
}
```

每小時績效回顧：

```json
{
  "name": "HIP-4 Performance Report",
  "schedule": { "kind": "cron", "expr": "0 * * * *" },
  "sessionTarget": "isolated",
  "payload": {
    "kind": "agentTurn",
    "message": "Run HIP-4 monitor.execute(), then print: health_check(), get_performance_metrics(), get_risk_status(), get_governance_events(). Report all results."
  },
  "delivery": { "mode": "announce", "channel": "telegram" }
}
```

---

## 數據來源說明

- **HIP-4** 是 Hyperliquid 的 Canonical Prediction Market 協議
- 市場由 validator L1 governance 創建和結算
- Outcome tokens（如 `#1040`, `#1041`）可在 Hyperliquid 現貨市場交易
- `allMids` 返回的價格就是隱含機率（0-1 之間）
- 投票者地址：`0x000000000056f99d36b6f2e0c51fd41496bbacb8` 等

---

## Phase 6 新增功能

### GovernanceTracker（Canonical Market Lifecycle）
- 自動追蹤每個新發現的 canonical 市場
- 記錄 first_seen、last_prob、pm_prob、diff
- 結算後呼叫 `ingest_settlement_result()` 記錄 actual_outcome

### Performance Analytics
- 所有 arb signal 寫入 `hip4_performance.json`
- 結算後比對 direction 計算 was_profitable
- 提供 win_rate、average_edge、total_simulated_pnl

### Circuit Breaker
- `consecutive_losses >= 3` → circuit_broken = True
- `daily_pnl <= -$200` → circuit_broken = True
- 進入 breaker 後 `get_trading_suggestion()` 強制回 HOLD
- 可呼叫 `reset_circuit_breaker()` 手動重置

### Advisory Position Sizing
- 公式：`min(100, 25 + diff*400) * min(1, participation/100)`
- 純 advisory，不觸發實際交易（無 SDK trading call）

---

## Phase 7: LLM Semantic Matching Module

`SemanticMatcher` 取代脆弱的 keyword matching，改用 LLM 判斷 Hyperliquid vs Polymarket 是否為同一事件。

```python
from hyperliquid_hip4_monitor.monitor import SemanticMatcher

def my_llm(prompt: str) -> str:
    # 呼叫任何 LLM，回傳 JSON 字串
    return '{"is_same_event": true, "confidence": 0.88}'

matcher = SemanticMatcher(
    llm_callable=my_llm,
    cache_ttl_minutes=60,
    confidence_threshold=0.75,
    fallback_to_keyword=True,
)

is_match, confidence = matcher.is_same_event(
    "Outcome #1710",
    "Will BTC exceed $100k by end of 2025?",
)
# → (True, 0.88)
```

### `is_same_event()` 回傳 `(is_match: bool, confidence: float)`
- confidence 低於 threshold 自動駁回（is_match=False）
- LLM 失敗時自動退回 keyword matching
- 結果快取 60 分鐘（避免重複呼叫）

### Arb signal enrichment
每個信號多兩個欄位：
```json
{
  "match_confidence": 0.88,
  "match_method": "llm"
}
```

---

## 依賴

```bash
pip install requests websocket-client
```

零 SDK，依賴 `requests` + `websocket-client` 直接 call REST/WebSocket API。

---

## 檔案結構

```
skills/hyperliquid_hip4_monitor/
├── SKILL.md              ← 本文件
├── monitor.py           ← 完整實作（REST-only, Phase D+6）
└── config.json           ← 設定檔
```

工作區附屬檔案（寫入 workspace 目錄）：
- `hip4_events.jsonl` — 事件日誌（每筆一行）
- `hip4_performance.json` — 歷史訊號 + 結算結果
- `hip4_phase6_state.json` — Canonical 市場追蹤 + 電路狀態
