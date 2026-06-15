#!/usr/bin/env python3
"""
Hyperliquid HIP-4 Monitor + Vote Tracker + Polymarket Arb + WebSocket Real-time Prices
Phase D + 6 + 7 整合版（REST-only，SDK 已排除）
"""

import json
import os
import time
import hashlib
import threading
import logging
from datetime import datetime, timezone, date, timedelta
from pathlib import Path
from typing import Optional, Dict, List, Any
from collections import defaultdict

import requests
from eth_account import Account
from eth_account.messages import encode_typed_data

# ── API ──────────────────────────────────────────────────────────────────────

API_BASE = "https://api.hyperliquid.xyz/info"
UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
WS_URL = "wss://api.hyperliquid.xyz/ws"


def _post(payload: dict, timeout: int = 15) -> requests.Response:
    return requests.post(
        API_BASE,
        json=payload,
        headers={"Content-Type": "application/json", "User-Agent": UA},
        timeout=timeout,
    )


# ── Core Data Fetchers (REST) ─────────────────────────────────────────────────

def get_validator_l1_votes() -> list[dict]:
    r = _post({"type": "validatorL1Votes"})
    r.raise_for_status()
    return r.json()


def get_all_mids() -> dict:
    r = _post({"type": "allMids"})
    r.raise_for_status()
    return r.json()


# ── Parsers ─────────────────────────────────────────────────────────────────

def _parse_expire_time(ts: int) -> str:
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M UTC")


def _extract_question_info(action: dict) -> dict:
    o = action.get("O", {})
    if "registerTokensAndQuestion" in o:
        rtq = o["registerTokensAndQuestion"]
        qnd = rtq.get("questionNameAndDescription", [])
        name = qnd[0] if qnd else "Unknown"
        desc = qnd[1] if len(qnd) > 1 else ""
        outcomes = rtq.get("namedOutcomes", [])
        return {
            "type": "new_market",
            "name": name,
            "description": desc[:200],
            "outcomes": [f"{o[0]}" for o in outcomes],
            "outcome_count": len(outcomes),
        }
    if "settleQuestion" in o:
        sq = o["settleQuestion"]
        question_id = sq.get("question", "N/A")
        fractions = sq.get("settleFractionsAndDetails", [])
        results = []
        for fid, detail in fractions:
            frac = detail[0]
            note = detail[1][:100]
            results.append(f"Token#{fid}={frac} ({note})")
        return {
            "type": "settlement",
            "question_id": question_id,
            "results": results,
        }
    return {"type": "unknown", "name": str(o)[:100]}


def parse_market(market: dict) -> dict:
    expire_ts = market.get("expireTime", 0)
    action = market.get("action", {})
    votes = market.get("votes", [])
    quorum = market.get("quorumReached", False)
    info = _extract_question_info(action)
    return {
        "expire_time": _parse_expire_time(expire_ts),
        "expire_ts": expire_ts,
        "market_type": info["type"],
        "name": info.get("name", ""),
        "description": info.get("description", ""),
        "outcomes": info.get("outcomes", []),
        "outcome_count": info.get("outcome_count", 0),
        "question_id": info.get("question_id", ""),
        "settlement_results": info.get("results", []),
        "vote_count": len(votes),
        "voters": votes,
        "quorum_reached": quorum,
        "status": "quorum_reached" if quorum else "pending",
    }


def parse_all_markets(votes_data: list[dict]) -> list[dict]:
    return [parse_market(m) for m in votes_data]


# ── Price Helpers ─────────────────────────────────────────────────────────────

def get_outcome_prices(mids: dict) -> dict:
    prices = {}
    for token, price_str in mids.items():
        if token.startswith("#"):
            try:
                prices[token] = float(price_str)
            except ValueError:
                pass
    return prices


# ── State Diff ────────────────────────────────────────────────────────────────

def compute_market_id(market: dict) -> str:
    key = f"{market['expire_ts']}_{market['name']}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


def detect_new_markets(old_markets: list[dict], new_markets: list[dict]) -> list[dict]:
    old_ids = {compute_market_id(m) for m in old_markets}
    return [m for m in new_markets if compute_market_id(m) not in old_ids]


def detect_settled_markets(old_markets: list[dict], new_markets: list[dict]) -> list[dict]:
    old_pending = {
        compute_market_id(m) for m in old_markets if not m["quorum_reached"]
    }
    return [
        m for m in new_markets
        if compute_market_id(m) in old_pending and m["quorum_reached"]
    ]


# ── State Persistence ─────────────────────────────────────────────────────────

STATE_FILE = Path("/tmp/hip4_state.json")


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {"markets": [], "last_check": None}


def save_state(state: dict):
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2))


# ── WebSocket Real-time Prices ────────────────────────────────────────────────

class WebSocketPriceFeed:
    def __init__(self):
        self.prices: Dict[str, float] = {}
        self._thread: Optional[threading.Thread] = None
        self._running = False

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False

    def _run(self):
        import websocket as ws_lib
        while self._running:
            ws = None
            try:
                ws = ws_lib.WebSocketApp(
                    WS_URL,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                )

                def on_open(ws_conn):
                    ws_conn.send(json.dumps({
                        "method": "subscribe",
                        "subscription": {"type": "allMids"},
                    }))

                ws.on_open = on_open
                ws.run_forever(ping_interval=15, ping_timeout=10)
            except Exception as e:
                print(f"[WS] Error: {e}")
            finally:
                if ws:
                    try:
                        ws.close()
                    except Exception:
                        pass
                if self._running:
                    time.sleep(3)

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get("channel") == "allMids":
                mids = data.get("data", {}).get("mids", {})
                for coin, price in mids.items():
                    try:
                        self.prices[coin] = float(price)
                    except (ValueError, TypeError):
                        pass
        except Exception:
            pass

    def _on_error(self, ws, error):
        pass

    def _on_close(self, ws, *args):
        pass

    def get_price(self, token_id: str) -> Optional[float]:
        return self.prices.get(token_id)


# ── Polymarket Arb ────────────────────────────────────────────────────────────

POLYMARKET_GAMMA_URL = "https://gamma-api.polymarket.com/markets"
EVENT_KEYWORDS = ["fed", "rate", "cpi", "inflation", "btc", "bitcoin", "election", "trump", "will_"]


# ── Phase 7: LLM Semantic Matcher ───────────────────────────────────────────

class SemanticMatcher:
    """
    Phase 7: 可插拔的 LLM 語義比對模組。
    取代脆弱的 keyword matching，改用 LLM 判斷 Hyperliquid vs Polymarket 是否為同一事件。

    Usage:
        matcher = SemanticMatcher(llm_callable=my_llm_function)
        is_match, confidence = matcher.is_same_event("Outcome #1710", "Will BTC exceed $100k by 2025?")
    """

    def __init__(
        self,
        llm_callable=None,          # sync fn: (prompt: str) -> str
        cache_ttl_minutes: int = 60,
        confidence_threshold: float = 0.75,
        fallback_to_keyword: bool = True,
    ):
        self.llm_callable = llm_callable
        self.cache_ttl = timedelta(minutes=cache_ttl_minutes)
        self.confidence_threshold = confidence_threshold
        self.fallback_to_keyword = fallback_to_keyword
        self.cache: Dict[str, tuple] = {}  # key: hash, value: ((is_match, confidence), timestamp)

    def _make_cache_key(self, hl_name: str, pm_question: str) -> str:
        combined = f"{hl_name.lower().strip()}||{pm_question.lower().strip()}"
        return hashlib.md5(combined.encode()).hexdigest()

    def _is_cache_valid(self, timestamp: datetime) -> bool:
        return datetime.now(timezone.utc) - timestamp < self.cache_ttl

    def _keyword_fallback(self, hl_name: str, pm_question: str) -> bool:
        """Keyword fallback when no LLM is available."""
        hl_lower = hl_name.lower()
        pm_lower = (pm_question + " ").lower()
        return any(kw in hl_lower for kw in EVENT_KEYWORDS) and any(
            kw in pm_lower for kw in EVENT_KEYWORDS
        )

    def _build_prompt(self, hl_name: str, pm_question: str) -> str:
        return (
            "You are an expert event-matching assistant for prediction markets.\n\n"
            "Determine if these two markets describe the SAME real-world event:\n\n"
            f"Hyperliquid market name:\n{hl_name}\n\n"
            f"Polymarket question:\n{pm_question}\n\n"
            'Respond ONLY with JSON in this exact format (no extra text):\n'
            '{"is_same_event": true/false, "confidence": 0.0-1.0, "reason": "optional short reason"}'
        )

    def _parse_llm_response(self, response: str) -> tuple:
        try:
            response = response.strip()
            # strip markdown code fences if present
            if response.startswith("```"):
                response = response.split("```")[1]
                if response.startswith("json"):
                    response = response[4:].strip()
            data = json.loads(response)
            is_match = bool(data.get("is_same_event", False))
            confidence = float(data.get("confidence", 0.5))
            confidence = min(max(confidence, 0.0), 1.0)
            return is_match, confidence
        except Exception:
            return False, 0.0

    def is_same_event(self, hl_market_name: str, pm_question: str) -> tuple:
        """
        Returns (is_match: bool, confidence: float).
        Checks cache first, falls back to keyword, then LLM if available.
        """
        cache_key = self._make_cache_key(hl_market_name, pm_question)

        # Cache hit
        if cache_key in self.cache:
            (is_match, confidence), ts = self.cache[cache_key]
            if self._is_cache_valid(ts):
                return is_match, confidence

        # No LLM → keyword fallback
        if not self.llm_callable:
            if self.fallback_to_keyword:
                is_match = self._keyword_fallback(hl_market_name, pm_question)
                confidence = 0.6 if is_match else 0.3
                self.cache[cache_key] = ((is_match, confidence), datetime.now(timezone.utc))
                return is_match, confidence
            return False, 0.0

        # LLM call
        prompt = self._build_prompt(hl_market_name, pm_question)
        try:
            llm_response = self.llm_callable(prompt)
            is_match, confidence = self._parse_llm_response(llm_response)

            # Confidence below threshold → reject even if LLM says match
            if confidence < self.confidence_threshold:
                is_match = False

            self.cache[cache_key] = ((is_match, confidence), datetime.now(timezone.utc))
            return is_match, confidence

        except Exception as e:
            # LLM failure → keyword fallback
            if self.fallback_to_keyword:
                is_match = self._keyword_fallback(hl_market_name, pm_question)
                confidence = 0.55 if is_match else 0.25
                self.cache[cache_key] = ((is_match, confidence), datetime.now(timezone.utc))
                return is_match, confidence
            return False, 0.0

    def clear_cache(self):
        """Clear the entire cache."""
        self.cache.clear()



def fetch_polymarket_markets(limit: int = 200) -> List[Dict]:
    try:
        r = requests.get(
            POLYMARKET_GAMMA_URL,
            params={"active": "true", "closed": "false", "limit": limit},
            timeout=10,
        )
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return []


def find_arb_signals(
    outcome_prices: dict,
    pm_markets: List[Dict],
    ws_feed: WebSocketPriceFeed,
    threshold: float = 0.05,
    semantic_matcher: SemanticMatcher = None,
) -> List[Dict]:
    signals = []
    for token_id, rest_prob in outcome_prices.items():
        ws_prob = ws_feed.get_price(token_id)
        hl_prob = ws_prob if ws_prob is not None else rest_prob
        hl_name = f"Outcome {token_id}"

        for pm in pm_markets:
            pm_question = pm.get("question", "") or ""
            slug = (pm.get("slug") or "").lower()

            # Phase 7: Use SemanticMatcher if provided
            if semantic_matcher is not None:
                is_match, confidence = semantic_matcher.is_same_event(hl_name, pm_question)
                if not is_match:
                    continue
            else:
                # Legacy keyword fallback
                pm_lower = pm_question.lower()
                if not (any(kw in hl_name.lower() for kw in EVENT_KEYWORDS) and
                        any(kw in pm_lower or kw in slug for kw in EVENT_KEYWORDS)):
                    continue
                confidence = 0.6

            try:
                pm_prices = pm.get("outcomePrices", [])
                pm_prob = float(pm_prices[0]) if pm_prices else None
            except (ValueError, TypeError):
                pm_prob = None

            if pm_prob is None:
                continue

            diff = abs(hl_prob - pm_prob)
            if diff >= threshold:
                signals.append({
                    "hl_market": hl_name,
                    "hl_prob": round(hl_prob, 4),
                    "pm_question": pm_question[:80],
                    "pm_prob": round(pm_prob, 4),
                    "diff": round(diff, 4),
                    "direction": "BUY_HL" if hl_prob < pm_prob else "BUY_PM",
                    "price_source": "websocket" if ws_prob else "rest",
                    "match_confidence": round(confidence, 3),  # Phase 7: LLM confidence
                    "match_method": "llm" if semantic_matcher and semantic_matcher.llm_callable else "keyword",
                })
    return signals


# ── Main Monitor ──────────────────────────────────────────────────────────────

def run_monitor(
    arb_threshold: float = 0.05,
    ws_enabled: bool = False,
    semantic_matcher: SemanticMatcher = None,
) -> dict:
    ws_feed = WebSocketPriceFeed()
    if ws_enabled:
        ws_feed.start()
        time.sleep(5)

    votes_raw = get_validator_l1_votes()
    mids = get_all_mids()
    outcome_prices = get_outcome_prices(mids)
    current_markets = parse_all_markets(votes_raw)

    state = load_state()
    old_markets = state.get("markets", [])

    new_markets = detect_new_markets(old_markets, current_markets)
    newly_settled = detect_settled_markets(old_markets, current_markets)

    pm_markets = fetch_polymarket_markets()
    arb_signals = find_arb_signals(outcome_prices, pm_markets, ws_feed, arb_threshold, semantic_matcher)

    save_state({
        "markets": current_markets,
        "last_check": datetime.now(timezone.utc).isoformat(),
    })

    new_market_summaries = [
        {
            "name": m["name"],
            "type": m["market_type"],
            "expire": m["expire_time"],
            "outcomes": m["outcomes"],
            "vote_count": m["vote_count"],
            "quorum": m["quorum_reached"],
        }
        for m in new_markets
    ]

    settled_summaries = [
        {
            "name": m["name"],
            "type": m["market_type"],
            "expire": m["expire_time"],
            "vote_count": m["vote_count"],
            "quorum": m["quorum_reached"],
            "results": m.get("settlement_results", []),
        }
        for m in newly_settled
    ]

    ws_price_count = len(ws_feed.prices)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "total_markets": len(current_markets),
        "new_markets": new_market_summaries,
        "newly_settled": settled_summaries,
        "arb_signals": arb_signals,
        "outcome_prices_sample": dict(list(outcome_prices.items())[:20]),
        "ws_prices_cached": ws_price_count,
        "ws_enabled": ws_enabled,
    }


def generate_summary(result: dict) -> str:
    lines = [f"🟢 HIP-4 Monitor | {result['timestamp'][:19]} UTC"]
    lines.append(f"📊 總市場數: {result['total_markets']}  |  WS 即時價格快取: {result['ws_prices_cached']} 個")

    if result["new_markets"]:
        lines.append(f"\n🆕 新市場 ({len(result['new_markets'])} 個):")
        for m in result["new_markets"]:
            lines.append(
                f"  • {m['name']} ({m['type']})"
                f"  到期:{m['expire']}  投票:{m['vote_count']}人"
                f"  {'✅ quorum' if m['quorum'] else '⏳ pending'}"
            )

    if result["newly_settled"]:
        lines.append(f"\n✅ 剛結算 ({len(result['newly_settled'])} 個):")
        for m in result["newly_settled"]:
            lines.append(
                f"  • {m['name']}  投票:{m['vote_count']}人"
                f"  結果: {m.get('results', ['N/A'])}"
            )

    if result["arb_signals"]:
        lines.append(f"\n💰 Arb Signal ({len(result['arb_signals'])} 個):")
        for a in result["arb_signals"]:
            src = "(WS)" if a.get("price_source") == "websocket" else "(REST)"
            lines.append(
                f"  • {a['pm_question'][:60]}"
                f"  HL:{a['hl_prob']:.1%} vs PM:{a['pm_prob']:.1%}"
                f"  差距:{a['diff']:.1%} → {a['direction']} {src}"
            )

    if not result["new_markets"] and not result["newly_settled"] and not result["arb_signals"]:
        lines.append("\n✅ 無新事件")

    return "\n".join(lines)


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 7: REST-native Trading Engine + Pre-trade Risk Engine
# ═══════════════════════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════════════════════
# Phase 7-B: Hyperliquid REST Order Signer
# ═══════════════════════════════════════════════════════════════════════════════

EXCHANGE_URL = "https://api.hyperliquid.xyz/exchange"


class HyperliquidRestSigner:
    """
    Phase 7-B: Hyperliquid REST order signing using eth_account.
    Uses /exchange endpoint with EIP-712 Ed25519 signing.
    """
    def __init__(self, private_key: str, api_url: str = "https://api.hyperliquid.xyz"):
        self.private_key = private_key
        self.api_url = api_url.rstrip("/")
        self.account = Account.from_key(private_key)
        self.address = self.account.address.lower()

    def _get_nonce(self) -> int:
        return int(time.time() * 1000)

    def _sign_action(self, action: dict, nonce: int) -> str:
        connection_id = self._make_connection_id(action, nonce)
        structured_data = {
            "types": {
                "EIP712Domain": [
                    {"name": "name", "type": "string"},
                    {"name": "version", "type": "string"},
                    {"name": "chainId", "type": "uint256"},
                    {"name": "verifyingContract", "type": "address"},
                ],
                "Agent": [
                    {"name": "source", "type": "string"},
                    {"name": "connectionId", "type": "bytes32"},
                ],
            },
            "primaryType": "Agent",
            "domain": {
                "name": "HyperliquidSignTransaction",
                "version": "1",
                "chainId": 1337,
                "verifyingContract": "0x0000000000000000000000000000000000000000",
            },
            "message": {
                "source": "a",
                "connectionId": connection_id,
            },
        }
        encoded = encode_typed_data(structured_data)
        signed = self.account.sign_message(encoded)
        return signed.signature.hex()

    def _make_connection_id(self, action: dict, nonce: int) -> str:
        action_str = json.dumps(action, sort_keys=True) + str(nonce)
        return "0x" + hashlib.sha256(action_str.encode()).hexdigest()

    def place_order(self, asset: str, is_buy: bool, sz: float, limit_px: float,
                   reduce_only: bool = False, tif: str = "Gtc") -> dict:
        nonce = self._get_nonce()
        action = {
            "type": "order",
            "orders": [{
                "asset": asset,
                "isBuy": is_buy,
                "limitPx": str(limit_px),
                "sz": str(sz),
                "reduceOnly": reduce_only,
                "tif": tif,
            }],
            "grouping": "na",
        }
        signature = self._sign_action(action, nonce)
        payload = {"action": action, "nonce": nonce, "signature": signature}
        try:
            r = requests.post(EXCHANGE_URL, json=payload, timeout=15)
            return r.json()
        except Exception as e:
            return {"status": "error", "message": str(e)}



# ── Phase C: Account State (REST) ───────────────────────────────────────────

def get_account_state(address: str) -> dict:
    """
    Phase C: Fetch account state via Hyperliquid REST API.
    Returns margin summary, withdrawable balance, and open positions.
    """
    try:
        r = requests.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "clearinghouseState", "user": address},
            timeout=10,
        )
        if r.status_code != 200:
            return {"ok": False, "error": f"HTTP {r.status_code}"}
        data = r.json()
        ms = data.get("marginSummary", {})
        return {
            "ok": True,
            "account_value": float(ms.get("accountValue", 0)),
            "total_ntl_pos": float(ms.get("totalNtlPos", 0)),
            "total_raw_usd": float(ms.get("totalRawUsd", 0)),
            "total_margin_used": float(ms.get("totalMarginUsed", 0)),
            "withdrawable": float(data.get("withdrawable", 0)),
            "asset_positions": data.get("assetPositions", []),
            "cross_maintenance_margin": float(data.get("crossMaintenanceMarginUsed", 0)),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_spot_balances(address: str) -> dict:
    """
    Phase C: Fetch spot balances via Hyperliquid REST API.
    Returns token balances (USDC, etc).
    """
    try:
        r = requests.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "spotClearinghouseState", "user": address},
            timeout=10,
        )
        if r.status_code != 200:
            return {"ok": False, "error": f"HTTP {r.status_code}"}
        data = r.json()
        balances = data.get("balances", [])
        return {
            "ok": True,
            "balances": [
                {
                    "coin": b["coin"],
                    "total": float(b["total"]),
                    "hold": float(b["hold"]),
                }
                for b in balances
            ],
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


class RestTradingEngine:
    """
    Phase 7: REST-native Safe Trading Execution.

    Replaces SDK Exchange with pure REST order placement.
    Currently dry_run only — real signing via /exchange endpoint is TODO.

    Features:
      • Multi-layer pre-trade risk checks
      • Smart position sizing
      • dry_run + live modes
      • Event publishing (trade_submitted / trade_rejected / risk_rejected)
    """

    def __init__(self, config: dict = None):
        cfg = config or {}
        self.enable_trading = cfg.get("enable_trading", False)
        self.dry_run = cfg.get("dry_run", True)
        self.max_position_usd = cfg.get("max_position_usd", 100.0)
        self.daily_loss_limit_usd = cfg.get("daily_loss_limit_usd", 200.0)
        self.private_key = cfg.get("private_key") or os.getenv("HL_PRIVATE_KEY")
        self.wallet_address = cfg.get("wallet_address") or os.getenv("HL_WALLET_ADDRESS")

        self.logger = logging.getLogger("RestTradingEngine")
        self._daily_pnl: float = 0.0
        self._consecutive_losses: int = 0
        self._consecutive_losses_limit: int = 3

        # Phase 7-B: REST signer (only init when trading enabled and not dry_run)
        self._signer: Optional[HyperliquidRestSigner] = None
        if self.enable_trading and not self.dry_run and self.private_key:
            try:
                self._signer = HyperliquidRestSigner(self.private_key)
            except Exception as e:
                self.logger.warning(f"REST signer init failed: {e}")

    # ── Risk Engine ───────────────────────────────────────────────────────────


    def _pre_trade_risk_check(self, signal: dict, size_usd: float) -> dict:
        """
        Phase C: Multi-layer risk check including actual account state.
        Fetches live balance from Hyperliquid REST API if wallet_address is set.
        """
        checks = {
            "daily_loss_limit": self._daily_pnl > -self.daily_loss_limit_usd,
            "consecutive_losses": self._consecutive_losses < self._consecutive_losses_limit,
            "position_size": size_usd <= self.max_position_usd,
            "trading_enabled": self.enable_trading,
        }

        # Phase C: Account balance check (only if wallet_address available)
        if self.wallet_address:
            state = get_account_state(self.wallet_address)
            if state.get("ok"):
                withdrawable = state["withdrawable"]
                checks["has_balance"] = withdrawable >= size_usd
                checks["margin_headroom"] = state["account_value"] > 0
            else:
                checks["account_fetch_ok"] = False

        passed = all(checks.values())
        failed_reasons = [k for k, v in checks.items() if not v]
        return {"passed": passed, "failed_reasons": failed_reasons, "checks": checks}


    def calculate_position_size(self, diff: float, participation_rate: float = 80.0) -> float:
        """Smart position sizing."""
        base = min(self.max_position_usd, 30 + diff * 350)
        confidence = min(1.0, participation_rate / 100)
        return round(base * confidence, 2)


    def record_trade_result(self, status: str):
        """Update P&L and loss streak after a trade."""
        if status in ("error", "risk_rejected"):
            self._consecutive_losses += 1
        else:
            self._consecutive_losses = 0

    # ── Order Execution ───────────────────────────────────────────────────────


    def execute_arb_trade(
        self,
        signal: dict,
        suggested_size_usd: float = None,
        event_logger = None,
    ) -> dict:
        """
        Execute arb trade (sync, not async as in Saba's draft —
        keeps everything in the same sync pattern as the rest of the skill).
        """
        size_usd = (
            suggested_size_usd
            or self.calculate_position_size(signal["diff"], 80.0)
        )

        risk = self._pre_trade_risk_check(signal, size_usd)
        if not risk["passed"]:
            self.logger.warning(f"交易被風險引擎拒絕: {risk['failed_reasons']}")
            self.record_trade_result("risk_rejected")
            if event_logger:
                event_logger("risk_rejected", {
                    "reasons": risk["failed_reasons"],
                    "signal": signal,
                    "size_usd": size_usd,
                })
            return {
                "status": "risk_rejected",
                "reasons": risk["failed_reasons"],
                "checks": risk["checks"],
                "signal": signal,
                "size_usd": size_usd,
            }


        is_buy = signal["direction"] == "PM > HL"
        hl_asset = signal["hl_market"]

        if self.dry_run or not self.enable_trading:
            self.logger.info(
                f"[DRY-RUN] {hl_asset} | ${size_usd} | {'BUY' if is_buy else 'SELL'}"
            )
            result = {
                "status": "dry_run_success",
                "asset": hl_asset,
                "size_usd": size_usd,
                "is_buy": is_buy,
                "signal": signal,
                "executed_at": datetime.now(timezone.utc).isoformat(),
            }
            if event_logger:
                event_logger("dry_run_trade", result)
            return result

        # ── Real REST order (TODO: signing) ───────────────────────────────────
        try:
            order_result = self._place_order_rest(
                asset=hl_asset,
                is_buy=is_buy,
                size_usd=size_usd,
                price=signal["hl_prob"],
            )
            self.logger.info(f"真實下單成功: {order_result}")
            self.record_trade_result("success")
            if event_logger:
                event_logger("live_trade_executed", {"result": order_result, "signal": signal})
            return {"status": "success", "result": order_result, "asset": hl_asset, "size_usd": size_usd}
        except Exception as e:
            self.record_trade_result("error")
            self.logger.error(f"下單失敗: {e}")
            if event_logger:
                event_logger("trade_error", {"error": str(e), "signal": signal})
            return {"status": "error", "error": str(e), "asset": hl_asset}


    def _place_order_rest(self, asset: str, is_buy: bool, size_usd: float, price: float) -> dict:
        """
        Phase 7-B: Real REST order via Hyperliquid /exchange endpoint.
        Uses HyperliquidRestSigner for EIP-712 Ed25519 signing.
        """
        if not self._signer:
            return {
                "status": "error",
                "message": "REST signer not initialized (check private_key)",
            }
        result = self._signer.place_order(
            asset=asset,
            is_buy=is_buy,
            sz=size_usd,
            limit_px=price,
        )
        return result



# ═══════════════════════════════════════════════════════════════════════════════
# Phase 6+7 Skill Wrapper — REST-only (SDK calls removed)
# ═══════════════════════════════════════════════════════════════════════════════

class HIP4Monitor:
    """
    OpenClaw Skill — Hyperliquid HIP-4 Phase 6 REST Edition.

    Built on validated REST implementation. Features ported from Phase 6:
      • GovernanceTracker — canonical market lifecycle tracking
      • Performance Analytics — arb signal history, win rate, avg edge
      • Multi-agent interface — dedicated getters for governance / signals / risk
      • Event log system — JSONL event stream
      • Circuit breaker — consecutive losses tracking
      • Advisory position sizing — no SDK trading

    Usage:
        monitor = HIP4Monitor()
        result  = monitor.execute()
        print(monitor.get_summary())
        print(monitor.health_check())
    """

    def __init__(
        self,
        config_path: str = None,
        arb_threshold: float = 0.05,
        workspace: str = "/home/node/.openclaw/workspace",
    ):
        self.arb_threshold = arb_threshold
        self._config = {}
        self._last_result: Optional[dict] = None
        self._ws_feed: Optional[WebSocketPriceFeed] = None

        # Phase 6: Files
        self._workspace = Path(workspace)
        self._events_file = self._workspace / "hip4_events.jsonl"
        self._perf_file   = self._workspace / "hip4_performance.json"
        self._state_file  = self._workspace / "hip4_phase6_state.json"

        # Phase 7: Semantic Matcher
        self._llm_callable = self._config.get("llm_callable") or None
        self._semantic_matcher: Optional[SemanticMatcher] = None
        if self._llm_callable or self._config.get("enable_llm_matching", False):
            self._semantic_matcher = SemanticMatcher(
                llm_callable=self._llm_callable,
                cache_ttl_minutes=self._config.get("semantic_cache_ttl_minutes", 60),
                confidence_threshold=self._config.get("confidence_threshold", 0.75),
                fallback_to_keyword=self._config.get("fallback_to_keyword", True),
            )

        # Phase 6: Canonical market tracking (GovernanceTracker)
        self._canonical_markets: Dict[str, Dict] = {}


        # Phase 6: Performance / Risk
        self._daily_pnl: float = 0.0
        self._consecutive_losses: int = 0
        self._consecutive_losses_limit: int = 3
        self._daily_loss_limit_usd: float = 200.0
        self._max_position_usd: float = 100.0

        # Phase 6: Circuit breaker
        self._circuit_broken: bool = False

        # Phase 6: Last run metadata
        self._last_heartbeat: Optional[datetime] = None
        self._last_error: Optional[str] = None
        self._vote_participation: float = 0.0

        # Phase 6: Recent arb signals cache (last 50)
        self._recent_arb_signals: List[Dict] = []

        # Logger
        self._logger = logging.getLogger("HIP4Phase6")
        self._logger.setLevel(logging.INFO)

        if config_path:
            with open(config_path) as f:
                self._config = json.load(f)

        # Phase 7: REST Trading Engine (init after config loaded)
        self._trading_engine = RestTradingEngine(self._config)
        # Sync risk state from loaded state into trading engine
        self._trading_engine._daily_pnl = self._daily_pnl
        self._trading_engine._consecutive_losses = self._consecutive_losses

        self._load_state()

    # ── Phase 6: Public Interface ──────────────────────────────────────────────

    def execute(self, ws_enabled: bool = False) -> dict:
        """
        Run the full monitor loop (REST-only).
        Returns structured result dict with governance + arb data.
        """
        self._last_heartbeat = datetime.now(timezone.utc)
        try:
            result = run_monitor(
                arb_threshold=self.arb_threshold,
                ws_enabled=ws_enabled,
                semantic_matcher=self._semantic_matcher,
            )
            self._last_result = result

            # Phase 6: Update governance tracker
            self._update_canonical_tracking(result)

            # Phase 6: Cache recent arb signals
            self._update_arb_cache(result.get("arb_signals", []))

            # Phase 6: Event log
            self._log_event("monitor_run", {
                "total_markets": result["total_markets"],
                "arb_count": len(result.get("arb_signals", [])),
                "new_markets": len(result.get("new_markets", [])),
                "newly_settled": len(result.get("newly_settled", [])),
            })

            # Phase 6: Vote participation
            votes_raw = get_validator_l1_votes()
            self._vote_participation = self._calc_participation(votes_raw)

            return result

        except Exception as e:
            self._last_error = str(e)
            self._log_event("error", {"message": str(e)})
            return {"status": "error", "error": str(e)}

    def get_summary(self, result: dict = None) -> str:
        """Human-readable summary string."""
        data = result or self._last_result
        if data is None:
            return "⚠️ No data. Run execute() first."
        if data.get("status") == "error":
            return f"錯誤: {data.get('error')}"
        base = generate_summary(data)
        # Phase 6 enrichment
        governance = f"  │ Canonical 追蹤: {len(self._canonical_markets)} 個市場"
        risk = f"  │ Circuit breaker: {'🔴 OFF' if self._circuit_broken else '🟢 ON'}  |  連續虧損: {self._consecutive_losses}"
        return f"{base}\n{governance}\n{risk}"

    def get_governance_events(self) -> dict:
        """Phase 6: Return canonical market lifecycle state."""
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tracked_canonical_markets": self._canonical_markets,
            "total_tracked": len(self._canonical_markets),
            "circuit_broken": self._circuit_broken,
        }

    def get_recent_arb_signals(self, limit: int = 20) -> List[Dict]:
        """Phase 6: Return cached recent arb signals."""
        return self._recent_arb_signals[-limit:]

    def get_performance_metrics(self) -> dict:
        """
        Phase 6: Compute historical arb-signal statistics from perf file.
        Returns win_rate, average_edge, total_tracked_signals, last_updated.
        """
        try:
            if not self._perf_file.exists():
                return {"message": "尚無績效資料"}

            with open(self._perf_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            signals = data.get("signals", [])
            total = len(signals)
            if total == 0:
                return {"message": "尚無已結算訊號"}

            wins = sum(1 for s in signals if s.get("was_profitable"))
            win_rate = round(wins / total, 3)
            avg_edge = round(sum(s.get("edge", 0) for s in signals) / total, 4)
            total_sim_pnl = round(sum(s.get("sim_pnl", 0) for s in signals), 4)

            return {
                "total_tracked_signals": total,
                "win_rate": win_rate,
                "average_edge": avg_edge,
                "total_simulated_pnl": total_sim_pnl,
                "last_updated": data.get("last_updated"),
            }
        except Exception as e:
            return {"error": str(e)}

    def get_risk_status(self) -> dict:
        """Phase 6: Current risk / circuit-breaker state."""
        return {
            "daily_pnl": self._daily_pnl,
            "consecutive_losses": self._consecutive_losses,
            "consecutive_losses_limit": self._consecutive_losses_limit,
            "daily_loss_limit_usd": self._daily_loss_limit_usd,
            "circuit_broken": self._circuit_broken,
            "max_position_usd": self._max_position_usd,
        }

    def get_account_state(self) -> dict:
        """
        Phase C: Get live account state from Hyperliquid REST API.
        Requires wallet_address in config or HL_WALLET_ADDRESS env var.
        Returns margin summary, withdrawable balance, open positions.
        """
        address = (
            self._config.get("wallet_address")
            or os.getenv("HL_WALLET_ADDRESS")
        )
        if not address:
            return {"ok": False, "error": "No wallet_address configured"}
        state = get_account_state(address)
        if state.get("ok"):
            spot = get_spot_balances(address)
            state["spot_balances"] = spot.get("balances", []) if spot.get("ok") else []
        return state

    def get_trading_suggestion(self) -> dict:
        """
        Phase 6: Generate advisory trading suggestion (NO SDK call, read-only).
        Returns action: EXECUTE_ARB | MONITOR | HOLD
        """
        if self._circuit_broken:
            return {"action": "HOLD", "reason": "Circuit breaker active"}

        result = self._last_result
        if not result or not result.get("arb_signals"):
            return {"action": "HOLD", "reason": "無 arb 機會"}

        best = max(result["arb_signals"], key=lambda x: x["diff"])
        size = self._calculate_position_size(best["diff"], self._vote_participation)

        if (best["diff"] >= 0.08
                and self._vote_participation >= 80
                and self._consecutive_losses < self._consecutive_losses_limit):
            action = "EXECUTE_ARB"
        elif best["diff"] >= 0.05:
            action = "MONITOR"
        else:
            action = "HOLD"

        return {
            "action": action,
            "signal": best,
            "suggested_size_usd": size,
            "validator_participation": self._vote_participation,
            "circuit_broken": self._circuit_broken,
            "timestamp": result.get("timestamp", ""),
        }

    def execute_arb_trade(
        self,
        signal: dict = None,
        size_usd: float = None,
    ) -> dict:
        """
        Phase 7: Execute an arb trade via the REST Trading Engine.
        If signal is None, uses the best signal from last execute() result.
        """
        if signal is None:
            suggestion = self.get_trading_suggestion()
            if suggestion.get("action") not in ("EXECUTE_ARB", "MONITOR"):
                return {"status": "skipped", "reason": suggestion.get("reason", "no signal")}
            signal = suggestion.get("signal")
            size_usd = size_usd or suggestion.get("suggested_size_usd")

        result = self._trading_engine.execute_arb_trade(
            signal=signal,
            suggested_size_usd=size_usd,
            event_logger=self._log_event,
        )
        # Sync circuit breaker state back
        self._consecutive_losses = self._trading_engine._consecutive_losses
        self._daily_pnl = self._trading_engine._daily_pnl
        if self._consecutive_losses >= self._consecutive_losses_limit:
            self._circuit_broken = True
            self._log_event("circuit_breaker_tripped", {"consecutive_losses": self._consecutive_losses})
        return result

    def auto_execute_from_suggestion(self) -> dict:
        """
        Phase 7: Auto-execute based on get_trading_suggestion().
        Only executes if action == EXECUTE_ARB.
        """
        suggestion = self.get_trading_suggestion()
        if suggestion.get("action") != "EXECUTE_ARB":
            return {"status": "skipped", "action": suggestion["action"], "reason": suggestion.get("reason")}
        return self.execute_arb_trade(
            signal=suggestion.get("signal"),
            size_usd=suggestion.get("suggested_size_usd"),
        )

    def ingest_settlement_result(
        self,
        market_name: str,
        settled_to: float,
        actual_outcome: str,
    ):
        """
        Phase 6: Record a market settlement result for performance tracking.
        Call this externally when a market settles.
        """
        if market_name in self._canonical_markets:
            self._canonical_markets[market_name].update({
                "settled": True,
                "settled_to": settled_to,
                "actual_outcome": actual_outcome,
                "settled_at": datetime.now(timezone.utc).isoformat(),
            })
            self._log_event("market_settled", {
                "market": market_name,
                "settled_to": settled_to,
                "actual_outcome": actual_outcome,
            })
            self._update_performance_from_settlement(market_name, settled_to)
        self._save_state()

    def health_check(self) -> dict:
        """Phase 6: Full health check of all data sources + internal state."""
        status = {}

        try:
            r = _post({"type": "validatorL1Votes"}, timeout=10)
            status["hyperliquid_api"] = {
                "ok": r.status_code == 200,
                "status_code": r.status_code,
                "response_ms": int(r.elapsed.total_seconds() * 1000),
            }
        except Exception as e:
            status["hyperliquid_api"] = {"ok": False, "error": str(e)}

        try:
            r = _post({"type": "allMids"}, timeout=10)
            mids_data = r.json() if r.status_code == 200 else {}
            outcome_count = sum(1 for k in mids_data if k.startswith("#"))
            status["hyperliquid_allMids"] = {
                "ok": r.status_code == 200,
                "outcome_tokens": outcome_count,
            }
        except Exception as e:
            status["hyperliquid_allMids"] = {"ok": False, "error": str(e)}

        try:
            r = requests.get(
                POLYMARKET_GAMMA_URL,
                params={"active": "true", "closed": "false", "limit": 1},
                timeout=10,
            )
            status["polymarket_gamma"] = {
                "ok": r.status_code == 200,
                "status_code": r.status_code,
            }
        except Exception as e:
            status["polymarket_gamma"] = {"ok": False, "error": str(e)}

        # Phase 6: internal state
        status["circuit_breaker"] = {
            "ok": not self._circuit_broken,
            "broken": self._circuit_broken,
            "consecutive_losses": self._consecutive_losses,
        }
        status["canonical_markets"] = len(self._canonical_markets)
        status["last_heartbeat"] = (
            self._last_heartbeat.isoformat()
            if self._last_heartbeat else None
        )

        all_ok = all(
            v.get("ok", True) for v in status.values()
            if isinstance(v, dict) and "ok" in v
        )
        status["_all_ok"] = all_ok
        return status

    def reset_circuit_breaker(self):
        """Phase 6: Manually reset circuit breaker."""
        self._circuit_broken = False
        self._consecutive_losses = 0
        self._log_event("circuit_breaker_reset", {})

    def stop(self):
        if self._ws_feed:
            self._ws_feed.stop()

    # ── Phase 6: Internal ─────────────────────────────────────────────────────

    def _update_canonical_tracking(self, result: dict):
        """Track canonical market lifecycle from parsed market data."""
        for signal in result.get("arb_signals", []):
            name = signal.get("hl_market", "")
            if not name or name in self._canonical_markets:
                continue
            self._canonical_markets[name] = {
                "first_seen": datetime.now(timezone.utc).isoformat(),
                "last_prob": signal.get("hl_prob"),
                "pm_prob": signal.get("pm_prob"),
                "diff": signal.get("diff"),
                "settled": False,
            }
            self._log_event("canonical_market_discovered", {"market": name})

    def _update_arb_cache(self, signals: list):
        """Keep last 50 arb signals in memory."""
        self._recent_arb_signals.extend(signals)
        self._recent_arb_signals = self._recent_arb_signals[-50:]

    def _calc_participation(self, votes_raw: list) -> float:
        """Calculate validator participation rate from raw votes."""
        try:
            total = len(votes_raw) if isinstance(votes_raw, list) else 0
            if total == 0:
                return 0.0
            # Participation heuristic: fraction of markets with votes
            active = sum(1 for m in votes_raw if m.get("vote_count", 0) > 0)
            return round((active / total) * 100, 2)
        except Exception:
            return 0.0

    def _calculate_position_size(self, diff: float, participation: float) -> float:
        """
        Phase 6: Advisory position sizing (no SDK, pure formula).
        Returns suggested USD size.
        """
        base = min(self._max_position_usd, 25 + (diff * 400))
        return round(base * min(1.0, participation / 100), 2)

    def _update_performance_from_settlement(
        self, market_name: str, settled_price: float
    ):
        """Append settled market to performance log."""
        try:
            perf = {"signals": [], "last_updated": None}
            if self._perf_file.exists():
                with open(self._perf_file, "r", encoding="utf-8") as f:
                    perf = json.load(f)

            # Find the matching signal and mark it
            matched = False
            for sig in perf.get("signals", []):
                if sig.get("market") == market_name and not sig.get("settled"):
                    sig["settled"] = True
                    sig["settled_price"] = settled_price
                    sig["was_profitable"] = (
                        sig.get("direction") == "BUY_HL"
                        if settled_price > 0.5
                        else sig.get("direction") == "BUY_PM"
                    )
                    matched = True
                    break

            if not matched:
                # New settlement not pre-tracked — log as informational
                perf["signals"].append({
                    "market": market_name,
                    "settled_price": settled_price,
                    "settled": True,
                    "was_profitable": None,
                    "edge": 0.0,
                    "sim_pnl": 0.0,
                })

            perf["last_updated"] = datetime.now(timezone.utc).isoformat()
            self._perf_file.write_text(
                json.dumps(perf, ensure_ascii=False, indent=2)
            )
        except Exception as e:
            self._logger.error(f"更新績效失敗: {e}")

    def _record_signal(self, signal: dict):
        """Phase 6: Record an arb signal to perf file for later settlement matching."""
        try:
            perf = {"signals": [], "last_updated": None}
            if self._perf_file.exists():
                with open(self._perf_file, "r", encoding="utf-8") as f:
                    perf = json.load(f)

            perf["signals"].append({
                "market": signal.get("hl_market"),
                "direction": signal.get("direction"),
                "hl_prob": signal.get("hl_prob"),
                "pm_prob": signal.get("pm_prob"),
                "diff": signal.get("diff"),
                "settled": False,
                "was_profitable": None,
                "edge": signal.get("diff", 0),
                "sim_pnl": 0.0,
                "signal_at": datetime.now(timezone.utc).isoformat(),
            })
            perf["last_updated"] = datetime.now(timezone.utc).isoformat()
            self._perf_file.write_text(
                json.dumps(perf, ensure_ascii=False, indent=2)
            )
        except Exception as e:
            self._logger.error(f"Record signal failed: {e}")

    def _log_event(self, event_type: str, payload: dict):
        """Phase 6: Append structured event to JSONL log."""
        try:
            entry = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "event": event_type,
                "payload": payload,
            }
            with open(self._events_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")
        except Exception as e:
            self._logger.error(f"Log event failed: {e}")

    def _load_state(self):
        if self._state_file.exists():
            try:
                data = json.loads(self._state_file.read_text())
                self._canonical_markets = data.get("canonical_markets", {})
                self._consecutive_losses = data.get("consecutive_losses", 0)
                self._circuit_broken = data.get("circuit_broken", False)
                self._daily_pnl = data.get("daily_pnl", 0.0)
            except Exception:
                pass

    def _save_state(self):
        try:
            data = {
                "canonical_markets": self._canonical_markets,
                "consecutive_losses": self._consecutive_losses,
                "circuit_broken": self._circuit_broken,
                "daily_pnl": self._daily_pnl,
                "last_updated": datetime.now(timezone.utc).isoformat(),
            }
            self._state_file.write_text(
                json.dumps(data, ensure_ascii=False, indent=2)
            )
        except Exception as e:
            self._logger.error(f"Save state failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# Phase 8: Backtesting Framework
# ═══════════════════════════════════════════════════════════════════════════════

class HIP4Backtester:
    """
    Phase 8: 回測框架 — replays historical arb signals from event logs.
    """
    def __init__(self, workspace: str = None):
        self._workspace = Path(workspace or "/home/node/.openclaw/workspace")
        self._events_file = self._workspace / "hip4_events.jsonl"
        self._perf_file   = self._workspace / "hip4_performance.json"

    def load_events(self, start_date: str = None, end_date: str = None) -> list:
        if not self._events_file.exists():
            return []
        events = []
        with open(self._events_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    ts = entry.get("ts", "")
                    if start_date and ts < start_date:
                        continue
                    if end_date and ts > end_date:
                        continue
                    if entry.get("event") in ("dry_run_trade", "live_trade_executed"):
                        events.append(entry)
                except Exception:
                    pass
        return events

    def load_perf_signals(self) -> list:
        if not self._perf_file.exists():
            return []
        with open(self._perf_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("signals", [])

    def run_backtest(
        self,
        start_date: str = None,
        end_date: str = None,
        initial_capital: float = 1000.0,
    ) -> dict:
        events = self.load_events(start_date, end_date)
        signals = self.load_perf_signals()

        all_signals = []
        for e in events:
            p = e.get("payload", {})
            sig = p.get("signal", {})
            all_signals.append({
                "ts": e.get("ts"),
                "event": e.get("event"),
                "asset": p.get("asset"),
                "size_usd": p.get("size_usd"),
                "diff": sig.get("diff"),
                "direction": sig.get("direction"),
            })

        for s in signals:
            if s.get("settled"):
                all_signals.append({
                    "ts": s.get("signal_at"),
                    "event": "settled_signal",
                    "asset": s.get("market"),
                    "direction": s.get("direction"),
                    "diff": s.get("diff"),
                    "settled": True,
                    "settled_price": s.get("settled_price"),
                    "was_profitable": s.get("was_profitable"),
                })

        if not all_signals:
            return {"message": "尚無歷史資料可供回測", "signals_count": 0}

        all_signals.sort(key=lambda x: x.get("ts") or "")

        capital = initial_capital
        peak_capital = initial_capital
        max_drawdown = 0.0
        trade_log = []
        wins = 0
        losses = 0

        for sig in all_signals:
            size = sig.get("size_usd", 10.0)
            diff = sig.get("diff", 0)
            is_dry_run = sig.get("event") == "dry_run_trade"
            is_settled = sig.get("settled")

            if is_dry_run:
                pnl = diff * size if sig.get("direction") == "BUY_HL" else -diff * size
                capital += pnl
                trade_log.append({"ts": sig["ts"], "pnl": pnl, "capital": capital})
                wins += 1 if pnl > 0 else 0
                losses += 1 if pnl <= 0 else 0
            elif is_settled:
                was_prof = sig.get("was_profitable")
                if was_prof is not None:
                    pnl = diff * size if was_prof else -diff * size
                    capital += pnl
                    trade_log.append({"ts": sig["ts"], "pnl": pnl, "capital": capital})
                    wins += 1 if was_prof else 0
                    losses += 1 if not was_prof else 0

            peak_capital = max(peak_capital, capital)
            dd = (peak_capital - capital) / peak_capital if peak_capital > 0 else 0
            max_drawdown = max(max_drawdown, dd)

        total_trades = wins + losses
        win_rate = round(wins / total_trades, 3) if total_trades > 0 else 0
        total_return = round(capital - initial_capital, 2)
        total_return_pct = round((capital - initial_capital) / initial_capital * 100, 2)

        if trade_log:
            pnls = [t["pnl"] for t in trade_log]
            avg_pnl = sum(pnls) / len(pnls)
            import statistics
            std_pnl = statistics.stdev(pnls) if len(pnls) > 1 else 0
            sharpe = round((avg_pnl / std_pnl) if std_pnl > 0 else 0, 3)
        else:
            sharpe = 0.0

        return {
            "start_date": start_date or "all",
            "end_date": end_date or "all",
            "initial_capital": initial_capital,
            "final_capital": round(capital, 2),
            "total_return_usd": total_return,
            "total_return_pct": total_return_pct,
            "total_trades": total_trades,
            "wins": wins,
            "losses": losses,
            "win_rate": win_rate,
            "average_edge": round(sum(s.get("diff", 0) for s in all_signals) / len(all_signals), 4),
            "max_drawdown_pct": round(max_drawdown * 100, 2),
            "sharpe_ratio": sharpe,
            "signals_count": len(all_signals),
            "trade_log_sample": trade_log[-10:],
        }

    def format_report(self, report: dict) -> str:
        if "message" in report:
            return f"⚠️ {report['message']}"
        lines = [
            f"📊 HIP-4 Backtest ({report['start_date']} → {report['end_date']})",
            f"   Initial: ${report['initial_capital']}  |  Final: ${report['final_capital']}",
            f"   Return: ${report['total_return_usd']} ({report['total_return_pct']}%)",
            f"   Trades: {report['total_trades']}  |  Wins: {report['wins']}  |  Losses: {report['losses']}",
            f"   Win Rate: {report['win_rate']:.1%}  |  Avg Edge: {report['average_edge']:.2%}",
            f"   Max DD: {report['max_drawdown_pct']:.1f}%  |  Sharpe: {report['sharpe_ratio']}",
        ]
        return "\n".join(lines)



# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    monitor = HIP4Monitor()
    result = monitor.execute(ws_enabled=True)
    print(monitor.get_summary())
    print("\n─── Health ───")
    h = monitor.health_check()
    for k, v in h.items():
        print(f"  {k}: {v}")
    print("\n─── Risk ───")
    print(monitor.get_risk_status())
    print("\n─── Governance ───")
    print(monitor.get_governance_events())
    print("\n─── Trading Suggestion ───")
    print(monitor.get_trading_suggestion())
    print("\n─── Performance ───")
    print(monitor.get_performance_metrics())
