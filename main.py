"""
Kiedex Auto Trading Bot — Backend (Single File)
================================================
- Qwen AI via reverse-engineer (chat.qwen.ai) — tanpa gateway
- MEXC Futures candle data (public WebSocket/REST)
- Trade execution via Supabase Edge Functions (kiedex.app)
- 2 akun trade secara bersamaan (akun A & B)
- Analisa 1 coin → open di 2 akun → hold/close AI loop → cari coin berikutnya
- WebSocket server untuk frontend monitoring
- REST API: start/stop bot, set config

Deploy: Railway
Env vars (lihat bagian bawah file untuk daftar lengkap)
"""

import asyncio
import json
import logging
import os
import time
import uuid
import hmac
import hashlib
import re
import random
from typing import Optional, Dict, List
from datetime import datetime, timezone

import httpx
import websockets
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("bot")

# ─────────────────────────────────────────────
# ENV CONFIG
# ─────────────────────────────────────────────
# Akun A
ACCOUNT_A_JWT   = os.getenv("ACCOUNT_A_JWT", "")
ACCOUNT_A_UID   = os.getenv("ACCOUNT_A_UID", "")
# Akun B
ACCOUNT_B_JWT   = os.getenv("ACCOUNT_B_JWT", "")
ACCOUNT_B_UID   = os.getenv("ACCOUNT_B_UID", "")

SUPABASE_URL    = os.getenv("SUPABASE_URL", "https://ffcsrzbwbuzhboyyloam.supabase.co")
SUPABASE_APIKEY = os.getenv("SUPABASE_APIKEY", "sb_publishable_ZN-MbrdVe1UcfCHwl-I2aw_DFZ2aWDf")

# Qwen reverse API tokens (min 1)
QWEN_TOKEN_1 = os.getenv("QWEN_TOKEN_1", "")
QWEN_TOKEN_2 = os.getenv("QWEN_TOKEN_2", "")
QWEN_MODEL   = os.getenv("QWEN_MODEL", "qwen3-max")

# Bot default settings (dapat di-override via API)
DEFAULT_MARGIN    = float(os.getenv("DEFAULT_MARGIN", "50"))
DEFAULT_LEVERAGE  = int(os.getenv("DEFAULT_LEVERAGE", "5"))

MEXC_BASE_URL = "https://contract.mexc.com"
MEXC_WS_URL   = "wss://contract.mexc.com/edge"
HTTP_PORT      = int(os.getenv("PORT", "8000"))

# Coin yang tersedia di kiedex.app (simbol MEXC format)
AVAILABLE_COINS = [
    "BTC_USDT", "ETH_USDT", "BNB_USDT", "SOL_USDT",
    "LTC_USDT", "DOGE_USDT", "TRX_USDT", "SHIB_USDT",
]

# Mapping MEXC symbol → kiedex symbol
MEXC_TO_KIEDEX = {
    "BTC_USDT":  "BTCUSDT",
    "ETH_USDT":  "ETHUSDT",
    "BNB_USDT":  "BNBUSDT",
    "SOL_USDT":  "SOLUSDT",
    "LTC_USDT":  "LTCUSDT",
    "DOGE_USDT": "DOGEUSDT",
    "TRX_USDT":  "TRXUSDT",
    "SHIB_USDT": "SHIBUSDT",
}

TIMEFRAMES = ["5m", "15m", "1h", "4h"]
INTERVAL_MAP = {
    "5m": "Min5", "15m": "Min15", "1h": "Min60", "4h": "Hour4",
}
INTERVAL_SECONDS = {
    "Min5": 300, "Min15": 900, "Min60": 3600, "Hour4": 14400,
}

# ─────────────────────────────────────────────
# Global State
# ─────────────────────────────────────────────
class BotState:
    running: bool = False
    margin: float = DEFAULT_MARGIN
    leverage: int = DEFAULT_LEVERAGE
    current_coin: Optional[str] = None
    position_a: Optional[dict] = None   # {positionId, symbol, side, entry, tp, sl}
    position_b: Optional[dict] = None
    live_price: Dict[str, float] = {}
    status: str = "idle"                # idle | analyzing | in_trade | closing
    last_analysis: Optional[dict] = None
    logs: List[str] = []
    ws_clients: set = set()
    _lock: asyncio.Lock = None

    @classmethod
    def get_lock(cls):
        if cls._lock is None:
            cls._lock = asyncio.Lock()
        return cls._lock

state = BotState()


def log(msg: str, level: str = "INFO"):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    state.logs.append(entry)
    if len(state.logs) > 200:
        state.logs = state.logs[-200:]
    getattr(logger, level.lower(), logger.info)(msg)
    asyncio.create_task(broadcast({"type": "log", "msg": entry}))


async def broadcast(data: dict):
    if not state.ws_clients:
        return
    dead = set()
    for ws in state.ws_clients.copy():
        try:
            await ws.send_json(data)
        except Exception:
            dead.add(ws)
    state.ws_clients -= dead


async def broadcast_state():
    await broadcast({
        "type": "state",
        "running": state.running,
        "status": state.status,
        "current_coin": state.current_coin,
        "margin": state.margin,
        "leverage": state.leverage,
        "position_a": state.position_a,
        "position_b": state.position_b,
        "live_price": state.live_price,
        "last_analysis": state.last_analysis,
    })


# ─────────────────────────────────────────────
# MEXC REST — Candle fetcher
# ─────────────────────────────────────────────
async def fetch_candles(symbol: str, granularity: str, limit: int = 150) -> list:
    interval = INTERVAL_MAP.get(granularity, "Min5")
    secs = INTERVAL_SECONDS.get(interval, 300)
    end_ts = int(time.time())
    start_ts = end_ts - secs * (limit + 10)

    url = f"{MEXC_BASE_URL}/api/v1/contract/kline/{symbol}"
    params = {"interval": interval, "start": str(start_ts), "end": str(end_ts)}

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json().get("data") or {}
            if not isinstance(data, dict):
                return []

            times   = data.get("time",   [])
            opens   = data.get("open",   [])
            highs   = data.get("high",   [])
            lows    = data.get("low",    [])
            closes  = data.get("close",  [])
            vols    = data.get("vol",    [])

            candles = []
            for i in range(len(times)):
                try:
                    candles.append([
                        int(times[i]) * 1000,
                        float(opens[i]),
                        float(highs[i]),
                        float(lows[i]),
                        float(closes[i]),
                        float(vols[i]) if i < len(vols) else 0.0,
                    ])
                except Exception:
                    continue
            return candles[-limit:]
    except Exception as e:
        logger.error(f"fetch_candles {symbol}/{granularity}: {e}")
        return []


async def fetch_all_candles(symbol: str) -> dict:
    """Fetch all timeframes concurrently."""
    tasks = {tf: asyncio.create_task(fetch_candles(symbol, tf)) for tf in TIMEFRAMES}
    result = {}
    for tf, task in tasks.items():
        result[tf] = await task
    return result


# ─────────────────────────────────────────────
# MEXC WebSocket — Live Price Feed
# ─────────────────────────────────────────────
class MexcPriceFeed:
    def __init__(self):
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._ws = None
        self._subscribed: set = set()

    async def start(self):
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._loop(), name="mexc_ws")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass

    def subscribe(self, symbol: str):
        self._subscribed.add(symbol)
        if self._ws:
            asyncio.create_task(self._send_sub(symbol))

    async def _send_sub(self, symbol: str):
        if self._ws:
            try:
                await self._ws.send(json.dumps({
                    "method": "sub.ticker",
                    "param": {"symbol": symbol},
                    "gzip": False,
                }))
            except Exception:
                pass

    async def _loop(self):
        backoff = 2
        while self._running:
            try:
                async with websockets.connect(
                    MEXC_WS_URL,
                    ping_interval=None,
                    close_timeout=5,
                    max_size=10 * 1024 * 1024,
                ) as ws:
                    self._ws = ws
                    backoff = 2
                    log("📡 MEXC WS connected")

                    # Subscribe bulk tickers
                    await ws.send(json.dumps({
                        "method": "sub.tickers", "param": {}, "gzip": False
                    }))
                    for sym in self._subscribed:
                        await self._send_sub(sym)

                    ping_task = asyncio.create_task(self._ping(ws))
                    try:
                        async for raw in ws:
                            if not self._running:
                                break
                            await self._handle(raw)
                    finally:
                        ping_task.cancel()

            except asyncio.CancelledError:
                break
            except Exception as e:
                if not self._running:
                    break
                log(f"📡 MEXC WS error: {e} — retry in {backoff}s", "WARNING")
                self._ws = None
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
        self._ws = None

    async def _ping(self, ws):
        while True:
            try:
                await asyncio.sleep(15)
                await ws.send(json.dumps({"method": "ping"}))
            except Exception:
                break

    async def _handle(self, raw: str):
        try:
            msg = json.loads(raw)
        except Exception:
            return
        channel = msg.get("channel", "")
        if channel == "push.tickers":
            for item in msg.get("data", []):
                sym = item.get("symbol")
                price = item.get("lastPrice")
                if sym and price:
                    state.live_price[sym] = float(price)
                    # Broadcast live price update for current coin
                    if sym == state.current_coin:
                        asyncio.create_task(broadcast({
                            "type": "price",
                            "symbol": sym,
                            "price": float(price),
                        }))
        elif channel == "push.ticker":
            sym = msg.get("symbol") or (msg.get("data") or {}).get("symbol")
            price = (msg.get("data") or {}).get("lastPrice")
            if sym and price:
                state.live_price[sym] = float(price)
                if sym == state.current_coin:
                    asyncio.create_task(broadcast({
                        "type": "price",
                        "symbol": sym,
                        "price": float(price),
                    }))


price_feed = MexcPriceFeed()


# ─────────────────────────────────────────────
# Qwen Reverse API (testt.py style)
# ─────────────────────────────────────────────
QWEN_BASE_URL_REVERSE = "https://chat.qwen.ai"

def _qwen_headers(token: str, chat_id: str = None) -> dict:
    h = {
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Content-Type": "application/json",
        "source": "web",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36",
        "Origin": "https://chat.qwen.ai",
        "Version": "0.2.7",
        "bx-v": "2.5.36",
        "Authorization": f"Bearer {token}",
        "X-Request-Id": str(uuid.uuid4()),
    }
    if chat_id:
        h["Referer"] = f"https://chat.qwen.ai/c/{chat_id}"
    return h


async def qwen_create_chat(token: str, client: httpx.AsyncClient) -> Optional[str]:
    try:
        resp = await client.post(
            f"{QWEN_BASE_URL_REVERSE}/api/v2/chats/new",
            headers=_qwen_headers(token),
            json={
                "title": "Bot Analysis",
                "models": [QWEN_MODEL],
                "chat_mode": "normal",
                "chat_type": "t2t",
                "timestamp": int(time.time() * 1000),
                "project_id": "",
            },
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()["data"]["id"]
    except Exception as e:
        logger.error(f"qwen_create_chat error: {e}")
        return None


async def qwen_send_message(token: str, chat_id: str, prompt: str, client: httpx.AsyncClient) -> str:
    fid = str(uuid.uuid4())
    child_id = str(uuid.uuid4())
    ts = int(time.time())

    payload = {
        "stream": True,
        "version": "2.1",
        "incremental_output": True,
        "chat_id": chat_id,
        "chat_mode": "normal",
        "model": QWEN_MODEL,
        "parent_id": None,
        "messages": [{
            "fid": fid,
            "parentId": None,
            "childrenIds": [child_id],
            "role": "user",
            "content": prompt,
            "user_action": "chat",
            "files": [],
            "timestamp": ts,
            "models": [QWEN_MODEL],
            "chat_type": "t2t",
            "feature_config": {
                "thinking_enabled": True,
                "output_schema": "phase",
                "research_mode": "normal",
                "auto_thinking": True,
                "thinking_mode": "Auto",
                "thinking_format": "summary",
                "auto_search": False,
            },
            "extra": {"meta": {"subChatType": "t2t"}},
            "sub_chat_type": "t2t",
            "parent_id": None,
        }],
        "timestamp": ts + 1,
    }

    headers = {**_qwen_headers(token, chat_id), "x-accel-buffering": "no"}
    full_reply = ""

    try:
        async with client.stream(
            "POST",
            f"{QWEN_BASE_URL_REVERSE}/api/v2/chat/completions?chat_id={chat_id}",
            headers=headers,
            json=payload,
            timeout=180,
        ) as resp:
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                if not line or not line.startswith("data: "):
                    continue
                data_str = line[6:]
                if data_str.strip() == "[DONE]":
                    break
                try:
                    data = json.loads(data_str)
                    if not data.get("choices"):
                        continue
                    delta = data["choices"][0].get("delta", {})
                    content = delta.get("content", "")
                    status = delta.get("status")
                    if content:
                        full_reply += content
                    if status == "finished":
                        break
                except Exception:
                    continue
    except Exception as e:
        logger.error(f"qwen_send_message error: {e}")

    return full_reply


async def qwen_delete_chat(token: str, chat_id: str, client: httpx.AsyncClient):
    try:
        await client.delete(
            f"{QWEN_BASE_URL_REVERSE}/api/v2/chats/{chat_id}",
            headers=_qwen_headers(token),
            timeout=15,
        )
    except Exception:
        pass


def _get_qwen_token() -> str:
    tokens = [t for t in [QWEN_TOKEN_1, QWEN_TOKEN_2] if t]
    if not tokens:
        raise ValueError("No Qwen token configured! Set QWEN_TOKEN_1 in env.")
    return random.choice(tokens)


# ─────────────────────────────────────────────
# AI Analysis — Prompts
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """You are a professional crypto futures trading AI.
Your goal is to FIND HIGH-PROBABILITY SETUPS and TARGET PROFIT aggressively.

Strategy rules:
- Analyze multi-timeframe structure (5m, 15m, 1h, 4h)
- Find wick voids and liquidity imbalances
- UP trend → ONLY LONG | DOWN trend → ONLY SHORT
- Entry MUST be at market (not limit) — use current price context
- Set TP at realistic profit target (minimum 1:1.5 R/R, aim for 1:3 R/R)
- Set SL at clear invalidation level
- Be AGGRESSIVE in seeking profit — if setup is clear, take it
- Only skip if structure is genuinely unclear

You must respond in VALID JSON only, no markdown, no preamble."""

def build_analysis_prompt(symbol: str, candles_by_tf: dict, current_price: float) -> str:
    kiedex_sym = MEXC_TO_KIEDEX.get(symbol, symbol)
    blocks = []
    for tf in TIMEFRAMES:
        candles = candles_by_tf.get(tf, [])
        if not candles:
            continue
        last = candles[-50:]
        lines = [f"=== {kiedex_sym} | {tf} | {len(last)} candles ===", "ts, open, high, low, close, vol"]
        for c in last:
            lines.append(f"{c[0]}, {c[1]:.6f}, {c[2]:.6f}, {c[3]:.6f}, {c[4]:.6f}, {c[5]:.2f}")
        blocks.append("\n".join(lines))

    ohlcv = "\n\n".join(blocks) if blocks else "No data"

    return f"""Analyze {kiedex_sym} for a MARKET ORDER trade opportunity.

CURRENT PRICE: {current_price}

{ohlcv}

Rules:
- Entry is MARKET (at current price ~{current_price}), NOT limit
- TP should be an actual price level above (LONG) or below (SHORT) current price
- SL should be below entry (LONG) or above entry (SHORT) at clear invalidation
- Aim for minimum 1:1.5 R/R, ideally 1:3
- Be aggressive — if trend is clear, take the trade
- Available coins: BTC, ETH, BNB, SOL, LTC, DOGE, TRX, SHIB

Respond ONLY in this exact JSON:
{{
  "trend": "UP" or "DOWN" or "SIDEWAYS",
  "decision": "LONG" or "SHORT" or "NO TRADE",
  "entry": {current_price},
  "tp": <take profit price>,
  "sl": <stop loss price>,
  "reason": "brief reason",
  "confidence": <0-100>
}}

If NO TRADE, set entry/tp/sl to null."""


def build_hold_close_prompt(symbol: str, side: str, entry: float, current_price: float,
                              tp: Optional[float], sl: Optional[float],
                              candles_15m: list, pnl_pct: float) -> str:
    kiedex_sym = MEXC_TO_KIEDEX.get(symbol, symbol)
    last = candles_15m[-20:] if candles_15m else []
    lines = ["ts, open, high, low, close, vol"]
    for c in last:
        lines.append(f"{c[0]}, {c[1]:.6f}, {c[2]:.6f}, {c[3]:.6f}, {c[4]:.6f}, {c[5]:.2f}")
    candle_text = "\n".join(lines)

    direction = "LONG" if side == "long" else "SHORT"
    return f"""You are managing an open {direction} position on {kiedex_sym}.

Position details:
- Side: {direction}
- Entry: {entry}
- Current price: {current_price}
- TP target: {tp}
- SL level: {sl}
- Unrealized PnL: {pnl_pct:.2f}%

Recent 15m candles:
{candle_text}

Decision rules:
- CLOSE if: price is moving strongly AGAINST position, SL is near, structure broken
- HOLD if: trend still intact, momentum in our favor, TP not yet hit
- CLOSE if: PnL > 1% profit (take profit early if structure shows reversal)
- HOLD if: PnL is small negative but structure still valid
- Be PROFIT-SEEKING: close at good profit, don't let winners turn to losers

Respond ONLY in JSON:
{{
  "decision": "HOLD" or "CLOSE",
  "reason": "brief reason"
}}"""


# ─────────────────────────────────────────────
# AI Call — full flow
# ─────────────────────────────────────────────
async def call_qwen(prompt: str) -> str:
    token = _get_qwen_token()
    async with httpx.AsyncClient(timeout=200) as client:
        chat_id = await qwen_create_chat(token, client)
        if not chat_id:
            return ""
        try:
            reply = await qwen_send_message(token, chat_id, prompt, client)
        finally:
            await qwen_delete_chat(token, chat_id, client)
    return reply


def parse_json_from_text(text: str) -> Optional[dict]:
    start = text.find("{")
    end = text.rfind("}") + 1
    if start < 0 or end <= start:
        return None
    try:
        return json.loads(text[start:end])
    except Exception:
        # Try to clean up common issues
        snippet = text[start:end]
        # Remove trailing commas
        snippet = re.sub(r",\s*([}\]])", r"\1", snippet)
        try:
            return json.loads(snippet)
        except Exception:
            return None


# ─────────────────────────────────────────────
# Supabase Trade Execution
# ─────────────────────────────────────────────
async def execute_trade(jwt: str, symbol: str, side: str, margin: float, leverage: int,
                         tp: Optional[float], sl: Optional[float]) -> Optional[dict]:
    kiedex_sym = MEXC_TO_KIEDEX.get(symbol, symbol)
    headers = {
        "apikey": SUPABASE_APIKEY,
        "authorization": f"Bearer {jwt}",
        "content-type": "application/json",
        "origin": "https://kiedex.app",
        "referer": "https://kiedex.app/",
    }
    payload = {
        "symbol": kiedex_sym,
        "side": side,
        "leverage": leverage,
        "margin": margin,
        "takeProfitPrice": tp,
        "stopLossPrice": sl,
    }
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{SUPABASE_URL}/functions/v1/execute-trade",
                headers=headers,
                json=payload,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("success"):
                return data.get("data")
            logger.error(f"execute_trade failed: {data}")
            return None
    except Exception as e:
        logger.error(f"execute_trade exception: {e}")
        return None


async def close_trade(jwt: str, position_id: str) -> Optional[dict]:
    headers = {
        "apikey": SUPABASE_APIKEY,
        "authorization": f"Bearer {jwt}",
        "content-type": "application/json",
        "origin": "https://kiedex.app",
        "referer": "https://kiedex.app/",
    }
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                f"{SUPABASE_URL}/functions/v1/close-trade",
                headers=headers,
                json={"positionId": position_id, "reason": "bot"},
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("success"):
                return data.get("data")
            logger.error(f"close_trade failed: {data}")
            return None
    except Exception as e:
        logger.error(f"close_trade exception: {e}")
        return None


async def get_trade_history(jwt: str, uid: str, limit: int = 20) -> list:
    headers = {
        "apikey": SUPABASE_APIKEY,
        "authorization": f"Bearer {jwt}",
        "accept-profile": "public",
        "origin": "https://kiedex.app",
    }
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{SUPABASE_URL}/rest/v1/trades_history",
                headers=headers,
                params={
                    "select": "*",
                    f"user_id": f"eq.{uid}",
                    "order": "closed_at.desc",
                    "offset": "0",
                    "limit": str(limit),
                },
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        logger.error(f"get_trade_history error: {e}")
        return []


# ─────────────────────────────────────────────
# Core Bot Logic
# ─────────────────────────────────────────────

async def select_coin() -> Optional[str]:
    """Pick a coin with the strongest trend from the available list."""
    log("🔍 Scanning coins for best setup...")
    state.status = "analyzing"
    await broadcast_state()

    best_coin = None
    best_confidence = 0

    # Shuffle to avoid always picking same coin
    coins = AVAILABLE_COINS.copy()
    random.shuffle(coins)

    for symbol in coins:
        if not state.running:
            return None
        try:
            candles = await fetch_all_candles(symbol)
            current_price = state.live_price.get(symbol)
            if not current_price:
                # Get from 5m candle last close
                c5 = candles.get("5m", [])
                if c5:
                    current_price = float(c5[-1][4])
                else:
                    continue

            prompt = build_analysis_prompt(symbol, candles, current_price)
            log(f"🤖 Analyzing {MEXC_TO_KIEDEX.get(symbol, symbol)}...")

            reply = await call_qwen(prompt)
            if not reply:
                log(f"⚠️ No reply for {symbol}", "WARNING")
                continue

            result = parse_json_from_text(reply)
            if not result:
                log(f"⚠️ Parse failed for {symbol}", "WARNING")
                continue

            decision = result.get("decision", "NO TRADE").upper()
            confidence = int(result.get("confidence", 0))

            log(f"📊 {MEXC_TO_KIEDEX.get(symbol, symbol)}: {decision} conf={confidence}%")

            if decision in ("LONG", "SHORT") and confidence > best_confidence:
                best_confidence = confidence
                best_coin = symbol
                state.last_analysis = {
                    **result,
                    "symbol": symbol,
                    "kiedex_symbol": MEXC_TO_KIEDEX.get(symbol, symbol),
                    "current_price": current_price,
                }
                await broadcast_state()

            # Stop scanning once we find high-confidence trade
            if confidence >= 75:
                log(f"✅ High confidence found: {MEXC_TO_KIEDEX.get(symbol, symbol)} ({confidence}%)")
                break

        except Exception as e:
            log(f"❌ Error analyzing {symbol}: {e}", "ERROR")
            continue

    return best_coin


async def open_positions(symbol: str, analysis: dict):
    """Open trade on both accounts simultaneously."""
    side = analysis.get("decision", "LONG").lower()
    tp = analysis.get("tp")
    sl = analysis.get("sl")

    log(f"🚀 Opening {side.upper()} on {MEXC_TO_KIEDEX.get(symbol, symbol)} | margin={state.margin} lev={state.leverage}x")
    log(f"   TP={tp} | SL={sl}")

    # Open on both accounts concurrently
    task_a = asyncio.create_task(execute_trade(
        ACCOUNT_A_JWT, symbol, side, state.margin, state.leverage, tp, sl
    ))
    task_b = asyncio.create_task(execute_trade(
        ACCOUNT_B_JWT, symbol, side, state.margin, state.leverage, tp, sl
    ))

    result_a, result_b = await asyncio.gather(task_a, task_b, return_exceptions=True)

    current_price = state.live_price.get(symbol, analysis.get("current_price", 0))

    if isinstance(result_a, dict):
        state.position_a = {
            "positionId": result_a["positionId"],
            "symbol": symbol,
            "kiedex_symbol": MEXC_TO_KIEDEX.get(symbol, symbol),
            "side": side,
            "entry": result_a.get("entryPriceExecuted", current_price),
            "tp": tp,
            "sl": sl,
            "margin": state.margin,
            "leverage": state.leverage,
        }
        log(f"✅ Account A: positionId={result_a['positionId']}")
    else:
        log(f"❌ Account A open failed: {result_a}", "ERROR")

    if isinstance(result_b, dict):
        state.position_b = {
            "positionId": result_b["positionId"],
            "symbol": symbol,
            "kiedex_symbol": MEXC_TO_KIEDEX.get(symbol, symbol),
            "side": side,
            "entry": result_b.get("entryPriceExecuted", current_price),
            "tp": tp,
            "sl": sl,
            "margin": state.margin,
            "leverage": state.leverage,
        }
        log(f"✅ Account B: positionId={result_b['positionId']}")
    else:
        log(f"❌ Account B open failed: {result_b}", "ERROR")

    state.current_coin = symbol
    state.status = "in_trade"
    price_feed.subscribe(symbol)
    await broadcast_state()


async def close_positions(reason: str = "bot"):
    """Close both positions simultaneously."""
    if not (state.position_a or state.position_b):
        return

    symbol = state.current_coin
    log(f"🔴 Closing positions on {MEXC_TO_KIEDEX.get(symbol, symbol)} — {reason}")

    tasks = []
    if state.position_a:
        tasks.append(asyncio.create_task(close_trade(ACCOUNT_A_JWT, state.position_a["positionId"])))
    else:
        tasks.append(asyncio.create_task(asyncio.sleep(0)))

    if state.position_b:
        tasks.append(asyncio.create_task(close_trade(ACCOUNT_B_JWT, state.position_b["positionId"])))
    else:
        tasks.append(asyncio.create_task(asyncio.sleep(0)))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    if isinstance(results[0], dict):
        pnl = results[0].get("realizedPnl", 0)
        log(f"✅ Account A closed | PnL={pnl:.4f}")
    if isinstance(results[1], dict):
        pnl = results[1].get("realizedPnl", 0)
        log(f"✅ Account B closed | PnL={pnl:.4f}")

    state.position_a = None
    state.position_b = None
    state.current_coin = None
    state.status = "idle"
    await broadcast_state()


async def hold_close_loop():
    """AI monitoring loop while in trade. Check every 30s."""
    while state.running and (state.position_a or state.position_b):
        await asyncio.sleep(30)

        if not state.running:
            break

        symbol = state.current_coin
        if not symbol:
            break

        current_price = state.live_price.get(symbol)
        if not current_price:
            continue

        # Use whichever position is still open
        pos = state.position_a or state.position_b
        entry = pos.get("entry", current_price)
        side = pos.get("side", "long")
        tp = pos.get("tp")
        sl = pos.get("sl")

        # Calculate PnL %
        if side == "long":
            pnl_pct = ((current_price - entry) / entry) * 100 * state.leverage
        else:
            pnl_pct = ((entry - current_price) / entry) * 100 * state.leverage

        log(f"📈 Monitor {MEXC_TO_KIEDEX.get(symbol, symbol)} | price={current_price:.4f} pnl={pnl_pct:.2f}%")

        # Auto close if TP/SL hit
        if tp and sl:
            if side == "long":
                if current_price >= tp:
                    log(f"🎯 TP hit! price={current_price} tp={tp}")
                    await close_positions("tp_hit")
                    return
                if current_price <= sl:
                    log(f"🛑 SL hit! price={current_price} sl={sl}")
                    await close_positions("sl_hit")
                    return
            else:
                if current_price <= tp:
                    log(f"🎯 TP hit! price={current_price} tp={tp}")
                    await close_positions("tp_hit")
                    return
                if current_price >= sl:
                    log(f"🛑 SL hit! price={current_price} sl={sl}")
                    await close_positions("sl_hit")
                    return

        # AI hold/close decision every 2 minutes
        try:
            candles_15m = await fetch_candles(symbol, "15m", 30)
            prompt = build_hold_close_prompt(
                symbol, side, entry, current_price, tp, sl, candles_15m, pnl_pct
            )
            reply = await call_qwen(prompt)
            if reply:
                result = parse_json_from_text(reply)
                if result:
                    decision = result.get("decision", "HOLD").upper()
                    reason = result.get("reason", "")
                    log(f"🤖 AI Hold/Close: {decision} — {reason}")
                    if decision == "CLOSE":
                        await close_positions(f"ai_close: {reason}")
                        return
        except Exception as e:
            log(f"⚠️ Hold/close AI error: {e}", "WARNING")

        await asyncio.sleep(90)  # Check again in 90s (total ~2min)


# ─────────────────────────────────────────────
# Main Bot Loop
# ─────────────────────────────────────────────
async def bot_loop():
    log("🤖 Bot started!")
    state.status = "idle"

    while state.running:
        try:
            # Step 1: Find best coin
            coin = await select_coin()

            if not state.running:
                break

            if not coin or not state.last_analysis:
                log("😴 No good setup found. Waiting 60s...")
                state.status = "idle"
                await broadcast_state()
                await asyncio.sleep(60)
                continue

            analysis = state.last_analysis

            # Step 2: Open on both accounts
            await open_positions(coin, analysis)

            if not state.position_a and not state.position_b:
                log("❌ Both accounts failed to open. Retrying in 30s...", "ERROR")
                state.status = "idle"
                state.current_coin = None
                await broadcast_state()
                await asyncio.sleep(30)
                continue

            # Step 3: Monitor until close
            log(f"👀 Monitoring {MEXC_TO_KIEDEX.get(coin, coin)} position...")
            await hold_close_loop()

            if not state.running:
                break

            log("✅ Trade cycle complete. Starting next scan...")
            await asyncio.sleep(5)

        except asyncio.CancelledError:
            break
        except Exception as e:
            log(f"❌ Bot loop error: {e}", "ERROR")
            await asyncio.sleep(10)

    # Cleanup
    if state.position_a or state.position_b:
        log("⚠️ Bot stopping — closing open positions...")
        await close_positions("bot_stopped")

    state.status = "idle"
    state.running = False
    log("🛑 Bot stopped.")
    await broadcast_state()


_bot_task: Optional[asyncio.Task] = None


# ─────────────────────────────────────────────
# FastAPI HTTP Server
# ─────────────────────────────────────────────
app = FastAPI(title="Kiedex Bot API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class BotConfig(BaseModel):
    margin: Optional[float] = None
    leverage: Optional[int] = None


@app.get("/")
async def root():
    return {"status": "ok", "bot_running": state.running}


@app.post("/start")
async def start_bot(config: BotConfig = None):
    global _bot_task
    if state.running:
        raise HTTPException(400, "Bot already running")
    if not ACCOUNT_A_JWT or not ACCOUNT_B_JWT:
        raise HTTPException(400, "Account JWTs not configured")
    if not QWEN_TOKEN_1:
        raise HTTPException(400, "QWEN_TOKEN_1 not configured")

    if config:
        if config.margin:
            state.margin = config.margin
        if config.leverage:
            state.leverage = config.leverage

    state.running = True
    state.status = "idle"
    _bot_task = asyncio.create_task(bot_loop(), name="bot_loop")
    log(f"🚀 Bot started | margin={state.margin} | leverage={state.leverage}x")
    await broadcast_state()
    return {"success": True, "margin": state.margin, "leverage": state.leverage}


@app.post("/stop")
async def stop_bot():
    global _bot_task
    if not state.running:
        raise HTTPException(400, "Bot not running")
    state.running = False
    if _bot_task:
        _bot_task.cancel()
        try:
            await _bot_task
        except asyncio.CancelledError:
            pass
        _bot_task = None
    log("🛑 Bot stopped by user")
    await broadcast_state()
    return {"success": True}


@app.post("/config")
async def update_config(config: BotConfig):
    if config.margin is not None:
        state.margin = config.margin
    if config.leverage is not None:
        state.leverage = config.leverage
    await broadcast_state()
    return {"success": True, "margin": state.margin, "leverage": state.leverage}


@app.get("/status")
async def get_status():
    return {
        "running": state.running,
        "status": state.status,
        "current_coin": state.current_coin,
        "margin": state.margin,
        "leverage": state.leverage,
        "position_a": state.position_a,
        "position_b": state.position_b,
        "last_analysis": state.last_analysis,
        "live_prices": state.live_price,
        "logs": state.logs[-50:],
    }


@app.get("/history")
async def get_history():
    if not ACCOUNT_A_JWT or not ACCOUNT_A_UID:
        return {"trades": []}
    trades = await get_trade_history(ACCOUNT_A_JWT, ACCOUNT_A_UID, 50)
    return {"trades": trades}


@app.get("/history/b")
async def get_history_b():
    if not ACCOUNT_B_JWT or not ACCOUNT_B_UID:
        return {"trades": []}
    trades = await get_trade_history(ACCOUNT_B_JWT, ACCOUNT_B_UID, 50)
    return {"trades": trades}


@app.post("/close")
async def manual_close():
    if not state.position_a and not state.position_b:
        raise HTTPException(400, "No open positions")
    await close_positions("manual")
    return {"success": True}


# ─────────────────────────────────────────────
# WebSocket Endpoint (FastAPI — same port as HTTP)
# ─────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    state.ws_clients.add(websocket)
    log(f"🔌 Frontend connected ({len(state.ws_clients)} clients)")
    try:
        # Send current state on connect
        await websocket.send_json({
            "type": "state",
            "running": state.running,
            "status": state.status,
            "current_coin": state.current_coin,
            "margin": state.margin,
            "leverage": state.leverage,
            "position_a": state.position_a,
            "position_b": state.position_b,
            "live_price": state.live_price,
            "last_analysis": state.last_analysis,
        })
        # Send recent logs
        await websocket.send_json({
            "type": "logs",
            "logs": state.logs[-50:],
        })
        # Keep connection alive, handle incoming messages
        while True:
            try:
                data = await websocket.receive_json()
                if data.get("cmd") == "ping":
                    await websocket.send_json({"type": "pong"})
            except Exception:
                break
    except WebSocketDisconnect:
        pass
    finally:
        state.ws_clients.discard(websocket)


# ─────────────────────────────────────────────
# Startup / Shutdown
# ─────────────────────────────────────────────
@app.on_event("startup")
async def startup():
    log("⚡ Kiedex Bot backend starting...")
    await price_feed.start()
    # Subscribe all available coins for live prices
    for sym in AVAILABLE_COINS:
        price_feed.subscribe(sym)
    log(f"📡 Subscribed to {len(AVAILABLE_COINS)} coins")
    log(f"✅ Backend ready | HTTP+WS on port {HTTP_PORT}")


@app.on_event("shutdown")
async def shutdown():
    state.running = False
    await price_feed.stop()
    log("🛑 Backend shutdown")


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=HTTP_PORT,
        log_level="info",
        reload=False,
    )
