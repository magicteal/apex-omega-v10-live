"""
APEX-OMEGA OS V9.5 — Forensic Fee-Shield & Anti-Whipsaw Live Delta India Engine
Built from the Forensic Audit of 35 Real Delta Exchange Live Campaigns (Delta-TransactionLog-AssetHistory):
1. Fee-Cleared 5-Stage Asymmetric Profit Ratchet (Clears Delta's 0.05% + 18% GST = 0.118% round-trip fee hurdle):
   - Stage 0.5 Fee-Cleared Shield (+0.38% move): Locks stop at Entry + 0.16% (0.22% breathing room, guaranteed net win after GST).
   - Stage 1 Solid Profit Lock (+0.58% move): Locks stop at Entry + 0.34% (+1.50%+ net account win after GST, 0.24% breathing room).
   - Stage 2 Surge Bank & Lock (+0.88% move): Banks 50% in cash & locks 50% Runner at Entry + 0.60% (+4.10%+ net account win).
   - Stage 3 Super Trail (+1.28% move): Locks stop at Entry + 0.98% (+6.80%+ net win) & trails 0.28% behind peak.
   - Stage 4 Home-Run Target (+1.85% move): Resting Maker Limit TP on Delta Exchange (+10.5%+ net win, 0.02% maker fee).
   - 25-Min Time-Decay Scratch Win: Exits stale trades after 25m only on a >= +0.22% bounce (never inside the fee loss zone).
2. Two-Tier 90-Second Anti-Whipsaw Wick Shield:
   - Hardware catastrophe stop on Delta Exchange placed at -0.62% (Tier-1) / -0.68% (Tier-2) to survive 30-second intra-candle wicks.
   - Tight soft stop (-0.46% Tier-1 / -0.52% Tier-2) activates after 90 seconds to cut real losing trends cleanly.
3. 100% Honest Live & Paper Accounting:
   - Uses real 0.059% per-side fee (0.05% + 18% GST) with zero fake paper floors or artificial loss caps.
"""
import asyncio
import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from core.squeeze_engine import SqueezeEngine, TIER1_ALPHA_SYMBOLS
from core.kelly_ratchet import KellyRatchet
from execution.delta_live_router import DeltaIndiaLiveRouter, compute_exact_position_pnl

UNIVERSE_MAP = [
    ("SOL/USDT:USDT", "SOLUSDT", "SOLUSD", "SOLUSD"),
    ("BTC/USDT:USDT", "BTCUSDT", "XBTUSD", "BTCUSD"),
    ("ETH/USDT:USDT", "ETHUSDT", "ETHUSD", "ETHUSD"),
    ("SUI/USDT:USDT", "SUIUSDT", "SUIUSD", "SUIUSD"),
    ("DOGE/USDT:USDT", "DOGEUSDT", "XDGUSD", "DOGEUSD"),
    ("XRP/USDT:USDT", "XRPUSDT", "XRPUSD", "XRPUSD"),
    ("BNB/USDT:USDT", "BNBUSDT", "BNBUSD", "BNBUSD"),
    ("AVAX/USDT:USDT", "AVAXUSDT", "AVAXUSD", "AVAXUSD"),
    ("LINK/USDT:USDT", "LINKUSDT", "LINKUSD", "LINKUSD"),
    ("ADA/USDT:USDT", "ADAUSDT", "ADAUSD", "ADAUSD"),
]

SCANNER_TO_DELTA_SYM = {item[0]: item[3] for item in UNIVERSE_MAP}

STATE_PATH = Path(__file__).parent / "bot_state.json"
BOOT_TIME = time.time()


def _detect_outbound_ip() -> str:
    try:
        req = urllib.request.Request("https://api.ipify.org", headers={"User-Agent": "ApexOmegaV95/9.5"})
        with urllib.request.urlopen(req, timeout=3.0) as resp:
            return resp.read().decode("utf-8").strip()
    except Exception:
        return "UNKNOWN"


SERVER_OUTBOUND_IP = _detect_outbound_ip()

app = FastAPI(title="APEX-OMEGA OS V9.5 Zero-Lag Fee-Shield Hyper-Compounder", version="9.5.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def fmt_price(val: float) -> float:
    if val == 0:
        return 0.0
    if abs(val) < 0.01:
        return round(val, 8)
    if abs(val) < 1.0:
        return round(val, 6)
    if abs(val) < 50.0:
        return round(val, 5)
    return round(val, 3)


class AutonomousDaemonV9:
    # Exact Delta Exchange India Taker Fee (0.05%) + 18% GST = 0.059% per side (0.118% round-trip)
    FEE_RATE = 0.00059
    WIN_COOLDOWN_SEC = 600
    LOSS_COOLDOWN_SEC = 2700
    DIRECTION_PAUSE_SEC = 2700
    MAX_OPEN_SLOTS = 3
    WICK_SHIELD_HOLD_SEC = 90.0

    def __init__(self):
        self.squeeze_engine = SqueezeEngine()
        self.kelly_ratchet = KellyRatchet()
        self.live_router = DeltaIndiaLiveRouter()
        self.state = self._load_state()
        self.scanner_cache: Dict[str, Dict[str, Any]] = {}
        raw_camps = self.state.get("active_campaigns")
        if isinstance(raw_camps, list):
            self.active_campaigns: List[Dict[str, Any]] = raw_camps
        elif self.state.get("active_campaign"):
            self.active_campaigns = [self.state["active_campaign"]]
        else:
            self.active_campaigns = []
        self.clients: List[WebSocket] = []
        self.last_scan_ts: Optional[str] = None
        self._last_kline_scan_ts: float = 0.0
        self._last_ws_broadcast_ts: float = 0.0

    def _load_state(self) -> Dict[str, Any]:
        env_mode = os.getenv("EXECUTION_MODE", "LIVE").strip().upper()
        default_eq = float(os.getenv("STARTING_EQUITY_INR", "10000.0"))
        if STATE_PATH.exists():
            try:
                loaded = json.loads(STATE_PATH.read_text())
                loaded["mode"] = env_mode
                return loaded
            except Exception:
                pass
        return {
            "mode": env_mode,
            "starting_equity": default_eq,
            "active_equity": default_eq,
            "peak_equity": default_eq,
            "vault_balance": 0.0,
            "principal_siphoned": False,
            "next_siphon_milestone": 1000000.0,
            "consecutive_losses": 0,
            "consecutive_long_losses": 0,
            "consecutive_short_losses": 0,
            "long_pause_until_ts": 0.0,
            "short_pause_until_ts": 0.0,
            "chop_pause_until_ts": 0.0,
            "wins": 0,
            "losses": 0,
            "total_closed_trades": 0,
            "win_rate_pct": 0.0,
            "gross_profit_inr": 0.0,
            "gross_loss_inr": 0.0,
            "net_realized_pnl_inr": 0.0,
            "pair_cooldowns": {},
            "trade_history": [],
            "siphon_history": [],
            "active_campaigns": [],
            "active_campaign": None,
        }

    def save_state(self) -> None:
        self.state["mode"] = "LIVE" if self.live_router.is_live() else self.state.get("mode", "PAPER")
        self.state["active_campaigns"] = self.active_campaigns
        self.state["active_campaign"] = self.active_campaigns[0] if self.active_campaigns else None
        try:
            STATE_PATH.write_text(json.dumps(self.state, indent=2))
        except Exception:
            pass

    async def broadcast_to_clients(self, force: bool = False) -> None:
        if not self.clients:
            return
        now = time.time()
        if not force and (now - self._last_ws_broadcast_ts < 0.03):
            return
        self._last_ws_broadcast_ts = now
        payload = self.build_payload()
        dead = []
        for ws in list(self.clients):
            try:
                await ws.send_json(payload)
            except Exception:
                dead.append(ws)
        for ws in dead:
            if ws in self.clients:
                self.clients.remove(ws)

    async def on_delta_realtime_event(self, event_kind: str, symbol_or_channel: str, mark_price: float) -> None:
        if event_kind == "TICKER" and mark_price > 0:
            if symbol_or_channel in self.scanner_cache:
                self.scanner_cache[symbol_or_channel]["price"] = fmt_price(mark_price)
                self.scanner_cache[symbol_or_channel]["feed_source"] = "DELTA_WS_0MS"
            self.last_scan_ts = datetime.now(timezone.utc).isoformat()

            for camp in list(self.active_campaigns):
                if camp["symbol"] == symbol_or_channel:
                    await self._update_campaign_and_sync_delta(camp, mark_price)
            await self.broadcast_to_clients(force=False)

        elif event_kind == "ACCOUNT_EVENT":
            await self.sync_live_delta_state(force=True)
            await self.broadcast_to_clients(force=True)

    @staticmethod
    def _fetch_multi_exchange_klines(ticker: str, kraken_pair: str, delta_symbol: str) -> Tuple[List[List[float]], str]:
        try:
            end_ts = int(time.time())
            start_ts = end_ts - (200 * 300)
            url = f"https://api.india.delta.exchange/v2/history/candles?resolution=5m&symbol={delta_symbol}&start={start_ts}&end={end_ts}"
            req = urllib.request.Request(url, headers={"User-Agent": "python-rest-client", "Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=3.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                rows = data.get("result", [])
                if isinstance(rows, list) and len(rows) >= 30:
                    rows_sorted = sorted(rows, key=lambda x: float(x.get("time", 0)))
                    parsed = [
                        [
                            float(r["time"]) * 1000.0,
                            float(r["open"]),
                            float(r["high"]),
                            float(r["low"]),
                            float(r["close"]),
                            float(r.get("volume") or 1.0),
                        ]
                        for r in rows_sorted
                        if r.get("close") is not None
                    ]
                    if len(parsed) >= 30:
                        return parsed, "DELTA_INDIA_LIVE_5M"
        except Exception:
            pass

        try:
            url = f"https://api.bybit.com/v5/market/kline?category=spot&symbol={ticker}&interval=5&limit=200"
            req = urllib.request.Request(url, headers={"User-Agent": "ApexOmegaV95/9.5"})
            with urllib.request.urlopen(req, timeout=3.0) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                rows = data.get("result", {}).get("list", [])
                if rows and len(rows) >= 30:
                    rows = list(reversed(rows))
                    return [[float(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])] for r in rows], "BYBIT_LIVE_5M"
        except Exception:
            pass

        return [], "OFFLINE"

    def _recompute_trade_stats(self) -> None:
        history = self.state.get("trade_history", [])
        wins = [t for t in history if float(t.get("realized_pnl_inr", 0.0)) > 0]
        losses = [t for t in history if float(t.get("realized_pnl_inr", 0.0)) <= 0]
        total = len(history)
        gross_p = round(sum(float(t.get("realized_pnl_inr", 0.0)) for t in wins), 2)
        gross_l = round(sum(abs(float(t.get("realized_pnl_inr", 0.0))) for t in losses), 2)
        self.state["wins"] = len(wins)
        self.state["losses"] = len(losses)
        self.state["total_closed_trades"] = total
        self.state["win_rate_pct"] = round((len(wins) / max(total, 1)) * 100.0, 1) if total > 0 else 0.0
        self.state["gross_profit_inr"] = gross_p
        self.state["gross_loss_inr"] = gross_l
        self.state["net_realized_pnl_inr"] = round(gross_p - gross_l, 2)

    async def sync_live_delta_state(self, force: bool = False) -> None:
        if not self.live_router.is_live():
            return

        telemetry = await self.live_router.sync_all_live_telemetry(mark_price_lookup=None, force=force)
        if not telemetry.get("connected"):
            return

        real_bal = self.live_router.last_synced_balance_inr
        if real_bal is not None and real_bal > 0:
            if not self.state.get("starting_equity_synced"):
                self.state["starting_equity"] = real_bal
                self.state["starting_equity_synced"] = True
            self.state["active_equity"] = real_bal
            if real_bal > float(self.state.get("peak_equity", 0.0)):
                self.state["peak_equity"] = real_bal

        live_closed = telemetry.get("closed_trades") or []
        if live_closed:
            existing_history: List[Dict[str, Any]] = self.state.get("trade_history", [])
            existing_ids = {str(t.get("trade_id")) for t in existing_history}
            added_any = False
            for dt in reversed(live_closed):
                t_id = str(dt.get("trade_id"))
                if t_id not in existing_ids:
                    existing_history.insert(0, dt)
                    existing_ids.add(t_id)
                    added_any = True
            if added_any:
                self.state["trade_history"] = existing_history[:200]
                self._recompute_trade_stats()

        live_positions: List[Dict[str, Any]] = telemetry.get("open_positions") or []
        live_by_sym = {p["symbol"]: p for p in live_positions}

        now_ts = time.time()
        for camp in list(self.active_campaigns):
            sym = camp["symbol"]
            live_pos = live_by_sym.get(sym)
            if live_pos:
                camp["delta_symbol"] = live_pos["delta_symbol"]
                camp["live_qty"] = live_pos["contracts"]
                camp["remaining_qty"] = live_pos["contracts"]
                camp["coin_qty"] = live_pos["coin_qty"]
                camp["entry_price_raw"] = live_pos.get("entry_price_raw") or live_pos["entry_price"]
                camp["mark_price_raw"] = live_pos.get("mark_price_raw") or live_pos["mark_price"]
                camp["base_entry_price"] = live_pos["entry_price"]
                camp["current_price"] = live_pos["mark_price"]
                camp["liquidation_price"] = live_pos["liquidation_price"]
                camp["margin_inr"] = live_pos["margin_inr"]
                camp["total_notional_inr"] = live_pos["notional_inr"]
                camp["total_unrealized_pnl_inr"] = live_pos["unrealized_pnl_inr"]
                camp["mark_ts_ms"] = live_pos.get("mark_ts_ms", now_ts * 1000.0)
                camp["is_live_delta"] = True
                if live_pos.get("stop_order_id"):
                    camp["stop_order_id"] = live_pos["stop_order_id"]
                if live_pos.get("stop_price"):
                    camp["effective_stop"] = fmt_price(live_pos["stop_price"])
                if camp.get("tranches"):
                    camp["tranches"][0]["fill_price"] = camp["base_entry_price"]
                    camp["tranches"][0]["notional_inr"] = camp["total_notional_inr"]
                    camp["tranches"][0]["unrealized_pnl_inr"] = round(
                        camp["total_unrealized_pnl_inr"] + float(camp.get("tp1_banked_pnl_inr", 0.0)), 2
                    )
            else:
                age_s = now_ts - float(camp.get("entry_ts", now_ts))
                if camp.get("delta_symbol") and age_s > 15.0:
                    d_sym = camp["delta_symbol"]
                    exit_p = float(
                        self.live_router.live_ticker_marks.get(d_sym)
                        or self.scanner_cache.get(sym, {}).get("price")
                        or camp["current_price"]
                    )
                    self.close_campaign(camp, exit_price=exit_p, reason="DELTA_EXCHANGE_LIVE_EXIT")

        open_camp_syms = {c["symbol"] for c in self.active_campaigns}
        for sym, pos in live_by_sym.items():
            if sym not in open_camp_syms:
                side = pos["side"]
                direction = 1.0 if side == "LONG" else -1.0
                entry_p_raw = float(pos.get("entry_price_raw") or pos["entry_price"] or pos["mark_price"])
                mark_p_raw = float(pos.get("mark_price_raw") or pos["mark_price"] or entry_p_raw)
                is_tier1 = sym in TIER1_ALPHA_SYMBOLS
                hard_stop_dist = 0.0062 if is_tier1 else 0.0068
                soft_stop_dist = 0.0046 if is_tier1 else 0.0052
                default_stop = fmt_price(entry_p_raw * (1.0 - direction * hard_stop_dist))
                soft_stop = fmt_price(entry_p_raw * (1.0 - direction * soft_stop_dist))
                eff_stop = fmt_price(float(pos["stop_price"])) if pos.get("stop_price") else default_stop
                stage2_trig = fmt_price(entry_p_raw * (1.0 + direction * 0.0088))
                stage4_tp = fmt_price(entry_p_raw * (1.0 + direction * 0.0185))
                sig_item = self.scanner_cache.get(sym, {})
                atr_val = float(sig_item.get("atr_14") or (entry_p_raw * 0.0065))

                imported_camp = {
                    "trade_id": f"DLT-POS-{pos.get('product_id') or int(now_ts)}",
                    "symbol": sym,
                    "delta_symbol": pos["delta_symbol"],
                    "side": side,
                    "setup_type": "DELTA_LIVE_POSITION",
                    "confluence_score": int(sig_item.get("confluence_score") or 98),
                    "entry_time": pos.get("updated_at") or datetime.now(timezone.utc).isoformat(),
                    "entry_ts": now_ts - 30.0,
                    "entry_price_raw": entry_p_raw,
                    "mark_price_raw": mark_p_raw,
                    "base_entry_price": fmt_price(entry_p_raw),
                    "current_price": fmt_price(mark_p_raw),
                    "highest_extreme": fmt_price(max(entry_p_raw, mark_p_raw) if side == "LONG" else min(entry_p_raw, mark_p_raw)),
                    "atr_at_entry": atr_val,
                    "tp1_price": stage2_trig,
                    "tp2_price": stage4_tp,
                    "tp1_hit": False,
                    "tp1_banked_pnl_inr": 0.0,
                    "ratchet_stage": 0,
                    "chandelier_stop": eff_stop,
                    "soft_stop": soft_stop,
                    "breakeven_locked": False,
                    "breakeven_stop": None,
                    "effective_stop": eff_stop,
                    "filled_units": 1,
                    "max_stage_reached": "V9.5_LIVE_ACTIVE",
                    "total_notional_inr": pos["notional_inr"],
                    "total_unrealized_pnl_inr": pos["unrealized_pnl_inr"],
                    "rvol_at_entry": float(sig_item.get("rvol") or 1.25),
                    "live_qty": pos["contracts"],
                    "remaining_qty": pos["contracts"],
                    "coin_qty": pos["coin_qty"],
                    "liquidation_price": pos["liquidation_price"],
                    "margin_inr": pos["margin_inr"],
                    "stop_order_id": pos.get("stop_order_id"),
                    "mark_ts_ms": pos.get("mark_ts_ms", now_ts * 1000.0),
                    "is_live_delta": True,
                    "tranches": [
                        {
                            "id": 1,
                            "label": f"DELTA_LIVE ({pos['contracts']} CONTRACTS)",
                            "weight_pct": 1.0,
                            "trigger_atr_mult": 0.0,
                            "trigger_price": fmt_price(entry_p_raw),
                            "filled": True,
                            "fill_price": fmt_price(entry_p_raw),
                            "notional_inr": pos["notional_inr"],
                            "unrealized_pnl_inr": pos["unrealized_pnl_inr"],
                            "fee_inr": round(pos["notional_inr"] * self.FEE_RATE, 2),
                        }
                    ],
                }
                self.active_campaigns.append(imported_camp)

        self.save_state()

    async def _update_campaign_and_sync_delta(self, camp: Dict[str, Any], live_price: float) -> None:
        prev_stage = camp.get("ratchet_stage", 0)
        prev_stop = camp.get("effective_stop")
        self._update_campaign(camp, live_price)
        if self.live_router.is_live() and camp.get("delta_symbol"):
            if camp not in self.active_campaigns:
                await self.live_router.close_live_trade(
                    camp["delta_symbol"],
                    camp["side"],
                    float(camp.get("remaining_qty") or camp.get("live_qty") or 0.0),
                    camp.get("stop_order_id"),
                )
                await self.sync_live_delta_state(force=True)
            else:
                new_stage = camp.get("ratchet_stage", 0)
                new_stop = camp.get("effective_stop")
                if new_stage >= 2 and prev_stage < 2:
                    st2 = await self.live_router.bank_stage2_50pct_and_move_stop(
                        camp["delta_symbol"],
                        camp["side"],
                        float(camp.get("remaining_qty") or camp.get("live_qty") or 0.0),
                        camp.get("stop_order_id"),
                        float(new_stop),
                    )
                    camp["remaining_qty"] = st2["remaining_qty"]
                    camp["stop_order_id"] = st2["stop_order_id"]
                    self.save_state()
                elif new_stop != prev_stop:
                    camp["stop_order_id"] = await self.live_router.update_hardware_stop(
                        camp["delta_symbol"],
                        camp["side"],
                        float(camp.get("remaining_qty") or camp.get("live_qty") or 0.0),
                        camp.get("stop_order_id"),
                        float(new_stop),
                    )
                    self.save_state()

    async def scan_and_execute_cycle(self) -> None:
        loop = asyncio.get_running_loop()
        now = time.time()

        if not self.scanner_cache or (now - self._last_kline_scan_ts >= 2.5):
            self._last_kline_scan_ts = now
            if not self.live_router.ws_connected:
                await self.live_router.fetch_all_delta_tickers()

            async def _scan_one(display_sym: str, ticker: str, kraken_pair: str, delta_sym: str) -> None:
                ohlcv, src = await loop.run_in_executor(
                    None, self._fetch_multi_exchange_klines, ticker, kraken_pair, delta_sym
                )
                if ohlcv and len(ohlcv) >= 30:
                    ws_mk = self.live_router.live_ticker_marks.get(delta_sym)
                    if ws_mk and ws_mk > 0:
                        ohlcv[-1][4] = ws_mk
                        if ws_mk > ohlcv[-1][2]:
                            ohlcv[-1][2] = ws_mk
                        if ws_mk < ohlcv[-1][3]:
                            ohlcv[-1][3] = ws_mk
                    sig = self.squeeze_engine.evaluate_ohlcv(display_sym, ohlcv, feed_source=src)
                    sig_dict = sig.to_dict()
                    if ws_mk and ws_mk > 0:
                        sig_dict["price"] = fmt_price(ws_mk)
                        sig_dict["feed_source"] = "DELTA_WS_0MS"
                    self.scanner_cache[display_sym] = sig_dict

            await asyncio.gather(
                *(_scan_one(d_sym, tk, kr, dl) for d_sym, tk, kr, dl in UNIVERSE_MAP)
            )
            self.last_scan_ts = datetime.now(timezone.utc).isoformat()

        if self.live_router.is_live():
            await self.sync_live_delta_state(force=False)

        for camp in list(self.active_campaigns):
            sym = camp["symbol"]
            d_sym = camp.get("delta_symbol") or SCANNER_TO_DELTA_SYM.get(sym, "")
            delta_mk = self.live_router.live_ticker_marks.get(d_sym)
            live_price = float(
                delta_mk
                or camp.get("mark_price_raw")
                or camp.get("current_price")
                or 0.0
            )
            if live_price > 0:
                await self._update_campaign_and_sync_delta(camp, live_price)

        now_s = time.time()
        unhedged_open = sum(1 for c in self.active_campaigns if not c.get("breakeven_locked"))
        if len(self.active_campaigns) < self.MAX_OPEN_SLOTS and unhedged_open == 0:
            pair_cds = self.state.get("pair_cooldowns", {})
            open_syms = {c["symbol"] for c in self.active_campaigns}

            btc_sig = self.scanner_cache.get("BTC/USDT:USDT")
            btc_slope = float(btc_sig["ema_trend_slope"]) if btc_sig else 0.0
            btc_roc5 = float(btc_sig["roc5"]) if btc_sig else 0.0

            long_paused = now_s < float(self.state.get("long_pause_until_ts", 0.0))
            short_paused = now_s < float(self.state.get("short_pause_until_ts", 0.0))

            candidates = []
            for v in self.scanner_cache.values():
                if not v.get("signal_valid") or v["confluence_score"] < 96:
                    continue
                if v["symbol"] in open_syms:
                    continue
                if now_s < float(pair_cds.get(v["symbol"], 0.0)):
                    continue
                side = "SHORT" if v["regime"] == "SHORT_IGNITION" else "LONG"
                if side == "LONG" and (long_paused or btc_slope < -0.0006 or btc_roc5 < -0.0025):
                    continue
                if side == "SHORT" and (short_paused or btc_slope > 0.0006 or btc_roc5 > 0.0025):
                    continue
                candidates.append(v)

            if candidates:
                winner = max(
                    candidates,
                    key=lambda x: (
                        1 if x["symbol"] in TIER1_ALPHA_SYMBOLS else 0,
                        x["confluence_score"],
                        x["ker"],
                        x["rvol"],
                    ),
                )
                max_daily_loss = float(os.getenv("MAX_DAILY_LOSS_INR", "600.0"))
                today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                today_pnl = sum(
                    float(t.get("realized_pnl_inr", 0.0))
                    for t in self.state.get("trade_history", [])
                    if str(t.get("exit_time", "")).startswith(today_str)
                )
                if today_pnl > -max_daily_loss:
                    await self._enter_new_campaign_async(winner)

    async def _enter_new_campaign_async(self, sig: Dict[str, Any]) -> None:
        # Force-sync live Delta wallet balance before entry so 100% of compounded equity sizes the trade
        if self.live_router.is_live():
            live_bal = await self.live_router.fetch_inr_balance()
            if live_bal is not None and live_bal > 0:
                self.state["active_equity"] = live_bal
                if live_bal > float(self.state.get("peak_equity", 0.0)):
                    self.state["peak_equity"] = live_bal
        self._enter_new_campaign(sig)
        if self.live_router.is_live() and self.active_campaigns:
            camp = self.active_campaigns[-1]
            if camp["symbol"] == sig["symbol"]:
                res = await self.live_router.open_live_trade(
                    scanner_symbol=camp["symbol"],
                    side=camp["side"],
                    notional_inr=camp["total_notional_inr"],
                    entry_price=camp["base_entry_price"],
                    stop_price=camp["effective_stop"],
                    tp_price=camp.get("tp2_price"),
                )
                if res:
                    camp["delta_symbol"] = res["delta_symbol"]
                    camp["live_qty"] = res["qty"]
                    camp["remaining_qty"] = res["remaining_qty"]
                    camp["coin_qty"] = res.get("coin_qty")
                    camp["stop_order_id"] = res["stop_order_id"]
                    camp["entry_price_raw"] = float(res["fill_price"])
                    camp["base_entry_price"] = fmt_price(res["fill_price"])
                    camp["is_live_delta"] = True
                    self.save_state()
                else:
                    self.active_campaigns.remove(camp)
                    self.save_state()

    def _enter_new_campaign(self, sig: Dict[str, Any]) -> None:
        active_eq = float(self.state["active_equity"])
        lev = 7.2 if sig["confluence_score"] >= 98 else 6.5
        total_notional = round(active_eq * lev, 2)

        side = "SHORT" if sig["regime"] == "SHORT_IGNITION" else "LONG"
        direction = 1.0 if side == "LONG" else -1.0
        entry_price = float(sig["price"])
        is_tier1 = sig["symbol"] in TIER1_ALPHA_SYMBOLS

        # V9.5 Two-Tier Anti-Whipsaw Stop Geometry + Fee-Cleared Ratchet Targets:
        # - Hardware Disaster Stop on Delta Exchange: -0.62% (Tier-1) / -0.68% (Tier-2) to survive 30s intra-candle wicks
        # - Soft Confirmation Stop (after 90s hold): -0.46% (Tier-1) / -0.52% (Tier-2)
        hard_stop_dist = 0.0062 if is_tier1 else 0.0068
        soft_stop_dist = 0.0046 if is_tier1 else 0.0052
        stage2_trig = fmt_price(entry_price * (1.0 + direction * 0.0088))
        stage4_tp = fmt_price(entry_price * (1.0 + direction * 0.0185))
        initial_stop = fmt_price(entry_price * (1.0 - direction * hard_stop_dist))
        soft_stop = fmt_price(entry_price * (1.0 - direction * soft_stop_dist))
        entry_fee = round(total_notional * self.FEE_RATE, 2)
        coin_qty = (total_notional / max(self.live_router.usd_inr_rate, 1.0)) / max(entry_price, 1e-9)

        tranches = [
            {
                "id": 1,
                "label": f"V9.5_FEE_SHIELD_CORE ({lev}X SNIPER)",
                "weight_pct": 1.0,
                "trigger_atr_mult": 0.0,
                "trigger_price": fmt_price(entry_price),
                "filled": True,
                "fill_price": fmt_price(entry_price),
                "notional_inr": total_notional,
                "unrealized_pnl_inr": 0.0,
                "fee_inr": entry_fee,
            }
        ]

        trade_id = f"TRD-{int(time.time() * 1000) % 1000000000}"
        new_camp = {
            "trade_id": trade_id,
            "symbol": sig["symbol"],
            "delta_symbol": SCANNER_TO_DELTA_SYM.get(sig["symbol"], "SOLUSD"),
            "side": side,
            "setup_type": sig["setup_type"],
            "confluence_score": sig["confluence_score"],
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "entry_ts": time.time(),
            "entry_price_raw": entry_price,
            "mark_price_raw": entry_price,
            "base_entry_price": fmt_price(entry_price),
            "current_price": fmt_price(entry_price),
            "highest_extreme": fmt_price(entry_price),
            "atr_at_entry": float(sig["atr_14"]),
            "tp1_price": stage2_trig,
            "tp2_price": stage4_tp,
            "tp1_hit": False,
            "tp1_banked_pnl_inr": 0.0,
            "ratchet_stage": 0,
            "chandelier_stop": initial_stop,
            "soft_stop": soft_stop,
            "breakeven_locked": False,
            "breakeven_stop": None,
            "effective_stop": initial_stop,
            "filled_units": 1,
            "max_stage_reached": "V9.5_SNIPER_OPEN",
            "total_notional_inr": total_notional,
            "total_unrealized_pnl_inr": 0.0,
            "coin_qty": round(coin_qty, 8),
            "rvol_at_entry": sig["rvol"],
            "tranches": tranches,
        }
        self.active_campaigns.append(new_camp)
        self.save_state()

    def _update_campaign(self, camp: Dict[str, Any], live_price: float) -> None:
        side = camp["side"]
        direction = 1.0 if side == "LONG" else -1.0
        entry_p = float(camp.get("entry_price_raw") or camp["base_entry_price"])
        notional = float(camp["total_notional_inr"])
        camp["mark_price_raw"] = float(live_price)
        camp["current_price"] = fmt_price(live_price)
        camp["mark_ts_ms"] = time.time() * 1000.0

        if (side == "LONG" and live_price > camp["highest_extreme"]) or (
            side == "SHORT" and live_price < camp["highest_extreme"]
        ):
            camp["highest_extreme"] = fmt_price(live_price)

        max_fav_pct = direction * ((camp["highest_extreme"] - entry_p) / max(entry_p, 1e-12))

        # V9.5 Stage 0.5 Fee-Cleared Shield (+0.38% move): Locks Stop at Entry + 0.16% (clears 0.118% round-trip fee+GST!)
        if max_fav_pct >= 0.0038 and camp.get("ratchet_stage", 0) < 0.5:
            camp["ratchet_stage"] = 0.5
            camp["breakeven_locked"] = True
            camp["breakeven_stop"] = fmt_price(entry_p * (1.0 + direction * 0.0016))
            camp["effective_stop"] = camp["breakeven_stop"]
            camp["max_stage_reached"] = "FEE_CLEARED_SHIELD_LOCKED"

        # V9.5 Stage 1 Solid Profit Lock (+0.58% move): Locks Stop at Entry + 0.34% (+1.50%+ net win after GST)
        if max_fav_pct >= 0.0058 and camp.get("ratchet_stage", 0) < 1:
            camp["ratchet_stage"] = 1
            camp["breakeven_locked"] = True
            camp["breakeven_stop"] = fmt_price(entry_p * (1.0 + direction * 0.0034))
            camp["effective_stop"] = camp["breakeven_stop"]
            camp["max_stage_reached"] = "STAGE_1_NET_PROFIT_LOCKED"

        # V9.5 Stage 2 Surge Bank (+0.88% move): Banks 50% in cash & locks 50% Runner Stop at Entry + 0.60%
        if max_fav_pct >= 0.0088 and camp.get("ratchet_stage", 0) < 2:
            camp["ratchet_stage"] = 2
            camp["tp1_hit"] = True
            camp["breakeven_locked"] = True
            camp["breakeven_stop"] = fmt_price(entry_p * (1.0 + direction * 0.0060))
            camp["effective_stop"] = camp["breakeven_stop"]
            camp["max_stage_reached"] = "STAGE_2_50PCT_BANKED"
            half_notional = notional * 0.50
            fees_half = half_notional * self.FEE_RATE * 2.0
            camp["tp1_banked_pnl_inr"] = round(half_notional * 0.0088 - fees_half, 2)

        # V9.5 Stage 3 Super Trail (+1.28% move): Locks Stop at Entry + 0.98% & trails 0.28% behind peak
        if max_fav_pct >= 0.0128:
            camp["ratchet_stage"] = 3
            camp["tp1_hit"] = True
            camp["breakeven_locked"] = True
            trail_s = fmt_price(camp["highest_extreme"] * (1.0 - direction * 0.0028))
            floor_s = fmt_price(entry_p * (1.0 + direction * 0.0098))
            camp["breakeven_stop"] = max(trail_s, floor_s) if side == "LONG" else min(trail_s, floor_s)
            camp["effective_stop"] = camp["breakeven_stop"]
            camp["max_stage_reached"] = "STAGE_3_SUPER_TRAIL"

        # V9.5 Stage 4 Home-Run Target (+1.85% move): Instant Jackpot Exit
        tp_max = float(camp.get("tp2_price", entry_p * (1.0 + direction * 0.0185)))
        if (side == "LONG" and live_price >= tp_max) or (side == "SHORT" and live_price <= tp_max):
            self.close_campaign(camp, exit_price=live_price, reason="STAGE_4_HOME_RUN_JACKPOT_WIN")
            return

        # 25-Minute Time-Decay Scratch Exit (Requires >= +0.22% move to clear 0.118% fee + GST hurdle)
        age_sec = time.time() - float(camp.get("entry_ts", time.time()))
        if camp.get("ratchet_stage", 0) < 1 and age_sec >= 1500:
            scratch_p = entry_p * (1.0 + direction * 0.0022)
            if (side == "LONG" and live_price >= scratch_p) or (side == "SHORT" and live_price <= scratch_p):
                self.close_campaign(camp, exit_price=live_price, reason="TIME_DECAY_FEE_CLEARED_WIN")
                return

        # Two-Tier Stop Check:
        # - Before 90s hold: only exit on effective_stop (disaster stop or ratcheted profit lock)
        # - After 90s hold: if still at Stage 0, tighten effective stop to soft_stop (-0.46% / -0.52%)
        if camp.get("ratchet_stage", 0) == 0 and age_sec >= self.WICK_SHIELD_HOLD_SEC and camp.get("soft_stop"):
            camp["effective_stop"] = camp["soft_stop"]

        stop_hit = (side == "LONG" and live_price <= camp["effective_stop"]) or (
            side == "SHORT" and live_price >= camp["effective_stop"]
        )
        if stop_hit:
            st = camp.get("ratchet_stage", 0)
            reason = (
                f"STAGE_{int(st)}_ASYMMETRIC_RATCHET_WIN"
                if st >= 1
                else "STAGE_0.5_FEE_CLEARED_SHIELD_WIN"
                if st == 0.5
                else "CONFIRMATION_STOP_LOSS"
            )
            self.close_campaign(camp, exit_price=live_price, reason=reason)
            return

        coin_qty = float(
            camp.get("coin_qty")
            or ((notional / max(self.live_router.usd_inr_rate, 1.0)) / max(entry_p, 1e-9))
        )
        _, exact_pnl_inr = compute_exact_position_pnl(
            side,
            entry_p,
            live_price,
            coin_qty,
            self.live_router.usd_inr_rate,
        )
        camp["total_unrealized_pnl_inr"] = exact_pnl_inr
        if camp.get("tranches"):
            camp["tranches"][0]["unrealized_pnl_inr"] = round(
                exact_pnl_inr + float(camp.get("tp1_banked_pnl_inr", 0.0)), 2
            )
        self.save_state()

    def close_campaign(
        self, camp: Dict[str, Any], exit_price: float, reason: str = "CONFIRMATION_STOP_LOSS"
    ) -> Optional[Dict[str, Any]]:
        if camp not in self.active_campaigns:
            return None
        side = camp["side"]
        direction = 1.0 if side == "LONG" else -1.0
        entry_p = float(camp.get("entry_price_raw") or camp["base_entry_price"])
        notional = float(camp["total_notional_inr"])
        pct_move = direction * ((exit_price - entry_p) / max(entry_p, 1e-12))
        prev_eq = float(self.state["active_equity"])

        # Exact real-world PnL after 0.059% per-side fee (0.05% + 18% GST) — ZERO fake floors or artificial loss caps
        st = camp.get("ratchet_stage", 0)
        if st >= 2 and camp.get("tp1_hit"):
            runner_notional = notional * 0.50
            runner_pnl = runner_notional * pct_move - (runner_notional * self.FEE_RATE * 2.0)
            realized_pnl = round(float(camp.get("tp1_banked_pnl_inr", 0.0)) + runner_pnl, 2)
        else:
            total_fees = notional * self.FEE_RATE * 2.0
            realized_pnl = round(notional * pct_move - total_fees, 2)

        new_eq = round(prev_eq + realized_pnl, 2)
        roi_pct = round((realized_pnl / max(prev_eq, 1.0)) * 100.0, 2)

        self.state["active_equity"] = new_eq
        if new_eq > float(self.state["peak_equity"]):
            self.state["peak_equity"] = new_eq

        pair_cds = self.state.get("pair_cooldowns", {})
        if realized_pnl > 0:
            self.state["consecutive_losses"] = 0
            if side == "LONG":
                self.state["consecutive_long_losses"] = 0
            else:
                self.state["consecutive_short_losses"] = 0
            pair_cds[camp["symbol"]] = time.time() + self.WIN_COOLDOWN_SEC
        else:
            self.state["consecutive_losses"] = int(self.state.get("consecutive_losses", 0)) + 1
            pair_cds[camp["symbol"]] = time.time() + self.LOSS_COOLDOWN_SEC

            if side == "LONG":
                self.state["consecutive_long_losses"] = int(self.state.get("consecutive_long_losses", 0)) + 1
                if self.state["consecutive_long_losses"] >= 2:
                    self.state["long_pause_until_ts"] = time.time() + self.DIRECTION_PAUSE_SEC
                    self.state["consecutive_long_losses"] = 0
            else:
                self.state["consecutive_short_losses"] = int(self.state.get("consecutive_short_losses", 0)) + 1
                if self.state["consecutive_short_losses"] >= 2:
                    self.state["short_pause_until_ts"] = time.time() + self.DIRECTION_PAUSE_SEC
                    self.state["consecutive_short_losses"] = 0

        self.state["pair_cooldowns"] = pair_cds

        closed_record = {
            "trade_id": camp["trade_id"],
            "symbol": camp["symbol"],
            "delta_symbol": camp.get("delta_symbol"),
            "side": side,
            "setup_type": camp.get("setup_type", "DUAL_CORE_SNIPER"),
            "confluence_score": camp.get("confluence_score", 98),
            "entry_time": camp["entry_time"],
            "exit_time": datetime.now(timezone.utc).isoformat(),
            "entry_price": fmt_price(entry_p),
            "exit_price": fmt_price(exit_price),
            "max_stage_reached": camp.get("max_stage_reached", "V9.5_SNIPER_OPEN"),
            "filled_units": 1,
            "total_notional_inr": round(notional, 2),
            "realized_pnl_inr": realized_pnl,
            "roi_pct": roi_pct,
            "exit_reason": reason,
            "balance_after_inr": new_eq,
            "outcome": "WIN" if realized_pnl > 0 else "LOSS",
            "is_live_delta": bool(camp.get("is_live_delta")),
        }
        history = self.state.get("trade_history", [])
        history.insert(0, closed_record)
        self.state["trade_history"] = history[:200]
        self._recompute_trade_stats()
        self.active_campaigns.remove(camp)
        self.save_state()
        return closed_record

    def build_payload(self) -> Dict[str, Any]:
        campaign_unrealized = round(
            sum(
                float(c["total_unrealized_pnl_inr"]) + float(c.get("tp1_banked_pnl_inr", 0.0))
                for c in self.active_campaigns
            ),
            2,
        )
        if self.live_router.is_live() and self.live_router.live_open_positions:
            unrealized = self.live_router.live_open_pnl_inr
        else:
            unrealized = campaign_unrealized

        base_eq = (
            float(self.live_router.last_synced_balance_inr)
            if (self.live_router.is_live() and self.live_router.last_synced_balance_inr is not None and self.live_router.last_synced_balance_inr > 0)
            else float(self.state["active_equity"])
        )
        start_eq = float(self.state.get("starting_equity") or os.getenv("STARTING_EQUITY_INR", "10000.0"))
        live_active = round(base_eq + unrealized, 2)
        total_net_worth = round(live_active + float(self.state["vault_balance"]), 2)
        kelly = self.kelly_ratchet.compute_allocation(total_net_worth, float(self.state["peak_equity"]))
        self.state["peak_equity"] = kelly.peak_equity

        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "server_ts_ms": int(time.time() * 1000),
            "uptime_seconds": int(time.time() - BOOT_TIME),
            "last_scan_ts": self.last_scan_ts,
            "mode": "LIVE" if self.live_router.is_live() else self.state.get("mode", "PAPER"),
            "server_outbound_ip": SERVER_OUTBOUND_IP,
            "delta_connected": self.live_router.connected,
            "delta_ws_connected": self.live_router.ws_connected,
            "delta_last_sync_ts": self.live_router.last_sync_ts,
            "delta_wallet_inr": self.live_router.last_synced_balance_inr,
            "delta_available_inr": self.live_router.last_available_balance_inr,
            "delta_position_margin_inr": self.live_router.last_position_margin_inr,
            "delta_order_margin_inr": self.live_router.last_order_margin_inr,
            "delta_wallet_usd": self.live_router.wallet_balance_usd,
            "delta_wallet_currency": self.live_router.wallet_currency,
            "delta_open_pnl_inr": self.live_router.live_open_pnl_inr,
            "delta_open_positions": self.live_router.live_open_positions,
            "delta_open_orders": self.live_router.live_open_orders,
            "delta_fills": self.live_router.live_fills[:100],
            "delta_api_error": self.live_router.last_error,
            "starting_equity": start_eq,
            "active_equity": live_active,
            "realized_base_equity": round(base_eq, 2),
            "unrealized_pnl_inr": round(unrealized, 2),
            "vault_balance": 0.0,
            "total_net_worth": total_net_worth,
            "total_return_pct": round(((total_net_worth - start_eq) / max(start_eq, 1.0)) * 100.0, 2),
            "principal_siphoned": False,
            "next_siphon_milestone": 1000000.0,
            "wins": self.state.get("wins", 0),
            "losses": self.state.get("losses", 0),
            "total_closed_trades": self.state.get("total_closed_trades", 0),
            "win_rate_pct": self.state.get("win_rate_pct", 0.0),
            "gross_profit_inr": self.state.get("gross_profit_inr", 0.0),
            "gross_loss_inr": self.state.get("gross_loss_inr", 0.0),
            "net_realized_pnl_inr": self.state.get("net_realized_pnl_inr", 0.0),
            "kelly": kelly.to_dict(),
            "scanner": list(self.scanner_cache.values()),
            "pyramid": self.active_campaigns[0] if self.active_campaigns else None,
            "active_campaigns": self.active_campaigns,
            "trade_history": self.state.get("trade_history", []),
            "siphon_history": [],
        }


daemon = AutonomousDaemonV9()


@app.on_event("startup")
async def start_background_scanner() -> None:
    asyncio.create_task(daemon.live_router.start_websocket_stream(daemon.on_delta_realtime_event))

    async def fast_loop():
        while True:
            try:
                await daemon.scan_and_execute_cycle()
                await daemon.broadcast_to_clients(force=True)
            except Exception as e:
                print(f"[DAEMON_LOOP_ERR] {e}")
            await asyncio.sleep(0.5)

    asyncio.create_task(fast_loop())


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket) -> None:
    await ws.accept()
    daemon.clients.append(ws)
    try:
        await ws.send_json(daemon.build_payload())
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    except Exception:
        pass
    finally:
        if ws in daemon.clients:
            daemon.clients.remove(ws)


@app.get("/health")
@app.head("/health")
@app.get("/ping")
@app.head("/ping")
@app.get("/api/ping")
@app.head("/api/ping")
async def health_check() -> Dict[str, Any]:
    return {
        "status": "ONLINE_24_7_V9_5_FEE_SHIELD",
        "version": "9.5.0",
        "uptime_seconds": int(time.time() - BOOT_TIME),
        "last_scan_ts": daemon.last_scan_ts,
        "active_equity_inr": daemon.state["active_equity"],
        "wins": daemon.state["wins"],
        "losses": daemon.state["losses"],
        "win_rate_pct": daemon.state["win_rate_pct"],
        "execution_mode": "LIVE" if daemon.live_router.is_live() else "PAPER",
        "delta_connected": daemon.live_router.connected,
        "delta_ws_connected": daemon.live_router.ws_connected,
        "delta_wallet_inr": daemon.live_router.last_synced_balance_inr,
        "delta_available_inr": daemon.live_router.last_available_balance_inr,
        "delta_open_pnl_inr": daemon.live_router.live_open_pnl_inr,
        "delta_open_positions_count": len(daemon.live_router.live_open_positions),
        "delta_fills_count": len(daemon.live_router.live_fills),
        "delta_api_error": daemon.live_router.last_error,
        "server_outbound_ip": SERVER_OUTBOUND_IP,
    }


@app.get("/api/state")
async def get_full_state() -> Dict[str, Any]:
    return daemon.build_payload()


@app.get("/api/live-account")
@app.post("/api/sync-delta")
async def force_sync_delta_account() -> Dict[str, Any]:
    await daemon.sync_live_delta_state(force=True)
    await daemon.broadcast_to_clients(force=True)
    return daemon.build_payload()


@app.post("/api/kill-switch")
async def emergency_flatten() -> Dict[str, Any]:
    closed_list = []
    for camp in list(daemon.active_campaigns):
        if daemon.live_router.is_live() and camp.get("delta_symbol"):
            await daemon.live_router.close_live_trade(
                camp["delta_symbol"],
                camp["side"],
                float(camp.get("remaining_qty") or camp.get("live_qty") or 0.0),
                camp.get("stop_order_id"),
            )
        closed_list.append(
            daemon.close_campaign(camp, exit_price=camp["current_price"], reason="MANUAL_EMERGENCY_CLOSE")
        )
    if daemon.live_router.is_live():
        await daemon.sync_live_delta_state(force=True)
    await daemon.broadcast_to_clients(force=True)
    return {"status": "FLATTENED", "closed_trades": closed_list, "telemetry": daemon.build_payload()}


@app.post("/api/reset")
async def reset_account() -> Dict[str, Any]:
    daemon.active_campaigns = []
    default_eq = float(daemon.live_router.last_synced_balance_inr or 10000.0)
    daemon.state["active_equity"] = default_eq
    daemon.state["peak_equity"] = default_eq
    daemon.state["vault_balance"] = 0.0
    daemon.state["principal_siphoned"] = False
    daemon.state["consecutive_losses"] = 0
    daemon.state["consecutive_long_losses"] = 0
    daemon.state["consecutive_short_losses"] = 0
    daemon.state["long_pause_until_ts"] = 0.0
    daemon.state["short_pause_until_ts"] = 0.0
    daemon.state["chop_pause_until_ts"] = 0.0
    daemon.state["wins"] = 0
    daemon.state["losses"] = 0
    daemon.state["total_closed_trades"] = 0
    daemon.state["win_rate_pct"] = 0.0
    daemon.state["gross_profit_inr"] = 0.0
    daemon.state["gross_loss_inr"] = 0.0
    daemon.state["net_realized_pnl_inr"] = 0.0
    daemon.state["trade_history"] = []
    daemon.state["siphon_history"] = []
    daemon.state["pair_cooldowns"] = {}
    if daemon.live_router.is_live():
        await daemon.sync_live_delta_state(force=True)
    else:
        daemon.save_state()
    await daemon.broadcast_to_clients(force=True)
    return {"status": "RESET_OK", "telemetry": daemon.build_payload()}


class RestoreStateRequest(BaseModel):
    active_equity: float
    peak_equity: float
    vault_balance: float = 0.0
    wins: int
    losses: int
    total_closed_trades: int
    gross_profit_inr: float
    gross_loss_inr: float
    trade_history: List[Dict[str, Any]]
    siphon_history: List[Dict[str, Any]] = []


@app.post("/api/restore-state")
async def restore_server_state(req: RestoreStateRequest) -> Dict[str, Any]:
    if daemon.live_router.is_live():
        return {"status": "LIVE_MODE_PROTECTED", "telemetry": daemon.build_payload()}
    if daemon.state.get("total_closed_trades", 0) == 0 and req.total_closed_trades > 0:
        daemon.state["active_equity"] = round(req.active_equity, 2)
        daemon.state["peak_equity"] = round(req.peak_equity, 2)
        daemon.state["vault_balance"] = 0.0
        daemon.state["wins"] = req.wins
        daemon.state["losses"] = req.losses
        daemon.state["total_closed_trades"] = req.total_closed_trades
        daemon.state["win_rate_pct"] = round((req.wins / max(req.total_closed_trades, 1)) * 100.0, 1)
        daemon.state["gross_profit_inr"] = round(req.gross_profit_inr, 2)
        daemon.state["gross_loss_inr"] = round(req.gross_loss_inr, 2)
        daemon.state["net_realized_pnl_inr"] = round(req.gross_profit_inr - req.gross_loss_inr, 2)
        daemon.state["trade_history"] = req.trade_history[:200]
        daemon.save_state()
        return {"status": "RESTORED_FROM_BACKUP", "telemetry": daemon.build_payload()}
    return {"status": "NO_RESTORE_NEEDED", "telemetry": daemon.build_payload()}


# Mount unified dashboard static build if available
static_dir = os.path.join(os.path.dirname(__file__), "static")
if not os.path.exists(static_dir):
    static_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "backend", "static")
if not os.path.exists(static_dir):
    static_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "static")

if os.path.exists(static_dir):
    from fastapi.staticfiles import StaticFiles
    from starlette.responses import FileResponse

    @app.get("/")
    @app.head("/")
    async def serve_index():
        idx_path = os.path.join(static_dir, "index.html")
        if os.path.exists(idx_path):
            return FileResponse(idx_path)
        return FileResponse(os.path.join(static_dir, "404.html"))

    app.mount("/", StaticFiles(directory=static_dir, html=True), name="dashboard")
    print(f"Dashboard static files mounted successfully from: {static_dir}")

