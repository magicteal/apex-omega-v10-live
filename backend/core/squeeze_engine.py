"""
APEX-OMEGA OS V12.3 UNIFIED MASTER — Triple-Core Hybrid Closed-Bar + Live-Tick Guard Sniper Engine
Combines the best of V9.0 Restored Live and V12.2 Hyper-Compound:
1. Alpha-6 Winning Universe (SOL, SUI, DOGE, ADA, AVAX, LINK) with Top-5 Priority (SOL, SUI, DOGE, ADA, AVAX) and BTC as macro filter only.
2. Hybrid Closed-Bar + Live-Tick Guard Evaluation:
   - Computes EMA/RSI/VWAP/ADX/ATR structure on completed 5m candles (`full_arr[:-1]`) to eliminate intrabar 1-tick wick fakeouts.
   - Verifies live forming bar (`live_tick_price`) holds the breakout direction without chasing over-extended moves (>0.38% from signal close).
   - Also supports `SIGNAL_EVAL_MODE=REALTIME_TICK` env override for pure V9 0ms intrabar evaluation.
3. Triple-Core High-Precision Confluence (96+/100):
   - Core A: Trend Dip Sniper (RSI-2 <= 22 / >= 78 or VWAP Z <= -1.0 / >= +1.0 in aligned EMA21/50/100/200 trend)
   - Core B: Dual-Core EMA-21 Pullback Bounce
   - Core C: High-Velocity EMA-9 Touch Continuation (catches mid-trend impulse waves with RVOL >= 1.10 & KER >= 0.22)
4. Institutional Safety Filters:
   - Anti-Spike-Chase Safety (`cur_range <= 2.20 * atr14`)
   - Trend Strength Filter (`adx14 >= 14.0`)
   - Anti-Top-Buy / Anti-Bottom-Short Guards (`z_score <= 0.85, rsi14 <= 68` for Longs; `z_score >= -0.85, rsi14 >= 32` for Shorts)
"""
import os
from dataclasses import dataclass, asdict
from typing import List, Dict, Any
import numpy as np

TIER1_ALPHA_SYMBOLS = {
    "SOL/USDT:USDT",
    "SUI/USDT:USDT",
    "DOGE/USDT:USDT",
    "ADA/USDT:USDT",
    "AVAX/USDT:USDT",
    "LINK/USDT:USDT",
}

TOP5_PRIORITY_SYMBOLS = {
    "LINK/USDT:USDT",
    "DOGE/USDT:USDT",
    "SOL/USDT:USDT",
    "ADA/USDT:USDT",
    "AVAX/USDT:USDT",
}


@dataclass
class QuantV9Signal:
    symbol: str
    price: float
    atr_14: float
    vwap: float
    z_score: float
    rsi_2: float
    rsi_14: float
    compression_ratio: float
    squeeze_on: bool
    squeeze_bars_last_10: int
    ema_trend_slope: float
    rvol: float
    roc5: float
    ker: float
    confluence_score: int
    setup_type: str
    regime: str
    signal_valid: bool
    feed_source: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SqueezeEngine:
    def __init__(self, bb_len: int = 20, bb_mult: float = 2.0, kc_len: int = 20, kc_mult: float = 1.5):
        self.bb_len = bb_len
        self.bb_mult = bb_mult
        self.kc_len = kc_len
        self.kc_mult = kc_mult

    @staticmethod
    def _ema(arr: np.ndarray, span: int) -> np.ndarray:
        alpha = 2.0 / (span + 1.0)
        out = np.empty_like(arr, dtype=float)
        out[0] = arr[0]
        for i in range(1, len(arr)):
            out[i] = alpha * arr[i] + (1.0 - alpha) * out[i - 1]
        return out

    @staticmethod
    def _rsi(closes: np.ndarray, period: int) -> float:
        if len(closes) <= period:
            return 50.0
        diffs = np.diff(closes[-(period + 1):])
        gains = np.sum(diffs[diffs > 0]) / period
        losses = -np.sum(diffs[diffs < 0]) / period
        if losses <= 1e-12:
            return 98.0
        rs = gains / losses
        return float(np.clip(100.0 - (100.0 / (1.0 + rs)), 1.0, 99.0))

    @staticmethod
    def _atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
        tr = np.zeros_like(close, dtype=float)
        tr[0] = high[0] - low[0]
        for i in range(1, len(close)):
            tr[i] = max(
                high[i] - low[i],
                abs(high[i] - close[i - 1]),
                abs(low[i] - close[i - 1]),
            )
        return SqueezeEngine._ema(tr, period)

    @staticmethod
    def _adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, atr14: np.ndarray) -> float:
        n = len(close)
        if n < 15:
            return 20.0
        plus_dm = np.zeros(n, dtype=float)
        minus_dm = np.zeros(n, dtype=float)
        for i in range(1, n):
            h_diff = high[i] - high[i - 1]
            l_diff = low[i - 1] - low[i]
            if h_diff > l_diff and h_diff > 0:
                plus_dm[i] = h_diff
            if l_diff > h_diff and l_diff > 0:
                minus_dm[i] = l_diff
        plus_di = 100.0 * SqueezeEngine._ema(plus_dm, 14) / np.maximum(atr14, 1e-12)
        minus_di = 100.0 * SqueezeEngine._ema(minus_dm, 14) / np.maximum(atr14, 1e-12)
        dx = 100.0 * np.abs(plus_di - minus_di) / np.maximum(plus_di + minus_di, 1e-12)
        return float(SqueezeEngine._ema(dx, 14)[-1])

    def evaluate_ohlcv(self, symbol: str, ohlcv: List[List[float]], feed_source: str = "DELTA_INDIA_5M") -> QuantV9Signal:
        full_arr = np.array(ohlcv, dtype=float)
        live_tick_price = float(full_arr[-1, 4])
        eval_mode = os.getenv("SIGNAL_EVAL_MODE", "REALTIME_TICK").strip().upper()

        if eval_mode == "REALTIME_TICK" or len(full_arr) <= 35:
            arr = full_arr
        else:
            # Evaluate indicators on completed 5m candles so intrabar 1-tick wicks never trigger false entries
            arr = full_arr[:-1]

        open_p = arr[:, 1]
        high = arr[:, 2]
        low = arr[:, 3]
        close = arr[:, 4]
        vol = arr[:, 5]

        atr20 = self._atr(high, low, close, self.kc_len)
        atr14 = self._atr(high, low, close, 14)
        ema9 = self._ema(close, 9)
        ema20 = self._ema(close, self.kc_len)
        ema21 = self._ema(close, 21)
        ema50 = self._ema(close, 50)
        ema100 = self._ema(close, 100)
        ema200 = self._ema(close, 200)

        win = min(self.bb_len, len(close))
        recent_close = close[-win:]
        recent_high = high[-win:]
        recent_low = low[-win:]
        recent_vol = np.maximum(vol[-win:], 1e-9)

        typical = (recent_high + recent_low + recent_close) / 3.0
        vwap = float(np.sum(typical * recent_vol) / np.sum(recent_vol))

        std20 = float(np.std(recent_close))
        safe_std = max(std20, float(close[-1]) * 0.001)
        z_score = round((float(close[-1]) - vwap) / safe_std, 2)

        squeeze_count = 0
        squeeze_now = False
        comp_ratio = 1.0
        lookback_bars = min(10, len(close) - self.bb_len)
        for offset in range(max(1, lookback_bars)):
            idx = len(close) - 1 - offset
            sub = close[idx - self.bb_len + 1 : idx + 1]
            m = float(np.mean(sub))
            s = float(np.std(sub))
            bb_u = m + self.bb_mult * s
            bb_l = m - self.bb_mult * s
            kc_u = float(ema20[idx] + self.kc_mult * atr20[idx])
            kc_l = float(ema20[idx] - self.kc_mult * atr20[idx])
            in_sq = (bb_u < kc_u) and (bb_l > kc_l)
            if in_sq:
                squeeze_count += 1
            if offset == 0:
                squeeze_now = in_sq
                kc_w = max(kc_u - kc_l, 1e-9)
                comp_ratio = round((bb_u - bb_l) / kc_w, 2)

        rsi2 = round(self._rsi(close, 2), 1)
        rsi14 = round(self._rsi(close, 14), 1)

        prev_idx = max(0, len(ema21) - 5)
        ema_slope = round((float(ema21[-1]) - float(ema21[prev_idx])) / max(float(ema21[-1]), 1e-9), 5)
        prev50_idx = max(0, len(ema50) - 7)
        ema50_slope = round((float(ema50[-1]) - float(ema50[prev50_idx])) / max(float(ema50[-1]), 1e-9), 5)

        vol_baseline = float(np.mean(vol[-21:-1])) if len(vol) > 21 else float(np.mean(vol))
        cur_vol = max(float(vol[-1]), float(vol[-2]) if len(vol) > 1 else 0.0)
        rvol = round(cur_vol / max(vol_baseline, 1e-9), 2)

        roc5_idx = max(0, len(close) - 6)
        roc5 = round((float(close[-1]) - float(close[roc5_idx])) / max(float(close[roc5_idx]), 1e-9), 5)

        ker_win = min(10, len(close) - 1)
        net_move = abs(float(close[-1]) - float(close[-1 - ker_win]))
        path_sum = float(np.sum(np.abs(np.diff(close[-1 - ker_win :]))))
        ker = round(net_move / max(path_sum, 1e-12), 2)

        cur_range = max(float(high[-1] - low[-1]), 1e-12)
        not_parabolic_spike = cur_range <= float(atr14[-1]) * 2.20
        lower_wick = (min(float(open_p[-1]), float(close[-1])) - float(low[-1])) / cur_range
        upper_wick = (float(high[-1]) - max(float(open_p[-1]), float(close[-1]))) / cur_range
        green_bounce = not_parabolic_spike and float(close[-1]) > float(open_p[-1]) and float(close[-1]) >= float(close[-2])
        red_bounce = not_parabolic_spike and float(close[-1]) < float(open_p[-1]) and float(close[-1]) <= float(close[-2])

        adx14 = self._adx(high, low, close, atr14)
        is_tier1 = symbol in TIER1_ALPHA_SYMBOLS
        tier_bonus = 2 if is_tier1 else 0
        true_atr_pct = float(atr14[-1]) / max(float(close[-1]), 1e-12)
        no_top_buy = z_score <= 0.85 and rsi14 <= 68.0
        no_bot_short = z_score >= -0.85 and rsi14 >= 32.0
        vol_gate_ok = is_tier1 and true_atr_pct >= 0.0014 and ker >= 0.16 and adx14 >= 14.0

        # Live-Tick Guard: prevents entering a closed-bar signal that has already dumped or run away >0.38% intrabar
        sig_close = float(close[-1])
        live_long_guard = (sig_close * 0.9982) <= live_tick_price <= (sig_close * 1.0038)
        live_short_guard = (sig_close * 0.9962) <= live_tick_price <= (sig_close * 1.0018)

        core_a_bull_trend = (
            vol_gate_ok and no_top_buy
            and sig_close >= float(ema200[-1]) * 0.996
            and float(ema21[-1]) > float(ema50[-1])
            and float(ema50[-1]) >= float(ema100[-1]) * 0.9995
            and ema_slope >= 0.00030
            and roc5 > -0.0060
        )
        core_a_bear_trend = (
            vol_gate_ok and no_bot_short
            and sig_close <= float(ema200[-1]) * 1.004
            and float(ema21[-1]) < float(ema50[-1])
            and float(ema50[-1]) <= float(ema100[-1]) * 1.0005
            and ema_slope <= -0.00030
            and roc5 < 0.0060
        )

        score_a_bull = 38 + tier_bonus + (22 if core_a_bull_trend else 0) + (19 if rsi2 <= 22 else 0) + (11 if z_score <= -1.00 else 0) + (10 if (green_bounce or lower_wick >= 0.28) else 0) + (6 if rvol >= 1.08 else 0)
        score_a_bear = 38 + tier_bonus + (22 if core_a_bear_trend else 0) + (19 if rsi2 >= 78 else 0) + (11 if z_score >= 1.00 else 0) + (10 if (red_bounce or upper_wick >= 0.28) else 0) + (6 if rvol >= 1.08 else 0)

        core_b_bull_stack = (
            vol_gate_ok and no_top_buy
            and sig_close >= float(ema200[-1]) * 0.998
            and float(ema9[-1]) >= float(ema21[-1]) * 0.9995
            and float(ema21[-1]) > float(ema50[-1])
            and ema_slope >= 0.00035
        )
        core_b_bear_stack = (
            vol_gate_ok and no_bot_short
            and sig_close <= float(ema200[-1]) * 1.002
            and float(ema9[-1]) <= float(ema21[-1]) * 1.0005
            and float(ema21[-1]) < float(ema50[-1])
            and ema_slope <= -0.00035
        )

        recent_low_3 = min(float(low[-1]), float(low[-2]), float(low[-3]))
        recent_high_3 = max(float(high[-1]), float(high[-2]), float(high[-3]))
        bull_pullback_touch = recent_low_3 <= float(ema21[-1]) * 1.0035 and green_bounce and sig_close >= float(ema9[-1]) * 0.9995
        bear_pullback_touch = recent_high_3 >= float(ema21[-1]) * 0.9965 and red_bounce and sig_close <= float(ema9[-1]) * 1.0005

        bull_not_overext = -1.35 <= z_score <= 0.85 and 42 <= rsi14 <= 68
        bear_not_overext = -0.85 <= z_score <= 1.35 and 32 <= rsi14 <= 58

        score_b_bull = 42 + tier_bonus + (24 if core_b_bull_stack else 0) + (16 if bull_pullback_touch else 0) + (11 if bull_not_overext else 0) + (5 if rvol >= 1.02 else 0)
        score_b_bear = 42 + tier_bonus + (24 if core_b_bear_stack else 0) + (16 if bear_pullback_touch else 0) + (11 if bear_not_overext else 0) + (5 if rvol >= 1.02 else 0)

        # Core C: V12 High-Velocity EMA-9 Touch Continuation
        recent_low_2 = min(float(low[-1]), float(low[-2]))
        recent_high_2 = max(float(high[-1]), float(high[-2]))
        core_c_bull_ok = (
            core_a_bull_trend
            and float(ema9[-1]) > float(ema21[-1])
            and recent_low_2 <= float(ema9[-1]) * 1.0010
            and green_bounce
            and sig_close >= float(ema9[-1])
            and -1.10 <= z_score <= 0.65
            and rvol >= 1.10
            and ker >= 0.22
        )
        core_c_bear_ok = (
            core_a_bear_trend
            and float(ema9[-1]) < float(ema21[-1])
            and recent_high_2 >= float(ema9[-1]) * 0.9990
            and red_bounce
            and sig_close <= float(ema9[-1])
            and -0.65 <= z_score <= 1.10
            and rvol >= 1.10
            and ker >= 0.22
        )
        score_c_bull = 97 if core_c_bull_ok else 50
        score_c_bear = 97 if core_c_bear_ok else 50

        bull_score = max(score_a_bull, score_b_bull, score_c_bull)
        bear_score = max(score_a_bear, score_b_bear, score_c_bear)

        dip_abs_long = core_a_bull_trend and not_parabolic_spike and (rsi2 <= 18.0 or z_score <= -1.15) and sig_close <= float(ema21[-1]) * 1.0020 and (green_bounce or lower_wick >= 0.30)
        dip_abs_short = core_a_bear_trend and not_parabolic_spike and (rsi2 >= 82.0 or z_score >= 1.15) and sig_close >= float(ema21[-1]) * 0.9980 and (red_bounce or upper_wick >= 0.30)
        long_a_ok = (core_a_bull_trend and green_bounce and (rsi2 <= 22 or z_score <= -1.00) and score_a_bull >= 96) or dip_abs_long
        long_b_ok = core_b_bull_stack and bull_pullback_touch and bull_not_overext and score_b_bull >= 96
        short_a_ok = (core_a_bear_trend and red_bounce and (rsi2 >= 78 or z_score >= 1.00) and score_a_bear >= 96) or dip_abs_short
        short_b_ok = core_b_bear_stack and bear_pullback_touch and bear_not_overext and score_b_bear >= 96

        if (long_a_ok or long_b_ok or core_c_bull_ok) and live_long_guard:
            regime = "LONG_IGNITION"
            setup_type = (
                "DUAL_CORE_PULLBACK_LONG"
                if long_b_ok
                else "TREND_DIP_SNIPER_LONG"
                if long_a_ok
                else "EMA9_CONTINUATION_LONG"
            )
            score = min(99, bull_score)
            signal_valid = True
        elif (short_a_ok or short_b_ok or core_c_bear_ok) and live_short_guard:
            regime = "SHORT_IGNITION"
            setup_type = (
                "DUAL_CORE_PULLBACK_SHORT"
                if short_b_ok
                else "TREND_DIP_SNIPER_SHORT"
                if short_a_ok
                else "EMA9_CONTINUATION_SHORT"
            )
            score = min(99, bear_score)
            signal_valid = True
        else:
            regime = "COILING" if squeeze_now else "NEUTRAL"
            setup_type = "SCANNING"
            score = int(np.clip(max(bull_score, bear_score) - 5, 25, 94))
            signal_valid = False

        raw_atr = float(atr14[-1])
        eff_atr = max(raw_atr, sig_close * 0.0055)

        return QuantV9Signal(
            symbol=symbol,
            price=live_tick_price,
            atr_14=eff_atr,
            vwap=float(vwap),
            z_score=z_score,
            rsi_2=rsi2,
            rsi_14=rsi14,
            compression_ratio=comp_ratio,
            squeeze_on=bool(squeeze_now),
            squeeze_bars_last_10=int(squeeze_count),
            ema_trend_slope=ema_slope,
            rvol=rvol,
            roc5=roc5,
            ker=ker,
            confluence_score=int(score),
            setup_type=setup_type,
            regime=regime,
            signal_valid=signal_valid,
            feed_source=feed_source,
        )
