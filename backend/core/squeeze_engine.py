"""
APEX-OMEGA OS V11.0 LIVE-SHADOW & 1-CLICK DELTA LIVE MASTER — Triple-Core Closed-Bar Sniper Engine
Combines:
1. Alpha-6 Winning Universe (SOL, SUI, DOGE, ADA, AVAX, LINK) with Top-5 Priority (SOL, SUI, DOGE, ADA, AVAX) and BTC as macro filter only.
2. Completed 5m Candle Evaluation (`full_arr[:-1]`) — eliminates intrabar 1-tick wick fakeouts.
3. Triple-Core High-Precision Confluence (96+/100):
   - Core A: Trend Dip Sniper (RSI-2 <= 22 / >= 78 or VWAP Z <= -1.0 / >= +1.0 in aligned EMA21/50/100/200 trend)
   - Core B: Dual-Core EMA-21 Pullback Bounce
   - Core C: High-Velocity EMA-9 Touch Continuation (catches mid-trend impulse waves with RVOL >= 1.10 & KER >= 0.22)
4. Anti-Spike-Chase Safety (`cur_range <= 2.2 * atr14`) — skips blown-out parabolic candles.
"""
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
    "SOL/USDT:USDT",
    "SUI/USDT:USDT",
    "DOGE/USDT:USDT",
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
    rsi_14: float = 50.0
    compression_ratio: float = 1.0
    squeeze_on: bool = False
    squeeze_bars_last_10: int = 0
    ema_trend_slope: float = 0.0
    rvol: float = 1.0
    roc5: float = 0.0
    ker: float = 0.45
    confluence_score: int = 50
    setup_type: str = "SCANNING"
    regime: str = "NEUTRAL"
    signal_valid: bool = False
    feed_source: str = "DELTA_WS_0MS"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class SqueezeEngine:
    @staticmethod
    def _ema(values: np.ndarray, span: int) -> np.ndarray:
        alpha = 2.0 / (span + 1.0)
        ema = np.zeros_like(values, dtype=float)
        ema[0] = values[0]
        for i in range(1, len(values)):
            ema[i] = alpha * values[i] + (1.0 - alpha) * ema[i - 1]
        return ema

    @staticmethod
    def _rsi(closes: np.ndarray, period: int) -> float:
        if len(closes) <= period:
            return 50.0
        diffs = np.diff(closes[-(period + 1):])
        gains = float(np.sum(np.where(diffs > 0, diffs, 0.0))) / period
        losses = float(np.sum(np.where(diffs < 0, -diffs, 0.0))) / period
        if losses <= 1e-12:
            return 98.0
        rs = gains / losses
        return round(100.0 - (100.0 / (1.0 + rs)), 1)

    @staticmethod
    def _atr(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> np.ndarray:
        tr = np.zeros_like(close, dtype=float)
        tr[0] = max(high[0] - low[0], close[0] * 0.002)
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
        # Evaluate on completed 5m candles so intrabar 1-tick wicks never trigger false entries
        arr = full_arr[:-1] if len(full_arr) > 35 else full_arr
        open_p = arr[:, 1]
        high = arr[:, 2]
        low = arr[:, 3]
        close = arr[:, 4]
        vol = arr[:, 5]
        n = len(close)

        atr20 = self._atr(high, low, close, 20)
        atr14 = self._atr(high, low, close, 14)
        ema9 = self._ema(close, min(9, n))
        ema21 = self._ema(close, min(21, n))
        ema50 = self._ema(close, min(50, n))
        ema100 = self._ema(close, min(100, n))
        ema200 = self._ema(close, min(200, n))
        true_atr_pct = float(atr14[-1]) / max(float(close[-1]), 1e-12)

        win = min(20, n)
        typ = (high[-win:] + low[-win:] + close[-win:]) / 3.0
        v_slice = np.maximum(vol[-win:], 1e-9)
        vwap = float(np.sum(typ * v_slice) / np.sum(v_slice))
        std20 = float(np.std(close[-win:]))
        z_score = round((float(close[-1]) - vwap) / max(std20, float(close[-1]) * 0.001), 2)

        squeeze_count = 0
        current_comp = 1.0
        squeeze_now = False
        for offset in range(min(9, n - 20), -1, -1):
            idx = n - 1 - offset
            sub = close[max(0, idx - 19) : idx + 1]
            sma = float(np.mean(sub))
            std = float(np.std(sub))
            bb_u, bb_l = sma + 2.0 * std, sma - 2.0 * std
            kc_mid = float(self._ema(close[: idx + 1], 20)[-1])
            kc_u, kc_l = kc_mid + 1.5 * float(atr20[idx]), kc_mid - 1.5 * float(atr20[idx])
            in_sq = (bb_u < kc_u) and (bb_l > kc_l)
            if in_sq:
                squeeze_count += 1
            if offset == 0:
                current_comp = round((bb_u - bb_l) / max(kc_u - kc_l, 1e-9), 2)
                squeeze_now = in_sq

        rsi2 = self._rsi(close, 2)
        rsi14 = self._rsi(close, 14)
        ema_now = float(ema21[-1])
        ema_prev = float(ema21[max(0, n - 5)])
        ema_slope = (ema_now - ema_prev) / max(ema_prev, 1e-9)

        vol_mean = float(np.mean(vol[max(0, n - 21) : n - 1])) if n > 5 else float(np.mean(vol))
        rvol = round(max(float(vol[-1]), float(vol[-2])) / max(vol_mean, 1e-9), 2)

        close5 = float(close[max(0, n - 6)])
        roc5 = round((float(close[-1]) - close5) / max(close5, 1e-9), 5)

        ker_p = min(10, n - 1)
        net_move = abs(float(close[-1] - close[-1 - ker_p]))
        path_sum = float(np.sum(np.abs(np.diff(close[-(ker_p + 1):]))))
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
        no_top_buy = z_score <= 0.85 and rsi14 <= 68.0
        no_bot_short = z_score >= -0.85 and rsi14 >= 32.0
        vol_gate_ok = is_tier1 and true_atr_pct >= 0.0014 and ker >= 0.16 and adx14 >= 14.0

        core_a_bull_trend = (
            vol_gate_ok and no_top_buy
            and float(close[-1]) >= float(ema200[-1]) * 0.996
            and float(ema21[-1]) > float(ema50[-1])
            and float(ema50[-1]) >= float(ema100[-1]) * 0.9995
            and ema_slope >= 0.00030
            and roc5 > -0.0060
        )
        core_a_bear_trend = (
            vol_gate_ok and no_bot_short
            and float(close[-1]) <= float(ema200[-1]) * 1.004
            and float(ema21[-1]) < float(ema50[-1])
            and float(ema50[-1]) <= float(ema100[-1]) * 1.0005
            and ema_slope <= -0.00030
            and roc5 < 0.0060
        )

        score_a_bull = 38 + tier_bonus + (22 if core_a_bull_trend else 0) + (19 if rsi2 <= 22 else 0) + (11 if z_score <= -1.00 else 0) + (10 if (green_bounce or lower_wick >= 0.28) else 0) + (6 if rvol >= 1.08 else 0)
        score_a_bear = 38 + tier_bonus + (22 if core_a_bear_trend else 0) + (19 if rsi2 >= 78 else 0) + (11 if z_score >= 1.00 else 0) + (10 if (red_bounce or upper_wick >= 0.28) else 0) + (6 if rvol >= 1.08 else 0)

        core_b_bull_stack = (
            vol_gate_ok and no_top_buy
            and float(close[-1]) >= float(ema200[-1]) * 0.998
            and float(ema9[-1]) >= float(ema21[-1]) * 0.9995
            and float(ema21[-1]) > float(ema50[-1])
            and ema_slope >= 0.00035
        )
        core_b_bear_stack = (
            vol_gate_ok and no_bot_short
            and float(close[-1]) <= float(ema200[-1]) * 1.002
            and float(ema9[-1]) <= float(ema21[-1]) * 1.0005
            and float(ema21[-1]) < float(ema50[-1])
            and ema_slope <= -0.00035
        )

        recent_low_3 = min(float(low[-1]), float(low[-2]), float(low[-3]))
        recent_high_3 = max(float(high[-1]), float(high[-2]), float(high[-3]))
        bull_pullback_touch = recent_low_3 <= float(ema21[-1]) * 1.0035 and green_bounce and float(close[-1]) >= float(ema9[-1]) * 0.9995
        bear_pullback_touch = recent_high_3 >= float(ema21[-1]) * 0.9965 and red_bounce and float(close[-1]) <= float(ema9[-1]) * 1.0005

        bull_not_overext = -1.35 <= z_score <= 0.85 and 42 <= rsi14 <= 68
        bear_not_overext = -0.85 <= z_score <= 1.35 and 32 <= rsi14 <= 58

        score_b_bull = 42 + tier_bonus + (24 if core_b_bull_stack else 0) + (16 if bull_pullback_touch else 0) + (11 if bull_not_overext else 0) + (5 if rvol >= 1.02 else 0)
        score_b_bear = 42 + tier_bonus + (24 if core_b_bear_stack else 0) + (16 if bear_pullback_touch else 0) + (11 if bear_not_overext else 0) + (5 if rvol >= 1.02 else 0)

        # Core C: V11 High-Velocity EMA-9 Touch Continuation
        recent_low_2 = min(float(low[-1]), float(low[-2]))
        recent_high_2 = max(float(high[-1]), float(high[-2]))
        core_c_bull_ok = (
            core_a_bull_trend
            and float(ema9[-1]) > float(ema21[-1])
            and recent_low_2 <= float(ema9[-1]) * 1.0010
            and green_bounce
            and float(close[-1]) >= float(ema9[-1])
            and -1.10 <= z_score <= 0.65
            and rvol >= 1.10
            and ker >= 0.22
        )
        core_c_bear_ok = (
            core_a_bear_trend
            and float(ema9[-1]) < float(ema21[-1])
            and recent_high_2 >= float(ema9[-1]) * 0.9990
            and red_bounce
            and float(close[-1]) <= float(ema9[-1])
            and -0.65 <= z_score <= 1.10
            and rvol >= 1.10
            and ker >= 0.22
        )
        score_c_bull = 97 if core_c_bull_ok else 50
        score_c_bear = 97 if core_c_bear_ok else 50

        bull_score = max(score_a_bull, score_b_bull, score_c_bull)
        bear_score = max(score_a_bear, score_b_bear, score_c_bear)

        long_a_ok = core_a_bull_trend and green_bounce and (rsi2 <= 22 or z_score <= -1.00) and score_a_bull >= 96
        long_b_ok = core_b_bull_stack and bull_pullback_touch and bull_not_overext and score_b_bull >= 96
        short_a_ok = core_a_bear_trend and red_bounce and (rsi2 >= 78 or z_score >= 1.00) and score_a_bear >= 96
        short_b_ok = core_b_bear_stack and bear_pullback_touch and bear_not_overext and score_b_bear >= 96

        if long_a_ok or long_b_ok or core_c_bull_ok:
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
        elif short_a_ok or short_b_ok or core_c_bear_ok:
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
            score = min(92, max(bull_score, bear_score))
            regime = "COILING" if (squeeze_now or score >= 75) else "NEUTRAL"
            setup_type = "SCANNING"
            signal_valid = False

        eff_atr = max(float(atr14[-1]), float(close[-1]) * 0.0065)

        return QuantV9Signal(
            symbol=symbol,
            price=live_tick_price,
            atr_14=eff_atr,
            vwap=float(vwap),
            z_score=z_score,
            rsi_2=rsi2,
            rsi_14=rsi14,
            compression_ratio=current_comp,
            squeeze_on=bool(squeeze_now),
            squeeze_bars_last_10=int(squeeze_count),
            ema_trend_slope=round(ema_slope, 6),
            rvol=rvol,
            roc5=roc5,
            ker=ker,
            confluence_score=int(score),
            setup_type=setup_type,
            regime=regime,
            signal_valid=bool(signal_valid),
            feed_source=feed_source,
        )
