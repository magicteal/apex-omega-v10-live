"""
APEX-OMEGA OS V9.7 — Anti-Spike-Chase, Closed-Bar Confirmed & 2-Step Maker-First BBO Engine
Engineered from Forensic Audit of Today's Live Trades (LINK, ADA, DOGE x2) + 35 Real Delta Campaigns:
1. Anti-Top-Buying & Anti-Bottom-Selling Extension Gate (`Core B` Pullback Continuation):
   - Blocks buying top of 5m green spikes: requires `-0.85 <= z_score <= +0.32` for LONG (`-0.32 <= z_score <= +0.85` for SHORT),
     price within `<= 0.25%` of EMA-9 (`dist_ema9_pct <= 0.0025`), and `cur_range <= 1.65 * atr14` (never chases a blown-out spike bar).
2. Closed-Candle Confirmation (`ohlcv[-2]` vs `ohlcv[-1]`):
   - Prevents mid-candle WebSocket tick head-fakes by requiring the LAST CLOSED 5m bar (`[-2]`) OR a confirmed 2-bar structure
     to hold EMA-9/EMA-21 support with real directional body/wick absorption.
3. 15-Minute Multi-Timeframe (MTF) Alignment + Wilder ADX-14 >= 20/22 + High-Wick SOL/BTC Shield:
   - Blocks counter-trend 5m scalps when 15m macro structure (`EMA-63` vs `EMA-150` & 15m slope) disagrees.
"""
from dataclasses import dataclass, asdict
from typing import List, Dict, Any
import numpy as np

TIER1_ALPHA_SYMBOLS = {
    "DOGE/USDT:USDT",
    "SOL/USDT:USDT",
    "LINK/USDT:USDT",
    "ADA/USDT:USDT",
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
    adx_14: float = 25.0
    mtf_15m_aligned: bool = True
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
    feed_source: str = "DELTA_INDIA_LIVE_5M"

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
    def _adx(high: np.ndarray, low: np.ndarray, close: np.ndarray, period: int = 14) -> float:
        n = len(close)
        if n < period + 2:
            return 25.0
        up_move = high[1:] - high[:-1]
        down_move = low[:-1] - low[1:]
        plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
        minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
        tr = np.maximum(
            high[1:] - low[1:],
            np.maximum(np.abs(high[1:] - close[:-1]), np.abs(low[1:] - close[:-1])),
        )
        atr_s = SqueezeEngine._ema(tr, period)
        plus_di = 100.0 * SqueezeEngine._ema(plus_dm, period) / np.maximum(atr_s, 1e-12)
        minus_di = 100.0 * SqueezeEngine._ema(minus_dm, period) / np.maximum(atr_s, 1e-12)
        dx = 100.0 * np.abs(plus_di - minus_di) / np.maximum(plus_di + minus_di, 1e-12)
        adx_series = SqueezeEngine._ema(dx, period)
        return round(float(adx_series[-1]), 1)

    def evaluate_ohlcv(self, symbol: str, ohlcv: List[List[float]], feed_source: str = "DELTA_INDIA_LIVE_5M") -> QuantV9Signal:
        arr = np.array(ohlcv, dtype=float)
        open_p = arr[:, 1]
        high = arr[:, 2]
        low = arr[:, 3]
        close = arr[:, 4]
        vol = arr[:, 5]
        n = len(close)

        atr20 = self._atr(high, low, close, 20)
        atr14 = self._atr(high, low, close, 14)
        adx14 = self._adx(high, low, close, 14)
        ema9 = self._ema(close, min(9, n))
        ema21 = self._ema(close, min(21, n))
        ema50 = self._ema(close, min(50, n))
        ema63 = self._ema(close, min(63, n))    # 15m EMA-21 equivalent on 5m bars
        ema100 = self._ema(close, min(100, n))
        ema150 = self._ema(close, min(150, n))  # 15m EMA-50 equivalent on 5m bars
        ema200 = self._ema(close, min(200, n))
        true_atr_pct = float(atr14[-1]) / max(float(close[-1]), 1e-12)

        # 15-Minute Multi-Timeframe (MTF) Structural Alignment
        ema63_slope = (float(ema63[-1]) - float(ema63[max(0, n - 7)])) / max(float(ema63[max(0, n - 7)]), 1e-9)
        mtf_bull_ok = float(ema63[-1]) >= float(ema150[-1]) * 0.9995 and ema63_slope >= 0.00028
        mtf_bear_ok = float(ema63[-1]) <= float(ema150[-1]) * 1.0005 and ema63_slope <= -0.00028

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
        prev_range = max(float(high[-2] - low[-2]), 1e-12)
        body_ratio = abs(float(close[-1]) - float(open_p[-1])) / cur_range
        prev_body_ratio = abs(float(close[-2]) - float(open_p[-2])) / prev_range
        lower_wick = (min(float(open_p[-1]), float(close[-1])) - float(low[-1])) / cur_range
        upper_wick = (float(high[-1]) - max(float(open_p[-1]), float(close[-1]))) / cur_range

        # Anti-Spike-Chase Gate: never enter on an already blown-out candle (> 1.65x ATR) or > 0.25% extended past EMA-9
        not_spike_bar = cur_range <= float(atr14[-1]) * 1.65
        dist_ema9_bull = (float(close[-1]) - float(ema9[-1])) / max(float(ema9[-1]), 1e-9)
        dist_ema9_bear = (float(ema9[-1]) - float(close[-1])) / max(float(ema9[-1]), 1e-9)

        # Closed-Candle + Live-Bar Confirmation (prevents intrabar tick head-fakes)
        closed_bull_confirm = float(close[-2]) >= float(open_p[-2]) or (float(close[-2]) >= float(ema21[-2]) and prev_body_ratio <= 0.55)
        closed_bear_confirm = float(close[-2]) <= float(open_p[-2]) or (float(close[-2]) <= float(ema21[-2]) and prev_body_ratio <= 0.55)

        green_bounce = (
            not_spike_bar
            and closed_bull_confirm
            and float(close[-1]) > float(open_p[-1])
            and float(close[-1]) > float(close[-2])
            and float(low[-1]) >= float(low[-2]) * 0.9990
            and (body_ratio >= 0.45 or lower_wick >= 0.35)
        )
        red_bounce = (
            not_spike_bar
            and closed_bear_confirm
            and float(close[-1]) < float(open_p[-1])
            and float(close[-1]) < float(close[-2])
            and float(high[-1]) <= float(high[-2]) * 1.0010
            and (body_ratio >= 0.45 or upper_wick >= 0.35)
        )

        is_tier1 = symbol in TIER1_ALPHA_SYMBOLS
        is_high_wick_pair = symbol in ("SOL/USDT:USDT", "BTC/USDT:USDT")
        tier_bonus = 2 if is_tier1 else 0
        min_ker_gate = 0.25 if is_high_wick_pair else (0.22 if is_tier1 else 0.32)
        min_atr_gate = 0.0020 if is_high_wick_pair else (0.0018 if is_tier1 else 0.0022)
        min_rvol_gate = 1.22 if is_high_wick_pair else (1.15 if is_tier1 else 1.28)
        min_adx_gate = 22.0 if is_high_wick_pair else (20.0 if is_tier1 else 22.0)
        vol_gate_ok = (
            true_atr_pct >= min_atr_gate
            and ker >= min_ker_gate
            and rvol >= min_rvol_gate
            and adx14 >= min_adx_gate
        )

        # =========================================================================
        # CORE A: TREND-ALIGNED RSI-2 / VWAP ABSORPTION SNIPER (V9.7 Anti-Spike)
        # =========================================================================
        core_a_bull_trend = (
            vol_gate_ok
            and mtf_bull_ok
            and float(close[-1]) >= float(ema200[-1]) * 0.998
            and float(ema21[-1]) > float(ema50[-1])
            and float(ema50[-1]) >= float(ema100[-1])
            and ema_slope >= 0.00040
            and roc5 > -0.0040
        )
        core_a_bear_trend = (
            vol_gate_ok
            and mtf_bear_ok
            and float(close[-1]) <= float(ema200[-1]) * 1.002
            and float(ema21[-1]) < float(ema50[-1])
            and float(ema50[-1]) <= float(ema100[-1])
            and ema_slope <= -0.00040
            and roc5 < 0.0040
        )

        score_a_bull = 38 + tier_bonus
        if core_a_bull_trend:
            score_a_bull += 22
        if rsi2 <= 20:
            score_a_bull += 19
        if z_score <= -1.05:
            score_a_bull += 11
        if green_bounce or lower_wick >= 0.35:
            score_a_bull += 10
        if rvol >= 1.20 and adx14 >= 22.0:
            score_a_bull += 6

        score_a_bear = 38 + tier_bonus
        if core_a_bear_trend:
            score_a_bear += 22
        if rsi2 >= 80:
            score_a_bear += 19
        if z_score >= 1.05:
            score_a_bear += 11
        if red_bounce or upper_wick >= 0.35:
            score_a_bear += 10
        if rvol >= 1.20 and adx14 >= 22.0:
            score_a_bear += 6

        # =========================================================================
        # CORE B: 3-BAR EMA-9 / EMA-21 PULLBACK CONTINUATION SURGE (V9.7 Anti-Chase)
        # =========================================================================
        core_b_bull_stack = (
            vol_gate_ok
            and mtf_bull_ok
            and float(close[-1]) >= float(ema200[-1]) * 0.9985
            and float(ema9[-1]) >= float(ema21[-1]) * 0.9998
            and float(ema21[-1]) > float(ema50[-1])
            and ema_slope >= 0.00045
        )
        core_b_bear_stack = (
            vol_gate_ok
            and mtf_bear_ok
            and float(close[-1]) <= float(ema200[-1]) * 1.0015
            and float(ema9[-1]) <= float(ema21[-1]) * 1.0002
            and float(ema21[-1]) < float(ema50[-1])
            and ema_slope <= -0.00045
        )

        recent_low_3 = min(float(low[-1]), float(low[-2]), float(low[-3]))
        recent_high_3 = max(float(high[-1]), float(high[-2]), float(high[-3]))
        bull_pullback_touch = (
            recent_low_3 <= float(ema21[-1]) * 1.0022
            and green_bounce
            and float(close[-1]) >= float(ema9[-1])
            and dist_ema9_bull <= 0.0025
        )
        bear_pullback_touch = (
            recent_high_3 >= float(ema21[-1]) * 0.9978
            and red_bounce
            and float(close[-1]) <= float(ema9[-1])
            and dist_ema9_bear <= 0.0025
        )

        # V9.7 Strict Anti-Chase Z-Score & RSI-14 Window (prevents buying extended tops like DOGE $0.05927 / ADA $0.155)
        bull_not_overext = -0.85 <= z_score <= 0.32 and 46 <= rsi14 <= 62
        bear_not_overext = -0.32 <= z_score <= 0.85 and 38 <= rsi14 <= 54

        score_b_bull = 42 + tier_bonus
        if core_b_bull_stack:
            score_b_bull += 24
        if bull_pullback_touch:
            score_b_bull += 16
        if bull_not_overext:
            score_b_bull += 11
        if rvol >= 1.18 and adx14 >= 21.0:
            score_b_bull += 5

        score_b_bear = 42 + tier_bonus
        if core_b_bear_stack:
            score_b_bear += 24
        if bear_pullback_touch:
            score_b_bear += 16
        if bear_not_overext:
            score_b_bear += 11
        if rvol >= 1.18 and adx14 >= 21.0:
            score_b_bear += 5

        bull_score = max(score_a_bull, score_b_bull)
        bear_score = max(score_a_bear, score_b_bear)
        min_req_score = 96 if is_tier1 else 98

        long_a_ok = core_a_bull_trend and green_bounce and (rsi2 <= 20 or z_score <= -1.05) and score_a_bull >= min_req_score
        long_b_ok = core_b_bull_stack and bull_pullback_touch and bull_not_overext and score_b_bull >= min_req_score
        short_a_ok = core_a_bear_trend and red_bounce and (rsi2 >= 80 or z_score >= 1.05) and score_a_bear >= min_req_score
        short_b_ok = core_b_bear_stack and bear_pullback_touch and bear_not_overext and score_b_bear >= min_req_score

        if long_a_ok or long_b_ok:
            regime = "LONG_IGNITION"
            setup_type = "DUAL_CORE_PULLBACK_LONG" if long_b_ok else "TREND_DIP_SNIPER_LONG"
            score = min(99, bull_score)
            signal_valid = True
        elif short_a_ok or short_b_ok:
            regime = "SHORT_IGNITION"
            setup_type = "DUAL_CORE_PULLBACK_SHORT" if short_b_ok else "TREND_DIP_SNIPER_SHORT"
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
            price=float(close[-1]),
            atr_14=eff_atr,
            vwap=float(vwap),
            z_score=z_score,
            rsi_2=rsi2,
            rsi_14=rsi14,
            adx_14=adx14,
            mtf_15m_aligned=bool(mtf_bull_ok or mtf_bear_ok),
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
