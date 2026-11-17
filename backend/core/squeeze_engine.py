"""
APEX-OMEGA OS V15.0 BIG-WAVE AUTO-OPTIMIZER — Multi-Architecture Breakout, Swing & Asymmetric Wave Engine
Supports 8 orthogonal quantitative signal families dynamically selected by the Real-Candle Tournament Engine:
1. BRK20 (20-Bar Donchian & Volume Breakout Surge)
2. BRK48 (48-Bar 4-Hour Macro Range Breakout)
3. KELTNER_IGNITION (Bollinger-Keltner Squeeze Breakout Ignition)
4. SUPERTREND (EMA-9/21/50 Impulse Cross + Multi-Hour Momentum Swing)
5. ASYM_WAVE (V9 Dual-Core Pullback Sniper with Big-Wave 1:4 R:R)
6. INVERSE_TRAP (Counter-Trap Breakdown/Squeeze Runner)
7. LIQ_SWEEP (Turtle-Soup 20-Bar Liquidity Sweep & Rejection Reversal)
8. CAPITULATION (2.0-Sigma Bollinger/VWAP Washout Reversal)
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
        self.active_family = os.getenv("ACTIVE_SIGNAL_FAMILY", "BRK20").strip().upper()

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

    def evaluate_ohlcv(self, symbol: str, ohlcv: List[List[float]], feed_source: str = "DELTA_INDIA_5M", family_override: str = "") -> QuantV9Signal:
        arr = np.array(ohlcv, dtype=float)
        live_tick_price = float(arr[-1, 4])
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

        vol_baseline = float(np.mean(vol[-21:-1])) if len(vol) > 21 else float(np.mean(vol))
        cur_vol = max(float(vol[-1]), float(vol[-2]) if len(vol) > 1 else 0.0)
        rvol = round(cur_vol / max(vol_baseline, 1e-9), 2)

        roc5_idx = max(0, len(close) - 6)
        roc5 = round((float(close[-1]) - float(close[roc5_idx])) / max(float(close[roc5_idx]), 1e-9), 5)
        roc12_idx = max(0, len(close) - 13)
        roc12 = round((float(close[-1]) - float(close[roc12_idx])) / max(float(close[roc12_idx]), 1e-9), 5)

        ker_win = min(10, len(close) - 1)
        net_move = abs(float(close[-1]) - float(close[-1 - ker_win]))
        path_sum = float(np.sum(np.abs(np.diff(close[-1 - ker_win :]))))
        ker = round(net_move / max(path_sum, 1e-12), 2)

        cn = float(close[-1])
        on = float(open_p[-1])
        cp = float(close[-2]) if len(close) > 1 else cn
        hn = float(high[-1])
        ln = float(low[-1])
        cur_range = max(hn - ln, 1e-12)
        not_sp = cur_range <= float(atr14[-1]) * 2.40
        lw = (min(on, cn) - ln) / cur_range
        uw = (hn - max(on, cn)) / cur_range
        gb = not_sp and cn > on and cn >= cp
        rb = not_sp and cn < on and cn <= cp

        don_hi20 = float(np.max(high[-21:-1])) if len(high) > 21 else hn
        don_lo20 = float(np.min(low[-21:-1])) if len(low) > 21 else ln
        don_hi48 = float(np.max(high[-49:-1])) if len(high) > 49 else don_hi20
        don_lo48 = float(np.min(low[-49:-1])) if len(low) > 49 else don_lo20

        is_tier1 = symbol in TIER1_ALPHA_SYMBOLS
        fam = (family_override or self.active_family or "BRK20").split("__")[0].strip().upper()

        # Evaluate all families
        brk20_l = is_tier1 and cn > don_hi20 and cn > on and rvol >= 1.12 and ker >= 0.20 and float(ema9[-1]) > float(ema21[-1]) and roc5 >= 0.0018
        brk20_s = is_tier1 and cn < don_lo20 and cn < on and rvol >= 1.12 and ker >= 0.20 and float(ema9[-1]) < float(ema21[-1]) and roc5 <= -0.0018

        brk48_l = is_tier1 and cn > don_hi48 and cn > on and rvol >= 1.10 and float(ema21[-1]) > float(ema50[-1]) and roc12 >= 0.0025
        brk48_s = is_tier1 and cn < don_lo48 and cn < on and rvol >= 1.10 and float(ema21[-1]) < float(ema50[-1]) and roc12 <= -0.0025

        st_l = is_tier1 and len(ema9) > 2 and float(ema9[-2]) <= float(ema21[-2]) and float(ema9[-1]) > float(ema21[-1]) and cn > float(ema50[-1]) and roc12 >= 0.0028 and rvol >= 1.05
        st_s = is_tier1 and len(ema9) > 2 and float(ema9[-2]) >= float(ema21[-2]) and float(ema9[-1]) < float(ema21[-1]) and cn < float(ema50[-1]) and roc12 <= -0.0028 and rvol >= 1.05

        cab = is_tier1 and cn >= float(ema200[-1]) * 0.996 and float(ema21[-1]) > float(ema50[-1]) >= float(ema100[-1]) * 0.9995 and ema_slope >= 0.0003 and roc5 > -0.006
        cae = is_tier1 and cn <= float(ema200[-1]) * 1.004 and float(ema21[-1]) < float(ema50[-1]) <= float(ema100[-1]) * 1.0005 and ema_slope <= -0.0003 and roc5 < 0.006
        cbb = is_tier1 and cn >= float(ema200[-1]) * 0.998 and float(ema9[-1]) >= float(ema21[-1]) * 0.9995 and float(ema21[-1]) > float(ema50[-1]) and ema_slope >= 0.00035
        cbe = is_tier1 and cn <= float(ema200[-1]) * 1.002 and float(ema9[-1]) <= float(ema21[-1]) * 1.0005 and float(ema21[-1]) < float(ema50[-1]) and ema_slope <= -0.00035
        l3 = min(ln, float(low[-2]), float(low[-3])) if len(low) >= 3 else ln
        h3 = max(hn, float(high[-2]), float(high[-3])) if len(high) >= 3 else hn
        bt_ = l3 <= float(ema21[-1]) * 1.0035 and gb and cn >= float(ema9[-1]) * 0.9995 and -1.35 <= z_score <= 0.90 and 42 <= rsi14 <= 70
        st_p = h3 >= float(ema21[-1]) * 0.9965 and rb and cn <= float(ema9[-1]) * 1.0005 and -0.90 <= z_score <= 1.35 and 30 <= rsi14 <= 58
        aw_l = (cab and gb and (rsi2 <= 22 or z_score <= -1.0)) or (cbb and bt_)
        aw_s = (cae and rb and (rsi2 >= 78 or z_score >= 1.0)) or (cbe and st_p)

        inv_l = aw_s
        inv_s = aw_l

        cap_l = is_tier1 and z_score <= -1.85 and rsi2 <= 12.0 and (cn > on or lw >= 0.35) and not_sp
        cap_s = is_tier1 and z_score >= 1.85 and rsi2 >= 88.0 and (cn < on or uw >= 0.35) and not_sp

        ib_s = is_tier1 and hn > don_hi20 and cn < don_hi20 and cn < on and rvol >= 1.05
        ib_l = is_tier1 and ln < don_lo20 and cn > don_lo20 and cn > on and rvol >= 1.05

        tf_l = is_tier1 and float(ema9[-1]) > float(ema21[-1]) > float(ema50[-1]) > float(ema200[-1]) and cn > float(ema9[-1]) and cn > on and ker >= 0.26 and roc12 >= 0.0035 and rvol >= 1.12
        tf_s = is_tier1 and float(ema9[-1]) < float(ema21[-1]) < float(ema50[-1]) < float(ema200[-1]) and cn < float(ema9[-1]) and cn < on and ker >= 0.26 and roc12 <= -0.0035 and rvol >= 1.12

        fam_map = {
            "BRK20": (brk20_l, brk20_s, "DONCHIAN_20_BREAKOUT"),
            "BRK48": (brk48_l, brk48_s, "MACRO_48_BREAKOUT"),
            "SUPERTREND": (st_l, st_s, "SUPERTREND_SWING"),
            "ASYM_WAVE": (aw_l, aw_s, "V9_BIG_WAVE_PULLBACK"),
            "INVERSE_TRAP": (inv_l, inv_s, "COUNTER_TRAP_WAVE"),
            "CAPITULATION": (cap_l, cap_s, "SIGMA_CAPITULATION"),
            "INVERSE_BRK": (ib_l, ib_s, "LIQ_SWEEP_REVERSAL"),
            "HTF_TREND_FLOW": (tf_l, tf_s, "HTF_TREND_CONTINUATION"),
        }
        go_l, go_s, stype = fam_map.get(fam, (brk20_l or brk48_l, brk20_s or brk48_s, "DONCHIAN_BREAKOUT"))

        if go_l:
            regime = "LONG_IGNITION"
            setup_type = f"{stype}_LONG"
            score = 98
            signal_valid = True
        elif go_s:
            regime = "SHORT_IGNITION"
            setup_type = f"{stype}_SHORT"
            score = 98
            signal_valid = True
        else:
            regime = "COILING" if squeeze_now else "NEUTRAL"
            setup_type = "SCANNING"
            score = int(np.clip(55 + int(rvol * 10) + int(ker * 20), 30, 92))
            signal_valid = False

        raw_atr = float(atr14[-1])
        eff_atr = max(raw_atr, cn * 0.0055)

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
