"use client";
import React, { useState, useEffect, useRef, useCallback } from "react";

interface AssetSignal {
  symbol: string;
  ticker: string;
  bybitSymbol: string;
  okxInstId: string;
  krakenPair: string;
  deltaSymbol: string;
  coingeckoId: string;
  price: number;
  atr_14: number;
  vwap: number;
  z_score: number;
  rsi_2: number;
  rsi_14: number;
  compression_ratio: number;
  squeeze_on: boolean;
  squeeze_bars_last_10: number;
  ema_trend_slope: number;
  rvol: number;
  roc5: number;
  ker: number;
  confluence_score: number;
  setup_type: string;
  regime: "LONG_IGNITION" | "SHORT_IGNITION" | "COILING" | "NEUTRAL";
  signal_valid: boolean;
  feed_source: string;
}

interface TrancheUnit {
  id: number;
  label: string;
  weight_pct: number;
  trigger_atr_mult: number;
  trigger_price: number;
  filled: boolean;
  fill_price: number | null;
  notional_inr: number;
  unrealized_pnl_inr: number;
  fee_inr: number;
}

interface ActiveCampaign {
  trade_id: string;
  symbol: string;
  delta_symbol?: string;
  side: "LONG" | "SHORT";
  setup_type: string;
  confluence_score: number;
  entry_time: string;
  entry_price_raw?: number;
  mark_price_raw?: number;
  base_entry_price: number;
  current_price: number;
  highest_extreme: number;
  atr_at_entry: number;
  tp1_price: number;
  tp2_price?: number;
  tp1_hit: boolean;
  tp1_banked_pnl_inr: number;
  ratchet_stage?: number;
  chandelier_stop: number;
  soft_stop?: number;
  breakeven_locked: boolean;
  breakeven_stop: number | null;
  effective_stop: number;
  filled_units: number;
  max_stage_reached: string;
  total_notional_inr: number;
  total_unrealized_pnl_inr: number;
  rvol_at_entry: number;
  live_qty?: number;
  remaining_qty?: number;
  coin_qty?: number;
  liquidation_price?: number;
  margin_inr?: number;
  stop_order_id?: string | null;
  is_live_delta?: boolean;
  tranches: TrancheUnit[];
}

interface ClosedTradeRecord {
  trade_id: string;
  symbol: string;
  delta_symbol?: string;
  side: "LONG" | "SHORT";
  setup_type?: string;
  confluence_score?: number;
  entry_time: string;
  exit_time: string;
  entry_price: number;
  exit_price: number;
  max_stage_reached: string;
  filled_units: number;
  total_notional_inr: number;
  realized_pnl_inr: number;
  roi_pct: number;
  exit_reason: string;
  balance_after_inr: number;
  outcome: "WIN" | "LOSS";
  is_live_delta?: boolean;
}

interface DeltaLivePosition {
  product_id?: number;
  delta_symbol: string;
  symbol: string;
  side: "LONG" | "SHORT";
  contracts: number;
  contract_value: number;
  coin_qty: number;
  entry_price_raw?: number;
  mark_price_raw?: number;
  entry_price: number;
  mark_price: number;
  liquidation_price: number;
  margin_inr: number;
  notional_inr: number;
  unrealized_pnl_inr: number;
  unrealized_pnl_usd: number;
  realized_pnl_inr: number;
  roi_pct: number;
  stop_order_id?: string | null;
  stop_price?: number | null;
  updated_at?: string;
}

interface DeltaLiveFill {
  fill_id: string;
  order_id: string;
  delta_symbol: string;
  symbol: string;
  side: string;
  contracts: number;
  coin_qty: number;
  price: number;
  notional_inr: number;
  fee_inr: number;
  realized_pnl_inr: number | null;
  role: string;
  fill_type: string;
  timestamp: string;
}

interface TradePopupAlert {
  id: string;
  timestamp: string;
  event_type: "TRADE_ENTRY" | "TRADE_SCALE" | "TRADE_EXIT" | "VAULT_SIPHON";
  title: string;
  message: string;
  pnl_inr: number;
  trade_id?: string;
}

const UNIVERSE_PAIRS = [
  { symbol: "LINK/USDT:USDT", ticker: "LINKUSDT", bybitSymbol: "LINKUSDT", okxInstId: "LINK-USDT-SWAP", krakenPair: "LINKUSD", deltaSymbol: "LINKUSD", coingeckoId: "chainlink", seedPrice: 11.55, seedAtr: 0.095 },
  { symbol: "DOGE/USDT:USDT", ticker: "DOGEUSDT", bybitSymbol: "DOGEUSDT", okxInstId: "DOGE-USDT-SWAP", krakenPair: "XDGUSD", deltaSymbol: "DOGEUSD", coingeckoId: "dogecoin", seedPrice: 0.0768, seedAtr: 0.0007 },
  { symbol: "SOL/USDT:USDT", ticker: "SOLUSDT", bybitSymbol: "SOLUSDT", okxInstId: "SOL-USDT-SWAP", krakenPair: "SOLUSD", deltaSymbol: "SOLUSD", coingeckoId: "solana", seedPrice: 95.86, seedAtr: 0.68 },
  { symbol: "AVAX/USDT:USDT", ticker: "AVAXUSDT", bybitSymbol: "AVAXUSDT", okxInstId: "AVAX-USDT-SWAP", krakenPair: "AVAXUSD", deltaSymbol: "AVAXUSD", coingeckoId: "avalanche-2", seedPrice: 8.84, seedAtr: 0.082 },
  { symbol: "ADA/USDT:USDT", ticker: "ADAUSDT", bybitSymbol: "ADAUSDT", okxInstId: "ADA-USDT-SWAP", krakenPair: "ADAUSD", deltaSymbol: "ADAUSD", coingeckoId: "cardano", seedPrice: 0.3650, seedAtr: 0.0032 },
  { symbol: "BTC/USDT:USDT", ticker: "BTCUSDT", bybitSymbol: "BTCUSDT", okxInstId: "BTC-USDT-SWAP", krakenPair: "XBTUSD", deltaSymbol: "BTCUSD", coingeckoId: "bitcoin", seedPrice: 80112.50, seedAtr: 380.00 },
  { symbol: "ETH/USDT:USDT", ticker: "ETHUSDT", bybitSymbol: "ETHUSDT", okxInstId: "ETH-USDT-SWAP", krakenPair: "ETHUSD", deltaSymbol: "ETHUSD", coingeckoId: "ethereum", seedPrice: 2419.40, seedAtr: 14.80 },
  { symbol: "SUI/USDT:USDT", ticker: "SUIUSDT", bybitSymbol: "SUIUSDT", okxInstId: "SUI-USDT-SWAP", krakenPair: "SUIUSD", deltaSymbol: "SUIUSD", coingeckoId: "sui", seedPrice: 0.9366, seedAtr: 0.0085 },
  { symbol: "XRP/USDT:USDT", ticker: "XRPUSDT", bybitSymbol: "XRPUSDT", okxInstId: "XRP-USDT-SWAP", krakenPair: "XRPUSD", deltaSymbol: "XRPUSD", coingeckoId: "ripple", seedPrice: 1.3048, seedAtr: 0.0110 },
  { symbol: "BNB/USDT:USDT", ticker: "BNBUSDT", bybitSymbol: "BNBUSDT", okxInstId: "BNB-USDT-SWAP", krakenPair: "BNBUSD", deltaSymbol: "BNBUSD", coingeckoId: "binancecoin", seedPrice: 715.76, seedAtr: 4.10 },
];

const DELTA_TO_SCANNER_MAP: Record<string, string> = {};
UNIVERSE_PAIRS.forEach((p) => {
  DELTA_TO_SCANNER_MAP[p.deltaSymbol] = p.symbol;
  DELTA_TO_SCANNER_MAP[p.ticker] = p.symbol;
});

const DEFAULT_RENDER_BACKEND_URL = "https://apex-omega-v9-original-live.onrender.com";
const USD_INR_RATE = 85.0;
// Exact Delta Exchange India Taker Fee (0.05%) + 18% GST = 0.059% per side (0.118% round-trip)
const DELTA_FEE_WITH_GST_RATE = 0.00059;

function fmtPrice(val: number): number {
  if (!val || isNaN(val)) return 0;
  if (Math.abs(val) < 0.01) return +val.toFixed(8);
  if (Math.abs(val) < 1.0) return +val.toFixed(6);
  if (Math.abs(val) < 50.0) return +val.toFixed(5);
  return +val.toFixed(3);
}

/**
 * Single Authoritative PnL Function — Matches Backend `compute_exact_position_pnl` 1:1.
 * Uses full-precision entryPriceRaw, markPriceRaw, and coinQty so PnL NEVER flickers green/red!
 */
function computeExactPositionPnlInr(
  side: "LONG" | "SHORT" | string,
  entryPriceRaw: number,
  markPriceRaw: number,
  coinQty: number | undefined,
  notionalInr: number
): number {
  if (!entryPriceRaw || !markPriceRaw || entryPriceRaw <= 0 || markPriceRaw <= 0) {
    return 0.0;
  }
  const dir = String(side).toUpperCase() === "LONG" ? 1.0 : -1.0;
  const effCoinQty =
    coinQty && coinQty > 0
      ? coinQty
      : (notionalInr / USD_INR_RATE) / Math.max(entryPriceRaw, 1e-12);
  const upnlUsd = dir * (markPriceRaw - entryPriceRaw) * effCoinQty;
  return +(upnlUsd * USD_INR_RATE).toFixed(2);
}

const STORAGE_KEY = "apex_omega_real_state_v95_orig_live";
const RENDER_URL_KEY = "apex_omega_render_url_v9_orig_live";

export default function App() {
  const [activeTab, setActiveTab] = useState<"monitor" | "trades" | "cloud">("monitor");
  const [tradeSubTab, setTradeSubTab] = useState<"closed" | "fills">("closed");
  const [tradeFilter, setTradeFilter] = useState<"ALL" | "WIN" | "LOSS">("ALL");

  const [executionMode, setExecutionMode] = useState<string>("LIVE");
  const [startingEquity, setStartingEquity] = useState<number>(10000.0);
  const [activeEquity, setActiveEquity] = useState<number>(10000.0);
  const [peakEquity, setPeakEquity] = useState<number>(10000.0);
  const [vaultBalance, setVaultBalance] = useState<number>(0.0);
  const [principalSiphoned, setPrincipalSiphoned] = useState<boolean>(false);
  const [nextMilestone, setNextMilestone] = useState<number>(1000000.0);

  const [deltaConnected, setDeltaConnected] = useState<boolean>(false);
  const [deltaWsConnected, setDeltaWsConnected] = useState<boolean>(false);
  const [browserDeltaWsLive, setBrowserDeltaWsLive] = useState<boolean>(false);
  const [backendWsLive, setBackendWsLive] = useState<boolean>(false);
  const [deltaLastSyncTs, setDeltaLastSyncTs] = useState<string | null>(null);
  const [deltaWalletInr, setDeltaWalletInr] = useState<number | null>(null);
  const [deltaAvailableInr, setDeltaAvailableInr] = useState<number | null>(null);
  const [deltaPositionMarginInr, setDeltaPositionMarginInr] = useState<number>(0.0);
  const [deltaWalletUsd, setDeltaWalletUsd] = useState<number>(0.0);
  const [deltaOpenPnlInr, setDeltaOpenPnlInr] = useState<number | null>(null);
  const [deltaOpenPositions, setDeltaOpenPositions] = useState<DeltaLivePosition[]>([]);
  const [deltaFills, setDeltaFills] = useState<DeltaLiveFill[]>([]);
  const [deltaApiError, setDeltaApiError] = useState<string | null>(null);
  const [serverOutboundIp, setServerOutboundIp] = useState<string>("DETECTING...");
  const [isSyncingDelta, setIsSyncingDelta] = useState<boolean>(false);

  const [scanner, setScanner] = useState<AssetSignal[]>(() =>
    UNIVERSE_PAIRS.map((p) => ({
      symbol: p.symbol,
      ticker: p.ticker,
      bybitSymbol: p.bybitSymbol,
      okxInstId: p.okxInstId,
      krakenPair: p.krakenPair,
      deltaSymbol: p.deltaSymbol,
      coingeckoId: p.coingeckoId,
      price: p.seedPrice,
      atr_14: p.seedAtr,
      vwap: p.seedPrice,
      z_score: 0.0,
      rsi_2: 50.0,
      rsi_14: 50.0,
      compression_ratio: 0.92,
      squeeze_on: false,
      squeeze_bars_last_10: 2,
      ema_trend_slope: 0.0005,
      rvol: 1.25,
      roc5: 0.0035,
      ker: 0.34,
      confluence_score: 62,
      setup_type: "SCANNING",
      regime: "COILING",
      signal_valid: false,
      feed_source: "CONNECTING...",
    }))
  );
  const [activeCampaigns, setActiveCampaigns] = useState<ActiveCampaign[]>([]);
  const [tradeHistory, setTradeHistory] = useState<ClosedTradeRecord[]>([]);
  const [popupAlerts, setPopupAlerts] = useState<TradePopupAlert[]>([]);
  const [activePopup, setActivePopup] = useState<TradePopupAlert | null>(null);
  const [selectedTradeModal, setSelectedTradeModal] = useState<ClosedTradeRecord | null>(null);

  const [renderUrlInput, setRenderUrlInput] = useState<string>(DEFAULT_RENDER_BACKEND_URL);
  const [renderUrl, setRenderUrl] = useState<string>(DEFAULT_RENDER_BACKEND_URL);
  const [feedBadge, setFeedBadge] = useState<string>("CONNECTING TO V9.5 0MS DELTA STREAM...");
  const [lastScanTime, setLastScanTime] = useState<string>("Syncing...");

  const liveWsMarksRef = useRef<Record<string, number>>({});
  const liveWsTickTsRef = useRef<Record<string, number>>({});
  const inFlightPollRef = useRef<boolean>(false);

  useEffect(() => {
    try {
      const envUrl =
        typeof process !== "undefined" && process.env && process.env.NEXT_PUBLIC_API_URL
          ? process.env.NEXT_PUBLIC_API_URL
          : "";
      const savedUrl = localStorage.getItem(RENDER_URL_KEY) || envUrl || DEFAULT_RENDER_BACKEND_URL;
      if (savedUrl) {
        const cleaned = savedUrl.replace(/\/+$/, "");
        setRenderUrl(cleaned);
        setRenderUrlInput(cleaned);
      }

      const raw = localStorage.getItem(STORAGE_KEY);
      if (raw) {
        const parsed = JSON.parse(raw);
        if (typeof parsed.activeEquity === "number") setActiveEquity(parsed.activeEquity);
        if (typeof parsed.peakEquity === "number") setPeakEquity(parsed.peakEquity);
        if (typeof parsed.vaultBalance === "number") setVaultBalance(parsed.vaultBalance);
        if (Array.isArray(parsed.activeCampaigns)) setActiveCampaigns(parsed.activeCampaigns);
        if (Array.isArray(parsed.tradeHistory)) setTradeHistory(parsed.tradeHistory);
      }
    } catch {
      // Ignore storage errors
    }
  }, []);

  useEffect(() => {
    try {
      localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
          activeEquity,
          peakEquity,
          vaultBalance,
          principalSiphoned,
          nextMilestone,
          activeCampaigns,
          tradeHistory,
          popupAlerts,
        })
      );
    } catch {
      // Ignore storage errors
    }
  }, [activeEquity, peakEquity, vaultBalance, principalSiphoned, nextMilestone, activeCampaigns, tradeHistory, popupAlerts]);

  const applyServerPayload = useCallback((data: any) => {
    if (!data || typeof data !== "object") return;
    const nowMs = Date.now();

    if (data.mode) setExecutionMode(String(data.mode));
    if (typeof data.starting_equity === "number" && data.starting_equity > 0) {
      setStartingEquity(data.starting_equity);
    }
    if (typeof data.realized_base_equity === "number") {
      setActiveEquity(data.realized_base_equity);
    } else if (typeof data.active_equity === "number") {
      setActiveEquity(data.active_equity);
    }
    if (typeof data.kelly?.peak_equity === "number") {
      setPeakEquity(data.kelly.peak_equity);
    }
    if (typeof data.vault_balance === "number") setVaultBalance(data.vault_balance);
    setPrincipalSiphoned(Boolean(data.principal_siphoned));
    if (typeof data.next_siphon_milestone === "number") setNextMilestone(data.next_siphon_milestone);

    setDeltaConnected(Boolean(data.delta_connected));
    setDeltaWsConnected(Boolean(data.delta_ws_connected));
    if (data.delta_last_sync_ts) setDeltaLastSyncTs(String(data.delta_last_sync_ts));
    if (typeof data.delta_wallet_inr === "number") setDeltaWalletInr(data.delta_wallet_inr);
    if (typeof data.delta_available_inr === "number") setDeltaAvailableInr(data.delta_available_inr);
    if (typeof data.delta_position_margin_inr === "number") setDeltaPositionMarginInr(data.delta_position_margin_inr);
    if (typeof data.delta_wallet_usd === "number") setDeltaWalletUsd(data.delta_wallet_usd);
    if (Array.isArray(data.delta_fills)) setDeltaFills(data.delta_fills);
    setDeltaApiError(data.delta_api_error ? String(data.delta_api_error) : null);
    if (data.server_outbound_ip) setServerOutboundIp(String(data.server_outbound_ip));

    if (Array.isArray(data.scanner) && data.scanner.length > 0) {
      setScanner((prev) =>
        prev.map((oldItem) => {
          const updated = data.scanner.find((x: any) => x.symbol === oldItem.symbol);
          if (!updated) return oldItem;
          const wsMark = liveWsMarksRef.current[oldItem.symbol];
          const wsAge = nowMs - (liveWsTickTsRef.current[oldItem.symbol] || 0);
          if (wsMark && wsMark > 0 && wsAge < 5000) {
            return {
              ...oldItem,
              ...updated,
              price: fmtPrice(wsMark),
              feed_source: "DELTA_WS_0MS",
            };
          }
          return { ...oldItem, ...updated };
        })
      );
    }

    if (Array.isArray(data.delta_open_positions)) {
      let totalPosPnl = 0;
      const syncedPositions: DeltaLivePosition[] = data.delta_open_positions.map((pos: DeltaLivePosition) => {
        const wsMark = liveWsMarksRef.current[pos.symbol] || liveWsMarksRef.current[pos.delta_symbol];
        const wsAge = nowMs - (liveWsTickTsRef.current[pos.symbol] || 0);
        const entryRaw = Number(pos.entry_price_raw ?? pos.entry_price ?? 0);
        const markRaw =
          wsMark && wsMark > 0 && wsAge < 5000
            ? wsMark
            : Number(pos.mark_price_raw ?? pos.mark_price ?? entryRaw);

        const exactPnlInr = computeExactPositionPnlInr(
          pos.side,
          entryRaw,
          markRaw,
          pos.coin_qty,
          pos.notional_inr
        );
        totalPosPnl += exactPnlInr;
        return {
          ...pos,
          entry_price_raw: entryRaw,
          mark_price_raw: markRaw,
          mark_price: fmtPrice(markRaw),
          unrealized_pnl_inr: exactPnlInr,
        };
      });
      setDeltaOpenPositions(syncedPositions);
      setDeltaOpenPnlInr(+totalPosPnl.toFixed(2));
    } else if (typeof data.delta_open_pnl_inr === "number") {
      setDeltaOpenPnlInr(data.delta_open_pnl_inr);
    }

    const rawCamps: ActiveCampaign[] = Array.isArray(data.active_campaigns)
      ? data.active_campaigns
      : data.pyramid
      ? [data.pyramid]
      : [];

    const syncedCamps: ActiveCampaign[] = rawCamps.map((camp) => {
      const wsMark =
        liveWsMarksRef.current[camp.symbol] ||
        (camp.delta_symbol ? liveWsMarksRef.current[camp.delta_symbol] : undefined);
      const wsAge = nowMs - (liveWsTickTsRef.current[camp.symbol] || 0);
      const entryRaw = Number(camp.entry_price_raw ?? camp.base_entry_price ?? 0);
      const markRaw =
        wsMark && wsMark > 0 && wsAge < 5000
          ? wsMark
          : Number(camp.mark_price_raw ?? camp.current_price ?? entryRaw);

      const exactPnlInr = computeExactPositionPnlInr(
        camp.side,
        entryRaw,
        markRaw,
        camp.coin_qty,
        camp.total_notional_inr
      );

      return {
        ...camp,
        entry_price_raw: entryRaw,
        mark_price_raw: markRaw,
        current_price: fmtPrice(markRaw),
        total_unrealized_pnl_inr: exactPnlInr,
      };
    });
    setActiveCampaigns(syncedCamps);

    if (Array.isArray(data.trade_history)) {
      setTradeHistory(data.trade_history);
    }
    setLastScanTime(new Date().toLocaleTimeString());
  }, []);

  /**
   * STREAM 1: Direct Browser WebSocket to Delta Exchange India (wss://socket.india.delta.exchange)
   */
  useEffect(() => {
    let ws: WebSocket | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let active = true;

    const connectDeltaSocket = () => {
      if (!active || typeof window === "undefined") return;
      try {
        ws = new WebSocket("wss://socket.india.delta.exchange");
        ws.onopen = () => {
          if (!active || !ws) return;
          setBrowserDeltaWsLive(true);
          ws.send(JSON.stringify({ type: "enable_heartbeat" }));
          ws.send(
            JSON.stringify({
              type: "subscribe",
              payload: {
                channels: [
                  {
                    name: "v2/ticker",
                    symbols: UNIVERSE_PAIRS.map((p) => p.deltaSymbol),
                  },
                ],
              },
            })
          );
        };

        ws.onmessage = (event) => {
          if (!active) return;
          try {
            const msg = JSON.parse(event.data);
            if (msg?.type === "v2/ticker" && msg.symbol) {
              const dSym = String(msg.symbol).toUpperCase();
              const scannerSym = DELTA_TO_SCANNER_MAP[dSym];
              const mkPriceRaw = parseFloat(msg.mark_price || msg.close || "0");
              if (!scannerSym || !mkPriceRaw || isNaN(mkPriceRaw) || mkPriceRaw <= 0) return;

              const nowMs = Date.now();
              liveWsMarksRef.current[scannerSym] = mkPriceRaw;
              liveWsMarksRef.current[dSym] = mkPriceRaw;
              liveWsTickTsRef.current[scannerSym] = nowMs;

              const cleanPrice = fmtPrice(mkPriceRaw);

              setScanner((prev) =>
                prev.map((item) =>
                  item.symbol === scannerSym
                    ? { ...item, price: cleanPrice, feed_source: "DELTA_WS_0MS" }
                    : item
                )
              );

              setActiveCampaigns((prev) => {
                if (prev.length === 0) return prev;
                let changed = false;
                const next = prev.map((camp) => {
                  if (camp.symbol !== scannerSym && camp.delta_symbol !== dSym) return camp;
                  changed = true;
                  const entryRaw = Number(camp.entry_price_raw ?? camp.base_entry_price ?? mkPriceRaw);
                  const newPnl = computeExactPositionPnlInr(
                    camp.side,
                    entryRaw,
                    mkPriceRaw,
                    camp.coin_qty,
                    camp.total_notional_inr
                  );
                  return {
                    ...camp,
                    mark_price_raw: mkPriceRaw,
                    current_price: cleanPrice,
                    total_unrealized_pnl_inr: newPnl,
                  };
                });
                return changed ? next : prev;
              });

              setDeltaOpenPositions((prev) => {
                if (prev.length === 0) return prev;
                let changed = false;
                let sumUpnl = 0;
                const next = prev.map((pos) => {
                  if (pos.symbol !== scannerSym && pos.delta_symbol !== dSym) {
                    sumUpnl += pos.unrealized_pnl_inr;
                    return pos;
                  }
                  changed = true;
                  const entryRaw = Number(pos.entry_price_raw ?? pos.entry_price ?? mkPriceRaw);
                  const upnlInr = computeExactPositionPnlInr(
                    pos.side,
                    entryRaw,
                    mkPriceRaw,
                    pos.coin_qty,
                    pos.notional_inr
                  );
                  sumUpnl += upnlInr;
                  return {
                    ...pos,
                    mark_price_raw: mkPriceRaw,
                    mark_price: cleanPrice,
                    unrealized_pnl_inr: upnlInr,
                  };
                });
                if (changed) {
                  setDeltaOpenPnlInr(+sumUpnl.toFixed(2));
                  return next;
                }
                return prev;
              });
            }
          } catch {
            // Ignore malformed socket frames
          }
        };

        ws.onclose = () => {
          setBrowserDeltaWsLive(false);
          if (active) {
            reconnectTimer = setTimeout(connectDeltaSocket, 1000);
          }
        };

        ws.onerror = () => {
          try {
            ws?.close();
          } catch {
            // Ignore
          }
        };
      } catch {
        if (active) {
          reconnectTimer = setTimeout(connectDeltaSocket, 1500);
        }
      }
    };

    connectDeltaSocket();
    return () => {
      active = false;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      try {
        ws?.close();
      } catch {
        // Ignore
      }
    };
  }, []);

  /**
   * STREAM 2: Direct WebSocket to Render Backend (/ws)
   */
  useEffect(() => {
    if (!renderUrl || typeof window === "undefined") return;
    let ws: WebSocket | null = null;
    let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
    let active = true;

    const connectBackendWs = () => {
      if (!active) return;
      try {
        const wsEndpoint = renderUrl.replace(/^http/i, "ws") + "/ws";
        ws = new WebSocket(wsEndpoint);
        ws.onopen = () => {
          if (!active) return;
          setBackendWsLive(true);
          setFeedBadge("V9.5 FEE-SHIELD 0MS WEBSOCKET SYNCED");
        };
        ws.onmessage = (event) => {
          if (!active) return;
          try {
            const data = JSON.parse(event.data);
            applyServerPayload(data);
          } catch {
            // Ignore
          }
        };
        ws.onclose = () => {
          setBackendWsLive(false);
          if (active) {
            reconnectTimer = setTimeout(connectBackendWs, 1000);
          }
        };
        ws.onerror = () => {
          try {
            ws?.close();
          } catch {
            // Ignore
          }
        };
      } catch {
        if (active) {
          reconnectTimer = setTimeout(connectBackendWs, 1500);
        }
      }
    };

    connectBackendWs();
    return () => {
      active = false;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      try {
        ws?.close();
      } catch {
        // Ignore
      }
    };
  }, [renderUrl, applyServerPayload]);

  const handleForceSyncDelta = useCallback(async () => {
    if (!renderUrl) return;
    setIsSyncingDelta(true);
    try {
      const res = await fetch(`${renderUrl}/api/sync-delta`, {
        method: "POST",
        cache: "no-store",
      });
      if (res.ok) {
        const data = await res.json();
        applyServerPayload(data);
        setFeedBadge(
          data.delta_connected
            ? `DELTA INDIA V9.5 0MS LIVE (${data.mode || "LIVE"})`
            : "RENDER 24/7 CLOUD DAEMON SYNCED"
        );
      }
    } catch {
      // Ignore
    } finally {
      setIsSyncingDelta(false);
    }
  }, [renderUrl, applyServerPayload]);

  const campaignUnrealized = +activeCampaigns
    .reduce((acc, c) => acc + (c.total_unrealized_pnl_inr || 0) + (c.tp1_banked_pnl_inr || 0), 0)
    .toFixed(2);
  const unrealizedPnl =
    activeCampaigns.length > 0
      ? campaignUnrealized
      : deltaOpenPositions.length > 0 && deltaOpenPnlInr !== null
      ? deltaOpenPnlInr
      : 0.0;
  const baseWalletEquity =
    deltaConnected && deltaWalletInr !== null && deltaWalletInr > 0 ? deltaWalletInr : activeEquity;
  const liveActiveEquity = +(baseWalletEquity + unrealizedPnl).toFixed(2);
  const currentPeak = Math.max(peakEquity, liveActiveEquity, startingEquity);
  const protectedFloor = +(Math.max(startingEquity, 0.75 * currentPeak)).toFixed(2);
  const totalNetWorth = +(liveActiveEquity + vaultBalance).toFixed(2);
  const totalReturnInr = +(totalNetWorth - startingEquity).toFixed(2);
  const totalReturnPct = +(((totalNetWorth - startingEquity) / Math.max(startingEquity, 1)) * 100).toFixed(2);

  const totalClosed = tradeHistory.length;
  const winsCount = tradeHistory.filter((t) => t.realized_pnl_inr > 0).length;
  const lossesCount = totalClosed - winsCount;
  const winRatePct = totalClosed > 0 ? +((winsCount / totalClosed) * 100).toFixed(1) : 0.0;
  const grossProfit = +tradeHistory
    .filter((t) => t.realized_pnl_inr > 0)
    .reduce((acc, t) => acc + t.realized_pnl_inr, 0)
    .toFixed(2);
  const grossLoss = +tradeHistory
    .filter((t) => t.realized_pnl_inr <= 0)
    .reduce((acc, t) => acc + Math.abs(t.realized_pnl_inr), 0)
    .toFixed(2);
  const netRealizedPnl = +(grossProfit - grossLoss).toFixed(2);

  /**
   * STREAM 3: Non-Overlapping 500ms Fast Poll with AbortController & WS Mark Authority
   */
  useEffect(() => {
    let active = true;

    const runScan = async () => {
      if (inFlightPollRef.current || !renderUrl) return;
      inFlightPollRef.current = true;
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 2500);
      try {
        const apiEndpoint = [renderUrl, "api", "state"].join("/");
        const res = await fetch(apiEndpoint, {
          cache: "no-store",
          signal: controller.signal,
        });
        if (res.ok && active) {
          const data = await res.json();
          applyServerPayload(data);
          setFeedBadge(
            data.delta_connected
              ? `DELTA INDIA V9.5 0MS LIVE (${backendWsLive || browserDeltaWsLive ? "WS + 500ms" : "500ms FAST"})`
              : `RENDER 24/7 CLOUD DAEMON (${data.mode || "LIVE"})`
          );
        }
      } catch {
        // Ignore transient poll timeout; WebSocket stream remains live at 0ms
      } finally {
        clearTimeout(timeoutId);
        inFlightPollRef.current = false;
      }
    };

    runScan();
    const timer = setInterval(runScan, 500);
    return () => {
      active = false;
      clearInterval(timer);
    };
  }, [renderUrl, backendWsLive, browserDeltaWsLive, applyServerPayload]);

  const filteredTrades = tradeHistory.filter((t) => {
    if (tradeFilter === "WIN") return t.outcome === "WIN";
    if (tradeFilter === "LOSS") return t.outcome === "LOSS";
    return true;
  });

  const handleSaveRenderUrl = () => {
    const cleaned = renderUrlInput.trim().replace(/\/+$/, "");
    setRenderUrl(cleaned);
    try {
      localStorage.setItem(RENDER_URL_KEY, cleaned);
    } catch {
      // Ignore
    }
  };

  const handleEmergencyFlattenAll = async () => {
    if (renderUrl) {
      try {
        const res = await fetch(`${renderUrl}/api/kill-switch`, { method: "POST" });
        if (res.ok) {
          const json = await res.json();
          if (json.telemetry) applyServerPayload(json.telemetry);
          return;
        }
      } catch {
        // Fallback
      }
    }
    setActiveCampaigns([]);
  };

  const handleResetPaperAccount = async () => {
    if (renderUrl) {
      try {
        const res = await fetch(`${renderUrl}/api/reset`, { method: "POST" });
        if (res.ok) {
          const json = await res.json();
          if (json.telemetry) applyServerPayload(json.telemetry);
        }
      } catch {
        // Ignore
      }
    }
    setActiveCampaigns([]);
    setTradeHistory([]);
    setPopupAlerts([]);
    setActivePopup(null);
  };

  return (
    <main className="min-h-screen bg-[#0B0F12] text-slate-200 p-4 space-y-4 font-mono tabular-nums">
      {activePopup && (
        <div className="fixed top-4 right-4 z-50 max-w-md w-full bg-[#11161B] border-2 border-[#97CEB4] shadow-2xl p-4 space-y-2">
          <div className="flex items-center justify-between">
            <span className="text-xs font-bold text-[#97CEB4] uppercase tracking-wider">
              {activePopup.title}
            </span>
            <button
              type="button"
              onClick={() => setActivePopup(null)}
              className="text-xs text-slate-400 hover:text-white px-1.5 py-0.5 border border-[#1C242D]"
            >
              ✕
            </button>
          </div>
          <p className="text-xs text-slate-200 leading-relaxed">{activePopup.message}</p>
        </div>
      )}

      {selectedTradeModal && (
        <div className="fixed inset-0 z-50 bg-black/75 flex items-center justify-center p-4">
          <div className="bg-[#11161B] border border-[#97CEB4] max-w-lg w-full p-5 space-y-4">
            <div className="flex items-center justify-between border-b border-[#1C242D] pb-3">
              <div>
                <span className="text-xs text-[#97CEB4] block">
                  {selectedTradeModal.is_live_delta ? "OFFICIAL DELTA EXCHANGE LIVE TRADE RECEIPT" : "OFFICIAL V9.5 TRADE RECEIPT"}
                </span>
                <h3 className="text-sm font-bold text-white">
                  {selectedTradeModal.trade_id} // {selectedTradeModal.symbol} ({selectedTradeModal.side})
                </h3>
              </div>
              <button
                type="button"
                onClick={() => setSelectedTradeModal(null)}
                className="px-2.5 py-1 text-xs border border-[#1C242D] text-slate-300 hover:text-white"
              >
                CLOSE
              </button>
            </div>

            <div className="grid grid-cols-2 gap-3 text-xs">
              <div className="p-2.5 bg-[#0B0F12] border border-[#1C242D]">
                <span className="text-[10px] text-slate-400 block">ENTRY TIME</span>
                <strong className="text-slate-200">{selectedTradeModal.entry_time}</strong>
              </div>
              <div className="p-2.5 bg-[#0B0F12] border border-[#1C242D]">
                <span className="text-[10px] text-slate-400 block">EXIT TIME</span>
                <strong className="text-slate-200">{selectedTradeModal.exit_time}</strong>
              </div>
              <div className="p-2.5 bg-[#0B0F12] border border-[#1C242D]">
                <span className="text-[10px] text-slate-400 block">AVG ENTRY PRICE</span>
                <strong className="text-white">${selectedTradeModal.entry_price}</strong>
              </div>
              <div className="p-2.5 bg-[#0B0F12] border border-[#1C242D]">
                <span className="text-[10px] text-slate-400 block">EXIT PRICE</span>
                <strong className="text-white">${selectedTradeModal.exit_price}</strong>
              </div>
              <div className="p-2.5 bg-[#0B0F12] border border-[#1C242D]">
                <span className="text-[10px] text-slate-400 block">SETUP &amp; CONFLUENCE SCORE</span>
                <strong className="text-[#97CEB4]">
                  {selectedTradeModal.setup_type || "DUAL_CORE_V95"} ({selectedTradeModal.confluence_score || 98}/100)
                </strong>
              </div>
              <div className="p-2.5 bg-[#0B0F12] border border-[#1C242D]">
                <span className="text-[10px] text-slate-400 block">EXIT TRIGGER</span>
                <strong className="text-amber-300">{selectedTradeModal.exit_reason}</strong>
              </div>
              <div className="p-2.5 bg-[#0B0F12] border border-[#1C242D]">
                <span className="text-[10px] text-slate-400 block">NET PROFIT / LOSS (AFTER 18% GST)</span>
                <strong
                  className={
                    selectedTradeModal.realized_pnl_inr >= 0 ? "text-[#97CEB4]" : "text-[#FF0055]"
                  }
                >
                  {selectedTradeModal.realized_pnl_inr >= 0 ? "+" : ""}₹
                  {selectedTradeModal.realized_pnl_inr.toLocaleString("en-IN")} (
                  {selectedTradeModal.roi_pct >= 0 ? "+" : ""}
                  {selectedTradeModal.roi_pct}%)
                </strong>
              </div>
              <div className="p-2.5 bg-[#0B0F12] border border-[#1C242D]">
                <span className="text-[10px] text-slate-400 block">BALANCE AFTER TRADE</span>
                <strong className="text-white">
                  ₹{selectedTradeModal.balance_after_inr.toLocaleString("en-IN")}
                </strong>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* TOP SCOREBOARD */}
      <header className="bg-[#11161B] border border-[#1C242D] p-4 space-y-4">
        <div className="flex flex-wrap items-center justify-between gap-3 border-b border-[#1C242D] pb-3">
          <div className="flex flex-wrap items-center gap-2.5">
            <span className="w-2.5 h-2.5 rounded-full bg-[#97CEB4] animate-pulse" />
            <h1 className="text-sm sm:text-base font-bold tracking-widest text-white">
              APEX-OMEGA V9.5 // FEE-SHIELD &amp; 90S WICK-GUARD LIVE DELTA ENGINE
            </h1>
            <span className="px-2 py-0.5 text-[10px] bg-[#97CEB4]/15 border border-[#97CEB4]/40 text-[#97CEB4]">
              {feedBadge}
            </span>
            <span
              className={`px-2 py-0.5 text-[10px] font-bold border ${
                deltaConnected
                  ? "bg-emerald-500/15 border-emerald-400 text-emerald-300"
                  : "bg-amber-500/15 border-amber-400 text-amber-300"
              }`}
            >
              {deltaConnected
                ? `DELTA WALLET SYNCED (₹${(deltaWalletInr ?? baseWalletEquity).toLocaleString("en-IN")})`
                : deltaApiError
                ? `DELTA API: ${deltaApiError.slice(0, 48)}`
                : `MODE: ${executionMode}`}
            </span>
            {(browserDeltaWsLive || deltaWsConnected || backendWsLive) && (
              <span className="px-2 py-0.5 text-[10px] bg-emerald-500/10 border border-emerald-400/40 text-emerald-300">
                ⚡ 0ms MARK-PRICE LOCKED
              </span>
            )}
          </div>
          <div className="text-[11px] text-slate-400 flex flex-wrap items-center gap-3">
            <button
              type="button"
              onClick={handleForceSyncDelta}
              disabled={isSyncingDelta}
              className="px-2.5 py-1 text-[10px] font-bold bg-[#97CEB4] text-[#0B0F12] hover:bg-[#b0e2ca] disabled:opacity-50"
            >
              {isSyncingDelta ? "SYNCING DELTA..." : "🔄 INSTANT DELTA SYNC"}
            </button>
            <span>LAST TICK: <strong className="text-slate-200">{lastScanTime}</strong></span>
            <span>|</span>
            <span>
              STARTING CAPITAL:{" "}
              <strong className="text-white">
                ₹{startingEquity.toLocaleString("en-IN", { minimumFractionDigits: 2 })}
              </strong>
            </span>
          </div>
        </div>

        {/* 6 CORE METRICS GRID */}
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
          <div className="p-3 bg-[#0B0F12] border border-[#1C242D]">
            <div className="text-[10px] text-slate-400 uppercase">LIVE ACCOUNT BALANCE</div>
            <div className="text-base sm:text-lg font-bold text-[#97CEB4] mt-0.5">
              ₹{liveActiveEquity.toLocaleString("en-IN", { minimumFractionDigits: 2 })}
            </div>
            <div className="text-[10px] text-slate-400 mt-0.5">
              Avail Margin:{" "}
              <span className="text-slate-200">
                ₹{(deltaAvailableInr ?? baseWalletEquity).toLocaleString("en-IN")}
              </span>
              {deltaWalletUsd > 0 ? ` ($${deltaWalletUsd.toFixed(2)})` : ""}
            </div>
          </div>

          <div className="p-3 bg-[#0B0F12] border border-[#1C242D]">
            <div className="text-[10px] text-slate-400 uppercase">LIVE OPEN PNL (0ms WS)</div>
            <div
              className={`text-base sm:text-lg font-bold mt-0.5 ${
                unrealizedPnl >= 0 ? "text-[#97CEB4]" : "text-[#FF0055]"
              }`}
            >
              {unrealizedPnl >= 0 ? "+" : ""}₹{unrealizedPnl.toLocaleString("en-IN", { minimumFractionDigits: 2 })}
            </div>
            <div className="text-[10px] text-slate-400 mt-0.5">
              Locked Margin: ₹{deltaPositionMarginInr.toLocaleString("en-IN")}
            </div>
          </div>

          <div className="p-3 bg-[#0B0F12] border border-[#1C242D]">
            <div className="text-[10px] text-slate-400 uppercase">NET REALIZED PNL (AFTER GST)</div>
            <div
              className={`text-base sm:text-lg font-bold mt-0.5 ${
                netRealizedPnl >= 0 ? "text-[#97CEB4]" : "text-[#FF0055]"
              }`}
            >
              {netRealizedPnl >= 0 ? "+" : ""}₹{netRealizedPnl.toLocaleString("en-IN")}
            </div>
            <div className="text-[10px] text-slate-400 mt-0.5">
              Total Return: {totalReturnPct >= 0 ? "+" : ""}{totalReturnPct}%
            </div>
          </div>

          <div className="p-3 bg-[#0B0F12] border border-[#1C242D]">
            <div className="text-[10px] text-slate-400 uppercase">WIN RATE % (FEE-CLEARED)</div>
            <div className="text-base sm:text-lg font-bold text-white mt-0.5">
              {winRatePct.toFixed(1)}%
            </div>
            <div className="text-[10px] text-slate-400 mt-0.5">
              {winsCount}W / {lossesCount}L ({totalClosed} Closed | {deltaFills.length} Fills)
            </div>
          </div>

          <div className="p-3 bg-[#0B0F12] border border-[#1C242D]">
            <div className="text-[10px] text-slate-400 uppercase">COMPOUNDING POWER (6.5X)</div>
            <div className="text-base sm:text-lg font-bold text-amber-300 mt-0.5">
              ₹{Math.round(liveActiveEquity * 6.5).toLocaleString("en-IN")}
            </div>
            <div className="text-[10px] text-slate-400 mt-0.5">
              Protected Floor: ₹{protectedFloor.toLocaleString("en-IN")}
            </div>
          </div>

          <div className="p-3 bg-[#0B0F12] border border-[#1C242D]">
            <div className="text-[10px] text-slate-400 uppercase">LIVE OPEN TRADES (MAX 3)</div>
            <div className="text-sm font-bold text-[#97CEB4] mt-1 truncate">
              {activeCampaigns.length > 0
                ? `${activeCampaigns.length}/3 OPEN (${activeCampaigns.map((c) => c.symbol.split("/")[0]).join(", ")})`
                : deltaOpenPositions.length > 0
                ? `${deltaOpenPositions.length} DELTA POS OPEN`
                : "0/3 OPEN (SCANNING)"}
            </div>
            <div className="text-[10px] text-slate-400 mt-0.5">
              {activeCampaigns.length > 0
                ? `${activeCampaigns.filter((c) => c.breakeven_locked).length} Fee-Cleared Shield 🔒`
                : "+0.16% Min Lock (>0.118% Fee)"}
            </div>
          </div>
        </div>

        {/* NAVIGATION TABS */}
        <div className="flex flex-wrap items-center justify-between gap-2 pt-2 border-t border-[#1C242D]">
          <div className="flex flex-wrap items-center gap-2">
            <button
              type="button"
              onClick={() => setActiveTab("monitor")}
              className={`px-3 py-1.5 text-xs font-bold border transition-colors ${
                activeTab === "monitor"
                  ? "bg-[#97CEB4] text-[#0B0F12] border-[#97CEB4]"
                  : "bg-[#0B0F12] text-slate-300 border-[#1C242D] hover:border-[#97CEB4]"
              }`}
            >
              01 // LIVE MARKET &amp; OPEN POSITIONS ({activeCampaigns.length})
            </button>
            <button
              type="button"
              onClick={() => setActiveTab("trades")}
              className={`px-3 py-1.5 text-xs font-bold border transition-colors flex items-center gap-2 ${
                activeTab === "trades"
                  ? "bg-[#97CEB4] text-[#0B0F12] border-[#97CEB4]"
                  : "bg-[#0B0F12] text-slate-300 border-[#1C242D] hover:border-[#97CEB4]"
              }`}
            >
              <span>02 // LIVE TRADE HISTORY &amp; DELTA FILLS</span>
              <span className="px-1.5 py-0.2 text-[10px] bg-black/30 rounded">
                {totalClosed} / {deltaFills.length}
              </span>
            </button>
            <button
              type="button"
              onClick={() => setActiveTab("cloud")}
              className={`px-3 py-1.5 text-xs font-bold border transition-colors ${
                activeTab === "cloud"
                  ? "bg-[#97CEB4] text-[#0B0F12] border-[#97CEB4]"
                  : "bg-[#0B0F12] text-slate-300 border-[#1C242D] hover:border-[#97CEB4]"
              }`}
            >
              03 // DELTA EXCHANGE &amp; RENDER DIAGNOSTICS
            </button>
          </div>

          {(activeCampaigns.length > 0 || deltaOpenPositions.length > 0) && (
            <button
              type="button"
              onClick={handleEmergencyFlattenAll}
              className="px-3 py-1.5 text-xs font-bold bg-[#FF0055] text-white hover:bg-[#ff246d]"
            >
              🚨 EMERGENCY FLATTEN ALL LIVE TRADES ({Math.max(activeCampaigns.length, deltaOpenPositions.length)})
            </button>
          )}
        </div>
      </header>

      {/* TAB 1: LIVE MARKET & OPEN POSITIONS */}
      {activeTab === "monitor" && (
        <div className="space-y-4">
          <section className="bg-[#11161B] border border-[#1C242D] p-4 space-y-3">
            <div className="flex flex-wrap items-center justify-between gap-2 pb-2 border-b border-[#1C242D]">
              <span className="text-xs font-bold uppercase tracking-wider text-[#97CEB4]">
                LIVE OPEN POSITIONS &amp; REAL-TIME 0MS OPEN PNL ({activeCampaigns.length}/3 SLOTS)
              </span>
              <span className="text-xs text-slate-400">
                Total Open PnL:{" "}
                <strong className={unrealizedPnl >= 0 ? "text-[#97CEB4]" : "text-[#FF0055]"}>
                  {unrealizedPnl >= 0 ? "+" : ""}₹{unrealizedPnl.toLocaleString("en-IN", { minimumFractionDigits: 2 })}
                </strong>
              </span>
            </div>

            {activeCampaigns.length > 0 ? (
              <div className="space-y-4">
                {activeCampaigns.map((camp) => {
                  const campPnl = +(
                    (camp.total_unrealized_pnl_inr || 0) + (camp.tp1_banked_pnl_inr || 0)
                  ).toFixed(2);
                  const estRoundTripFeeWithGst = +((camp.total_notional_inr || 0) * DELTA_FEE_WITH_GST_RATE * 2).toFixed(2);
                  const netAfterFee = +(campPnl - estRoundTripFeeWithGst).toFixed(2);
                  return (
                    <div
                      key={camp.trade_id}
                      className="p-3 bg-[#0B0F12] border border-[#97CEB4]/50 space-y-3"
                    >
                      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-[#1C242D] pb-2 text-xs">
                        <div className="flex items-center gap-2">
                          <span className="font-bold text-white text-sm">{camp.symbol}</span>
                          {camp.delta_symbol && (
                            <span className="px-1.5 py-0.5 text-[10px] bg-[#97CEB4]/15 text-[#97CEB4] border border-[#97CEB4]/40">
                              DELTA: {camp.delta_symbol}
                            </span>
                          )}
                          <span
                            className={`px-2 py-0.5 text-[10px] font-bold ${
                              camp.side === "LONG"
                                ? "bg-[#97CEB4] text-[#0B0F12]"
                                : "bg-[#FF0055] text-white"
                            }`}
                          >
                            {camp.side}
                          </span>
                          <span className="text-slate-400 text-[11px]">
                            Stage: <strong className="text-[#97CEB4]">{camp.max_stage_reached}</strong>
                          </span>
                        </div>
                        <div className="text-[11px] text-slate-400">
                          Notional:{" "}
                          <strong className="text-white">
                            ₹{camp.total_notional_inr.toLocaleString("en-IN")}
                          </strong>
                          {camp.live_qty ? ` (${camp.live_qty} Contracts)` : ""}
                        </div>
                      </div>

                      <div className="grid grid-cols-2 sm:grid-cols-6 gap-3 text-xs">
                        <div className="p-2.5 bg-[#11161B] border border-[#1C242D]">
                          <span className="text-[10px] text-slate-400 block">EXACT ENTRY PRICE</span>
                          <strong className="text-slate-200">
                            ${camp.entry_price_raw ? fmtPrice(camp.entry_price_raw) : camp.base_entry_price}
                          </strong>
                        </div>
                        <div className="p-2.5 bg-[#11161B] border border-[#1C242D]">
                          <span className="text-[10px] text-slate-400 block">DELTA MARK PRICE (0ms)</span>
                          <strong className="text-[#97CEB4]">${camp.current_price}</strong>
                        </div>
                        <div className="p-2.5 bg-[#11161B] border border-[#1C242D]">
                          <span className="text-[10px] text-slate-400 block">STAGE-2 BANK (+0.88%)</span>
                          <strong className="text-[#97CEB4]">
                            ${camp.tp1_price} {camp.tp1_hit ? "(50% BANKED ✅)" : "(50% Cash Lock)"}
                          </strong>
                        </div>
                        <div className="p-2.5 bg-[#11161B] border border-[#1C242D]">
                          <span className="text-[10px] text-slate-400 block">HARDWARE STOP</span>
                          <strong className="text-amber-300">
                            ${camp.effective_stop}{" "}
                            {camp.breakeven_locked ? "(FEE-CLEARED LOCK 🔒)" : "(90s Wick Shield)"}
                          </strong>
                        </div>
                        <div className="p-2.5 bg-[#11161B] border border-[#1C242D]">
                          <span className="text-[10px] text-slate-400 block">LIQUIDATION / MARGIN</span>
                          <strong className="text-slate-300">
                            {camp.liquidation_price ? `$${camp.liquidation_price}` : "Isolated 8x"}
                            {camp.margin_inr ? ` (₹${camp.margin_inr})` : ""}
                          </strong>
                        </div>
                        <div className="p-2.5 bg-[#11161B] border border-[#1C242D]">
                          <span className="text-[10px] text-slate-400 block">LIVE OPEN PNL (MARK)</span>
                          <strong className={campPnl >= 0 ? "text-[#97CEB4]" : "text-[#FF0055]"}>
                            {campPnl >= 0 ? "+" : ""}₹{campPnl.toLocaleString("en-IN", { minimumFractionDigits: 2 })}
                          </strong>
                          <span className="text-[9px] text-slate-500 block mt-0.5">
                            Net of Fee+GST: {netAfterFee >= 0 ? "+" : ""}₹{netAfterFee.toLocaleString("en-IN")}
                          </span>
                        </div>
                      </div>
                    </div>
                  );
                })}
              </div>
            ) : (
              <div className="p-6 bg-[#0B0F12] border border-[#1C242D] text-center space-y-1">
                <div className="text-xs font-bold text-slate-300">
                  NO OPEN POSITIONS ON DELTA EXCHANGE — V9.5 FEE-SHIELD &amp; 90S WICK-GUARD ACTIVE
                </div>
                <p className="text-[11px] text-slate-400">
                  Prioritizes High-Beta Alpha Pairs (LINK, DOGE, SOL, AVAX, ADA), blocks low-KER chop, places resting Maker Limit TP at +1.85%, and only locks stops above the 0.118% fee+GST hurdle (+0.16% / +0.34% / +0.60%).
                </p>
              </div>
            )}
          </section>

          {/* 10-PAIR REAL-TIME MULTI-EXCHANGE SCANNER */}
          <section className="bg-[#11161B] border border-[#1C242D] p-4">
            <div className="flex flex-wrap items-center justify-between gap-2 pb-2 mb-3 border-b border-[#1C242D]">
              <span className="text-xs font-bold uppercase tracking-wider text-slate-200">
                10-MARKET REAL-TIME 0MS DELTA WEBSOCKET &amp; V9.5 FEE-SHIELD SCANNER
              </span>
              <span className="text-[11px] text-[#97CEB4]">
                Live Stream: wss://socket.india.delta.exchange (v2/ticker Mark Price)
              </span>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs">
                <thead>
                  <tr className="text-slate-400 border-b border-[#1C242D]">
                    <th className="py-2 px-2">PAIR</th>
                    <th className="py-2 px-2 text-right">DELTA MARK PRICE</th>
                    <th className="py-2 px-2 text-right">VWAP Z-SCORE</th>
                    <th className="py-2 px-2 text-right">RSI(2) SNIPER</th>
                    <th className="py-2 px-2 text-center">SQUEEZE COIL</th>
                    <th className="py-2 px-2">RVOL METER</th>
                    <th className="py-2 px-2 text-center">QUANT SCORE</th>
                    <th className="py-2 px-2 text-right">SETUP REGIME</th>
                  </tr>
                </thead>
                <tbody>
                  {scanner.map((a) => {
                    const pDec = a.price < 0.001 ? 8 : a.price < 1 ? 5 : a.price < 50 ? 4 : 2;
                    const rvolPct = Math.min(100, Math.round((a.rvol / 2.8) * 100));
                    return (
                      <tr key={a.symbol} className="border-b border-[#1C242D] hover:bg-[#141B22]">
                        <td className="py-2.5 px-2 font-bold text-white">
                          <div>{a.symbol}</div>
                          <div className="text-[9px] text-slate-500 font-normal">{a.feed_source}</div>
                        </td>
                        <td className="py-2.5 px-2 text-right font-bold text-white">
                          ${a.price.toFixed(pDec)}
                        </td>
                        <td
                          className={`py-2.5 px-2 text-right ${
                            a.z_score <= -1.0
                              ? "text-[#97CEB4] font-bold"
                              : a.z_score >= 1.0
                              ? "text-[#FF0055] font-bold"
                              : "text-slate-300"
                          }`}
                        >
                          {a.z_score >= 0 ? "+" : ""}{a.z_score.toFixed(2)}σ
                        </td>
                        <td
                          className={`py-2.5 px-2 text-right ${
                            a.rsi_2 <= 22
                              ? "text-[#97CEB4] font-bold"
                              : a.rsi_2 >= 78
                              ? "text-[#FF0055] font-bold"
                              : "text-slate-300"
                          }`}
                        >
                          {a.rsi_2.toFixed(1)}
                        </td>
                        <td className="py-2.5 px-2 text-center">
                          <span
                            className={`px-2 py-0.5 text-[10px] border ${
                              a.squeeze_on
                                ? "border-[#97CEB4] text-[#97CEB4] bg-[#97CEB4]/10"
                                : "border-[#1C242D] text-slate-400"
                            }`}
                          >
                            {a.squeeze_on ? `COILED (${a.squeeze_bars_last_10}/10)` : `${a.compression_ratio.toFixed(2)}x`}
                          </span>
                        </td>
                        <td className="py-2.5 px-2 w-36">
                          <div className="flex items-center gap-2">
                            <div className="w-16 h-2 bg-[#0B0F12] border border-[#1C242D]">
                              <div
                                className={`h-full ${a.rvol >= 1.12 ? "bg-[#97CEB4]" : "bg-slate-600"}`}
                                style={{ width: `${rvolPct}%` }}
                              />
                            </div>
                            <span className={a.rvol >= 1.12 ? "text-[#97CEB4] font-bold" : "text-slate-400"}>
                              {a.rvol.toFixed(2)}x
                            </span>
                          </div>
                        </td>
                        <td className="py-2.5 px-2 text-center">
                          <span
                            className={`px-2 py-0.5 text-[10px] font-bold ${
                              a.confluence_score >= 96
                                ? "bg-[#97CEB4] text-[#0B0F12]"
                                : a.confluence_score >= 80
                                ? "border border-[#97CEB4]/50 text-[#97CEB4]"
                                : "text-slate-400"
                            }`}
                          >
                            {a.confluence_score}/100
                          </span>
                        </td>
                        <td className="py-2.5 px-2 text-right">
                          <span
                            className={`px-2 py-0.5 text-[10px] font-bold ${
                              a.signal_valid
                                ? "bg-[#97CEB4] text-[#0B0F12]"
                                : "border border-[#1C242D] text-slate-400"
                            }`}
                          >
                            {a.signal_valid ? `${a.regime} (${a.setup_type})` : a.regime}
                          </span>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </section>
        </div>
      )}

      {/* TAB 2: DEDICATED LIVE TRADE HISTORY & DELTA EXCHANGE FILLS LEDGER */}
      {activeTab === "trades" && (
        <section className="bg-[#11161B] border border-[#1C242D] p-4 space-y-4">
          <div className="flex flex-wrap items-center justify-between gap-3 pb-3 border-b border-[#1C242D]">
            <div>
              <h2 className="text-xs font-bold uppercase tracking-wider text-[#97CEB4]">
                02 // LIVE TRADE ENTRY, EXIT &amp; DELTA EXCHANGE FILLS LEDGER (INCLUDES 18% GST)
              </h2>
              <p className="text-[11px] text-slate-400 mt-0.5">
                Synced in real time with Delta Exchange India (/v2/fills + /v2/wallet/transactions)
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-2 text-xs">
              <button
                type="button"
                onClick={() => setTradeSubTab("closed")}
                className={`px-2.5 py-1 border ${
                  tradeSubTab === "closed"
                    ? "border-[#97CEB4] bg-[#97CEB4] text-[#0B0F12] font-bold"
                    : "border-[#1C242D] bg-[#0B0F12] text-slate-300"
                }`}
              >
                CLOSED ROUND-TRIP TRADES ({totalClosed})
              </button>
              <button
                type="button"
                onClick={() => setTradeSubTab("fills")}
                className={`px-2.5 py-1 border ${
                  tradeSubTab === "fills"
                    ? "border-[#97CEB4] bg-[#97CEB4] text-[#0B0F12] font-bold"
                    : "border-[#1C242D] bg-[#0B0F12] text-slate-300"
                }`}
              >
                RAW DELTA EXCHANGE FILLS ({deltaFills.length})
              </button>
            </div>
          </div>

          <div className="grid grid-cols-2 sm:grid-cols-5 gap-3 text-xs">
            <div className="p-3 bg-[#0B0F12] border border-[#1C242D]">
              <span className="text-[10px] text-slate-400 block">WIN RATE %</span>
              <strong className="text-base text-white">{winRatePct.toFixed(1)}%</strong>
            </div>
            <div className="p-3 bg-[#0B0F12] border border-[#1C242D]">
              <span className="text-[10px] text-slate-400 block">WINS / LOSSES</span>
              <strong className="text-base text-[#97CEB4]">
                {winsCount}W <span className="text-slate-500">/</span>{" "}
                <span className="text-[#FF0055]">{lossesCount}L</span>
              </strong>
            </div>
            <div className="p-3 bg-[#0B0F12] border border-[#1C242D]">
              <span className="text-[10px] text-slate-400 block">GROSS PROFIT</span>
              <strong className="text-base text-[#97CEB4]">+₹{grossProfit.toLocaleString("en-IN")}</strong>
            </div>
            <div className="p-3 bg-[#0B0F12] border border-[#1C242D]">
              <span className="text-[10px] text-slate-400 block">GROSS LOSS</span>
              <strong className="text-base text-[#FF0055]">-₹{grossLoss.toLocaleString("en-IN")}</strong>
            </div>
            <div className="p-3 bg-[#0B0F12] border border-[#1C242D]">
              <span className="text-[10px] text-slate-400 block">NET REALIZED PNL</span>
              <strong className={`text-base ${netRealizedPnl >= 0 ? "text-[#97CEB4]" : "text-[#FF0055]"}`}>
                {netRealizedPnl >= 0 ? "+" : ""}₹{netRealizedPnl.toLocaleString("en-IN")}
              </strong>
            </div>
          </div>

          {tradeSubTab === "closed" ? (
            <>
              <div className="flex items-center gap-2 text-xs">
                {(["ALL", "WIN", "LOSS"] as const).map((f) => (
                  <button
                    key={f}
                    type="button"
                    onClick={() => setTradeFilter(f)}
                    className={`px-2.5 py-1 border ${
                      tradeFilter === f
                        ? "border-[#97CEB4] bg-[#97CEB4]/15 text-[#97CEB4] font-bold"
                        : "border-[#1C242D] bg-[#0B0F12] text-slate-400"
                    }`}
                  >
                    {f === "ALL" ? `ALL (${totalClosed})` : f === "WIN" ? `WINS (${winsCount})` : `LOSSES (${lossesCount})`}
                  </button>
                ))}
              </div>

              <div className="overflow-x-auto">
                <table className="w-full text-left text-xs">
                  <thead>
                    <tr className="text-slate-400 border-b border-[#1C242D]">
                      <th className="py-2 px-2">TRADE ID</th>
                      <th className="py-2 px-2">PAIR &amp; SIDE</th>
                      <th className="py-2 px-2">ENTRY TIME &amp; PRICE</th>
                      <th className="py-2 px-2 text-center">STAGE &amp; SOURCE</th>
                      <th className="py-2 px-2">EXIT TIME &amp; PRICE</th>
                      <th className="py-2 px-2">EXIT REASON</th>
                      <th className="py-2 px-2 text-right">NET PNL (AFTER GST)</th>
                      <th className="py-2 px-2 text-right">NEW BALANCE</th>
                      <th className="py-2 px-2 text-right">RECEIPT</th>
                    </tr>
                  </thead>
                  <tbody>
                    {filteredTrades.length === 0 ? (
                      <tr>
                        <td colSpan={9} className="py-8 text-center text-slate-400">
                          No closed trades recorded yet. Click &quot;🔄 INSTANT DELTA SYNC&quot; to pull live Delta Exchange fills or wait for the next V9.5 trade execution.
                        </td>
                      </tr>
                    ) : (
                      filteredTrades.map((t) => (
                        <tr key={t.trade_id} className="border-b border-[#1C242D] hover:bg-[#141B22]">
                          <td className="py-2.5 px-2 font-bold text-slate-300">{t.trade_id}</td>
                          <td className="py-2.5 px-2 font-bold text-white">
                            {t.symbol}{" "}
                            <span
                              className={`px-1.5 py-0.5 text-[10px] ${
                                t.side === "LONG"
                                  ? "bg-[#97CEB4]/15 text-[#97CEB4]"
                                  : "bg-[#FF0055]/15 text-[#FF0055]"
                              }`}
                            >
                              {t.side}
                            </span>
                          </td>
                          <td className="py-2.5 px-2">
                            <div className="text-white">${t.entry_price}</div>
                            <div className="text-[10px] text-slate-400">{t.entry_time}</div>
                          </td>
                          <td className="py-2.5 px-2 text-center">
                            <span className="px-2 py-0.5 text-[10px] border border-[#97CEB4]/40 text-[#97CEB4]">
                              {t.max_stage_reached} ({t.confluence_score || 98}/100)
                            </span>
                          </td>
                          <td className="py-2.5 px-2">
                            <div className="text-white">${t.exit_price}</div>
                            <div className="text-[10px] text-slate-400">{t.exit_time}</div>
                          </td>
                          <td className="py-2.5 px-2 text-slate-300">{t.exit_reason}</td>
                          <td
                            className={`py-2.5 px-2 text-right font-bold ${
                              t.realized_pnl_inr >= 0 ? "text-[#97CEB4]" : "text-[#FF0055]"
                            }`}
                          >
                            {t.realized_pnl_inr >= 0 ? "+" : ""}₹{t.realized_pnl_inr.toLocaleString("en-IN")} (
                            {t.roi_pct >= 0 ? "+" : ""}
                            {t.roi_pct}%)
                          </td>
                          <td className="py-2.5 px-2 text-right font-bold text-white">
                            ₹{t.balance_after_inr.toLocaleString("en-IN")}
                          </td>
                          <td className="py-2.5 px-2 text-right">
                            <button
                              type="button"
                              onClick={() => setSelectedTradeModal(t)}
                              className="text-[#97CEB4] underline hover:text-white"
                            >
                              Receipt ↗
                            </button>
                          </td>
                        </tr>
                      ))
                    )}
                  </tbody>
                </table>
              </div>
            </>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-left text-xs">
                <thead>
                  <tr className="text-slate-400 border-b border-[#1C242D]">
                    <th className="py-2 px-2">FILL ID</th>
                    <th className="py-2 px-2">DELTA CONTRACT</th>
                    <th className="py-2 px-2">SIDE</th>
                    <th className="py-2 px-2 text-right">CONTRACTS</th>
                    <th className="py-2 px-2 text-right">FILL PRICE</th>
                    <th className="py-2 px-2 text-right">NOTIONAL (₹)</th>
                    <th className="py-2 px-2 text-right">FEE + 18% GST (₹)</th>
                    <th className="py-2 px-2 text-center">ROLE</th>
                    <th className="py-2 px-2 text-right">TIMESTAMP</th>
                  </tr>
                </thead>
                <tbody>
                  {deltaFills.length === 0 ? (
                    <tr>
                      <td colSpan={9} className="py-8 text-center text-slate-400">
                        No raw fills returned from Delta Exchange (/v2/fills) yet. Ensure your Render Outbound IP ({serverOutboundIp}) is whitelisted in Delta Exchange API Management.
                      </td>
                    </tr>
                  ) : (
                    deltaFills.map((f) => (
                      <tr key={f.fill_id} className="border-b border-[#1C242D] hover:bg-[#141B22]">
                        <td className="py-2.5 px-2 font-bold text-slate-300">{f.fill_id}</td>
                        <td className="py-2.5 px-2 font-bold text-white">
                          {f.delta_symbol} <span className="text-slate-500 text-[10px]">({f.symbol})</span>
                        </td>
                        <td className="py-2.5 px-2">
                          <span
                            className={`px-1.5 py-0.5 text-[10px] font-bold ${
                              f.side === "BUY"
                                ? "bg-[#97CEB4]/15 text-[#97CEB4]"
                                : "bg-[#FF0055]/15 text-[#FF0055]"
                            }`}
                          >
                            {f.side}
                          </span>
                        </td>
                        <td className="py-2.5 px-2 text-right text-white">{f.contracts}</td>
                        <td className="py-2.5 px-2 text-right text-white">${f.price}</td>
                        <td className="py-2.5 px-2 text-right text-slate-200">
                          ₹{f.notional_inr.toLocaleString("en-IN")}
                        </td>
                        <td className="py-2.5 px-2 text-right text-amber-300">
                          ₹{f.fee_inr.toLocaleString("en-IN")}
                        </td>
                        <td className="py-2.5 px-2 text-center text-slate-400">{f.role}</td>
                        <td className="py-2.5 px-2 text-right text-slate-400">{f.timestamp}</td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          )}
        </section>
      )}

      {/* TAB 3: DELTA EXCHANGE & RENDER CLOUD DIAGNOSTICS */}
      {activeTab === "cloud" && (
        <section className="bg-[#11161B] border border-[#1C242D] p-4 space-y-4">
          <div className="border-b border-[#1C242D] pb-3">
            <h2 className="text-xs font-bold uppercase tracking-wider text-[#97CEB4]">
              03 // LIVE DELTA EXCHANGE INDIA &amp; ZERO-LAG WEBSOCKET DIAGNOSTICS
            </h2>
            <p className="text-[11px] text-slate-400 mt-0.5">
              Verify live Delta Exchange India API + WebSocket connection, Render server outbound IP for whitelisting, and 24/7 keep-alive sync.
            </p>
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-3 gap-4 text-xs">
            <div className="p-4 bg-[#0B0F12] border border-[#1C242D] space-y-2">
              <div className="font-bold text-white">1. DELTA EXCHANGE INDIA LIVE STATUS</div>
              <div className="text-[11px] space-y-1">
                <div>
                  REST Account Sync:{" "}
                  <strong className={deltaConnected ? "text-[#97CEB4]" : "text-[#FF0055]"}>
                    {deltaConnected ? "CONNECTED (https://api.india.delta.exchange)" : "DISCONNECTED / CHECK IP WHITELIST"}
                  </strong>
                </div>
                <div>
                  0ms WebSocket Stream:{" "}
                  <strong className={browserDeltaWsLive || deltaWsConnected ? "text-[#97CEB4]" : "text-amber-300"}>
                    {browserDeltaWsLive || deltaWsConnected ? "ACTIVE (wss://socket.india.delta.exchange)" : "CONNECTING..."}
                  </strong>
                </div>
                <div>
                  Render Server Outbound IP:{" "}
                  <strong className="text-amber-300 select-all">{serverOutboundIp}</strong>
                </div>
                <div>
                  Live Wallet Equity:{" "}
                  <strong className="text-white">
                    {deltaWalletInr !== null ? `₹${deltaWalletInr.toLocaleString("en-IN")}` : "Syncing..."}
                  </strong>
                </div>
                <div>
                  Last Delta Sync:{" "}
                  <strong className="text-slate-300">{deltaLastSyncTs || "Pending"}</strong>
                </div>
                {deltaApiError && (
                  <div className="p-2 mt-2 bg-[#FF0055]/10 border border-[#FF0055] text-[#FF0055] text-[10px]">
                    Delta Error: {deltaApiError} — Add IP <strong>{serverOutboundIp}</strong> to Delta Exchange &gt; API Management Whitelist.
                  </div>
                )}
              </div>
            </div>

            <div className="p-4 bg-[#0B0F12] border border-[#1C242D] space-y-3">
              <div className="font-bold text-white">2. LINK YOUR RENDER BACKEND URL</div>
              <p className="text-slate-400 text-[11px]">
                Paste your Render service URL below:
              </p>
              <div className="flex gap-2">
                <input
                  type="text"
                  value={renderUrlInput}
                  onChange={(e) => setRenderUrlInput(e.target.value)}
                  placeholder="https://apex-omega-v9-original-live.onrender.com"
                  className="flex-1 bg-[#11161B] border border-[#1C242D] px-3 py-2 text-xs text-white focus:outline-none focus:border-[#97CEB4]"
                />
                <button
                  type="button"
                  onClick={handleSaveRenderUrl}
                  className="px-4 py-2 font-bold bg-[#97CEB4] text-[#0B0F12] hover:bg-[#b0e2ca]"
                >
                  SAVE &amp; SYNC
                </button>
              </div>
              <div className="text-[11px] text-slate-400">
                Active Backend: <strong className="text-[#97CEB4]">{renderUrl}</strong>
              </div>
            </div>

            <div className="p-4 bg-[#0B0F12] border border-[#1C242D] space-y-3">
              <div className="font-bold text-white">3. UPTIMEROBOT 5-MINUTE KEEP-ALIVE URL</div>
              <p className="text-slate-400 text-[11px]">
                Point UptimeRobot HTTP(s) Monitor (5m interval) to:
              </p>
              <div className="p-2.5 bg-[#11161B] border border-[#1C242D] text-[#97CEB4] select-all">
                {renderUrl ? `${renderUrl}/health` : "https://apex-omega-v9-original-live.onrender.com/health"}
              </div>
              <div className="flex items-center justify-between pt-2">
                <span className="text-[11px] text-slate-400">Re-sync / Reset Local Cache:</span>
                <button
                  type="button"
                  onClick={handleResetPaperAccount}
                  className="px-3 py-1 text-[11px] border border-[#FF0055]/60 text-[#FF0055] hover:bg-[#FF0055] hover:text-white"
                >
                  Reset &amp; Re-Sync Live Account
                </button>
              </div>
            </div>
          </div>
        </section>
      )}
    </main>
  );
}
