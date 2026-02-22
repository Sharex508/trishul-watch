import threading
import time
import logging
import os
import hmac
import hashlib
import urllib.parse
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Optional, List, Dict

import requests

from .coin_price_monitor import get_database_connection, get_all_coins
from .db_schema import ensure_trading_tables, ensure_intraday_tables, seed_portfolio, is_pg


class TradingManager:
    """
    Simple paper-trading manager that operates on live price data stored by the
    existing price monitor. When enabled, it checks prices periodically and
    emits paper trades based on a trivial demo strategy for clarity.

    Strategy (safer paper-trading):
    - Maintain a paper portfolio with a USDT cash balance.
    - Only open positions when the AI ensemble emits a BUY decision above a
      confidence threshold and risk/portfolio caps allow it.
    - Position size is limited by `max_notional_per_trade` and available cash.
    - Manage exits with stop-loss and take-profit, plus optional AI SELL signal.

    Trades are recorded in `trade_logs` with realized PnL, and open positions are
    tracked in `paper_positions`.
    """

    def __init__(self, interval_sec: float = 5.0, cooldown_sec: float = 60.0):
        self.interval_sec = interval_sec
        self.cooldown_sec = cooldown_sec
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._enabled: bool = False
        self.strategy_mode: str = "idle"  # idle | hybrid | coin | intraday | ai
        self.hybrid_enabled: bool = True
        # symbol -> last trade timestamp to prevent spamming
        self._last_trade_ts: Dict[str, float] = {}
        # Portfolio defaults (not actively used in simple mode)
        self.initial_cash = float(os.getenv("PAPER_INITIAL_CASH", "1000"))
        self.risk_perc_default = float(os.getenv("PAPER_RISK_PERC", "1.0"))
        self.rr_target_default = float(os.getenv("PAPER_RR_TARGET", "2.0"))
        self.atr_mult = float(os.getenv("PAPER_ATR_MULT", "1.5"))
        self.buffer_pct = float(os.getenv("PAPER_BUFFER_PCT", "0.001"))
        self.max_notional_per_trade = float(os.getenv("PAPER_MAX_NOTIONAL", "1000"))
        self.atr_trail_mult = float(os.getenv("PAPER_ATR_TRAIL_MULT", "1.0"))
        self.daily_loss_cap_pct = float(os.getenv("PAPER_DAILY_LOSS_CAP_PCT", "5.0"))
        # AI hybrid settings
        self.min_confidence = float(os.getenv("PAPER_MIN_CONFIDENCE", "0.6"))
        self.ai_decision_max_age_sec = int(os.getenv("AI_DECISION_MAX_AGE_SEC", "180"))
        # Coin Brain settings
        self.coin_brain_thread: Optional[threading.Thread] = None
        self.coin_brain_stop = threading.Event()
        self.coin_brain_symbol: Optional[str] = None
        self.coin_brain_paper: bool = True
        self.intraday_paper: bool = True
        self.paper_trading: bool = True
        self.coin_brain_buy_dip_pct = float(os.getenv("COIN_BRAIN_BUY_DIP_PCT", "0.005"))  # 0.5% dip triggers buy
        self.coin_brain_tp_pct = float(os.getenv("COIN_BRAIN_TP_PCT", "0.005"))  # 0.5% take-profit
        self.coin_brain_sl_pct = float(os.getenv("COIN_BRAIN_SL_PCT", "0.003"))  # 0.3% stop-loss
        self.coin_brain_reentry_pct = float(os.getenv("COIN_BRAIN_REENTRY_PCT", "0.01"))  # +1% after stop to re-enter
        self.coin_brain_reentry_bars = int(os.getenv("COIN_BRAIN_REENTRY_BARS", "2"))  # within N loop intervals
        self.coin_brain_vol_min_mult = float(os.getenv("COIN_BRAIN_VOL_MIN_MULT", "0.8"))  # require >=80% of avg20
        self.coin_brain_vol_surge_mult = float(os.getenv("COIN_BRAIN_VOL_SURGE_MULT", "1.2"))  # treat >=120% as surge
        self._last_stop_info: Dict[str, dict] = {}  # symbol -> {ts, price}
        # Laddering config (averaging in with capped slices)
        self.coin_brain_ladder_enabled = os.getenv("COIN_BRAIN_LADDER_ENABLED", "true").lower() == "true"
        self.coin_brain_ladder_slices = int(os.getenv("COIN_BRAIN_LADDER_SLICES", "10"))
        self.coin_brain_ladder_slice_usd = float(os.getenv("COIN_BRAIN_LADDER_SLICE_USD", "100"))
        self.coin_brain_ladder_step_pct = float(os.getenv("COIN_BRAIN_LADDER_STEP_PCT", "0.003"))  # 0.3% per extra fill
        self.coin_brain_ladder_tp_pct = float(os.getenv("COIN_BRAIN_LADDER_TP_PCT", "0.01"))  # 1% TP over blended avg
        self.coin_brain_ladder_stop_pct = float(os.getenv("COIN_BRAIN_LADDER_STOP_PCT", "0.02"))  # 2% blended stop
        self.coin_brain_ladder_total_cap = float(os.getenv("COIN_BRAIN_LADDER_TOTAL_CAP", str(self.initial_cash)))
        self._ladder_state: Dict[str, dict] = {}  # symbol -> {fills:int, last_fill_price:float}
        # Over-trading controls
        self.coin_brain_max_trades_per_hour = int(os.getenv("COIN_BRAIN_MAX_TRADES_PER_HOUR", "30"))
        self.coin_brain_cooldown_sec = int(os.getenv("COIN_BRAIN_COOLDOWN_SEC", "60"))
        self._coin_brain_trades: list = []  # timestamps of recent trades
        self._last_exit_ts: Dict[str, float] = {}
        # Intraday mode settings
        self.intraday_thread: Optional[threading.Thread] = None
        self._intraday_stop = threading.Event()
        self.intraday_enabled: bool = False
        self.intraday_loop_sec = float(os.getenv("INTRADAY_LOOP_SEC", "10"))
        self.intraday_margin3_pct = float(os.getenv("INTRADAY_MARGIN3_PCT", "0.5"))
        self.intraday_margin5_pct = float(os.getenv("INTRADAY_MARGIN5_PCT", "5"))
        self.intraday_margin10_pct = float(os.getenv("INTRADAY_MARGIN10_PCT", "10"))
        self.intraday_margin20_pct = float(os.getenv("INTRADAY_MARGIN20_PCT", "20"))
        self.intraday_default_limits = {
            "margin3count": int(os.getenv("INTRADAY_MARGIN3_COUNT", "10")),
            "margin5count": int(os.getenv("INTRADAY_MARGIN5_COUNT", "0")),
            "margin10count": int(os.getenv("INTRADAY_MARGIN10_COUNT", "0")),
            "margin20count": int(os.getenv("INTRADAY_MARGIN20_COUNT", "0")),
            "profit": float(os.getenv("INTRADAY_PROFIT_PCT", "0.5")),
            "stoploss": float(os.getenv("INTRADAY_STOPLOSS_PCT", "3.0")),
            "stoploss_limit": float(os.getenv("INTRADAY_STOPLOSS_LIMIT_PCT", "3.0")),
            "amount": float(os.getenv("INTRADAY_TRADE_AMOUNT", "100")),
            "number_of_trades": int(os.getenv("INTRADAY_NUM_TRADES", "10")),
            "pump_pullback_enabled": 1 if os.getenv("INTRADAY_PUMP_PULLBACK_ENABLED", "false").lower() == "true" else 0,
            "pump_threshold_pct": float(os.getenv("INTRADAY_PUMP_THRESHOLD_PCT", "3.0")),
            "pullback_atr_mult": float(os.getenv("INTRADAY_PULLBACK_ATR_MULT", "1.5")),
            "pullback_range_mult": float(os.getenv("INTRADAY_PULLBACK_RANGE_MULT", "0.6")),
            "bounce_pct": float(os.getenv("INTRADAY_BOUNCE_PCT", "0.3")),
            "bounce_lookback": int(os.getenv("INTRADAY_BOUNCE_LOOKBACK", "5")),
            "avoid_top_pct": float(os.getenv("INTRADAY_AVOID_TOP_PCT", "1.0")),
            "trades_filter_enabled": 1 if os.getenv("INTRADAY_TRADES_FILTER_ENABLED", "true").lower() == "true" else 0,
            "min_trades_1m": int(os.getenv("INTRADAY_MIN_TRADES_1M", "50")),
        }
        self.intraday_sync_limits = os.getenv("INTRADAY_SYNC_LIMITS", "true").lower() == "true"
        self.intraday_reset_on_start = os.getenv("INTRADAY_RESET_ON_START", "true").lower() == "true"
        self.intraday_disable_stop = os.getenv("INTRADAY_DISABLE_STOP", "false").lower() == "true"
        self.intraday_trend_filter = os.getenv("INTRADAY_TREND_FILTER", "ema25").lower()
        self.intraday_volume_min_mult = float(os.getenv("INTRADAY_VOL_MIN_MULT", "0.8"))
        self.intraday_pump_5m_pct = float(os.getenv("INTRADAY_PUMP_5M_PCT", "1.5"))
        self.intraday_pump_30m_pct = float(os.getenv("INTRADAY_PUMP_30M_PCT", "3.0"))
        self.intraday_live_confirm = os.getenv("INTRADAY_LIVE_CONFIRM", "true").lower() == "true"
        self.intraday_cooldown_sec = int(os.getenv("INTRADAY_COOLDOWN_SEC", "600"))
        self.intraday_loss_exit_pct = float(os.getenv("INTRADAY_LOSS_EXIT_PCT", "2.0"))
        self.intraday_loss_exit_candles = int(os.getenv("INTRADAY_LOSS_EXIT_CANDLES", "5"))
        self.intraday_loss_sell_ratio = float(os.getenv("INTRADAY_LOSS_SELL_RATIO", "1.2"))
        self.intraday_fee_buffer_pct = float(os.getenv("INTRADAY_FEE_BUFFER_PCT", "0.2"))
        self.intraday_loss_red_ratio = float(os.getenv("INTRADAY_LOSS_RED_RATIO", "0.6"))
        self.intraday_trend_slope_lookback = int(os.getenv("INTRADAY_TREND_SLOPE_LOOKBACK", "5"))
        self.intraday_trend_slope_min = float(os.getenv("INTRADAY_TREND_SLOPE_MIN", "0.0"))
        self.intraday_bearish_block_enabled = os.getenv("INTRADAY_BEARISH_BLOCK_ENABLED", "true").lower() == "true"
        self.intraday_bearish_lookback = int(os.getenv("INTRADAY_BEARISH_LOOKBACK", "8"))
        self.intraday_trend_exit_pct = float(os.getenv("INTRADAY_TREND_EXIT_PCT", "0.7"))
        # Binance live trading settings
        self.binance_api_key: Optional[str] = None
        self.binance_api_secret: Optional[str] = None
        self.binance_base_url = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
        self.binance_recv_window = int(os.getenv("BINANCE_RECV_WINDOW", "5000"))
        self.binance_timeout_sec = float(os.getenv("BINANCE_TIMEOUT_SEC", "10"))
        self.binance_exchangeinfo_ttl = int(os.getenv("BINANCE_EXCHANGEINFO_TTL", "300"))
        self.binance_sell_qty_buffer = float(os.getenv("BINANCE_SELL_QTY_BUFFER", "0.001"))  # 0.1% qty buffer
        self.paper_use_live_price = os.getenv("PAPER_USE_LIVE_PRICE", "true").lower() == "true"
        self._exchange_info_cache: Dict[str, dict] = {}
        # determine DB driver once and ensure tables
        self._driver = 'sqlite'
        conn = None
        cur = None
        try:
            conn, cur = get_database_connection()
            mod = type(cur).__module__
            if 'psycopg2' in mod:
                self._driver = 'postgres'
        except Exception:
            pass
        finally:
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass
        self._ensure_tables()

    @property
    def enabled(self) -> bool:
        return self._enabled or self.intraday_enabled or (self.coin_brain_thread is not None and self.coin_brain_thread.is_alive())

    def _ensure_tables(self):
        conn = None
        cur = None
        try:
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            ensure_trading_tables(cur, pg)
            ensure_intraday_tables(cur, pg)
            seed_portfolio(cur, pg, self.initial_cash)
            conn.commit()
        except Exception as e:
            logging.error(f"Error ensuring trade_logs table: {e}")
        finally:
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass

    def start(self):
        # ensure intraday loop is not running in parallel
        if self.intraday_enabled:
            self.stop_intraday()
        if self._thread and self._thread.is_alive():
            self._enabled = True
            self.strategy_mode = "hybrid"
            return
        self._stop.clear()
        self._enabled = True
        self.paper_trading = True
        self.strategy_mode = "hybrid"
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logging.info("TradingManager started")

    def stop(self):
        self._enabled = False
        self._stop.set()
        self.stop_coin_brain()
        self.stop_intraday()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)
        self.strategy_mode = "idle"
        logging.info("TradingManager stopped")

    def reset(self):
        # First halt all loops so nothing runs immediately after reset
        self.stop()
        # Clear all trade logs
        conn = None
        cur = None
        try:
            conn, cur = get_database_connection()
            cur.execute("DELETE FROM trade_logs")
            cur.execute("DELETE FROM paper_positions")
            cur.execute("DELETE FROM paper_portfolio")
            cur.execute(self._q("INSERT INTO paper_portfolio(cash) VALUES (?)"), (self.initial_cash,))
            conn.commit()
        except Exception as e:
            logging.error(f"Error clearing trade_logs: {e}")
        finally:
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass
        # also reset cooldowns
        self._last_trade_ts.clear()
        self.strategy_mode = "idle"
        self.hybrid_enabled = True
        self._last_stop_info.clear()
        self._ladder_state.clear()
        self._coin_brain_trades.clear()
        self._last_exit_ts.clear()

    def set_hybrid_enabled(self, enabled: bool):
        self.hybrid_enabled = bool(enabled)

    def _q(self, sql: str) -> str:
        # Translate SQLite-style placeholders to psycopg2 style when needed
        return sql.replace('?', '%s') if self._driver == 'postgres' else sql

    # ======== Intraday Mode ========
    def start_intraday(self, paper: bool = True):
        if self.intraday_thread and self.intraday_thread.is_alive():
            self.intraday_enabled = True
            self.intraday_paper = paper
            self.paper_trading = paper
            self.strategy_mode = "intraday"
            return
        # stop other loops to keep modes exclusive
        self.stop()
        self.stop_coin_brain()
        self._intraday_stop.clear()
        self.intraday_enabled = True
        self.intraday_paper = paper
        self.paper_trading = paper
        self.hybrid_enabled = False
        self.strategy_mode = "intraday"
        self.intraday_thread = threading.Thread(target=self._run_intraday_loop, daemon=True)
        self.intraday_thread.start()
        logging.info("IntradayManager started")

    def stop_intraday(self):
        self.intraday_enabled = False
        self._intraday_stop.set()
        if self.intraday_thread and self.intraday_thread.is_alive():
            self.intraday_thread.join(timeout=1.0)
        self.intraday_thread = None
        if self.strategy_mode == "intraday":
            self.strategy_mode = "idle"
        logging.info("IntradayManager stopped")

    def _daily_pnl(self, cur) -> float:
        """Realized PnL for the current day from trade_logs."""
        try:
            if self._driver == 'postgres':
                cur.execute("SELECT COALESCE(SUM(pnl),0) FROM trade_logs WHERE DATE(created_at) = CURRENT_DATE")
            else:
                cur.execute("SELECT COALESCE(SUM(pnl),0) FROM trade_logs WHERE DATE(created_at) = DATE('now')")
            row = cur.fetchone()
            return float(row[0] or 0.0)
        except Exception:
            return 0.0

    def _recent_ai_intention(self, cur, symbol: str) -> Optional[dict]:
        """Fetch the latest AI decision within the allowed age window."""
        if self._driver == 'postgres':
            sql = """
                SELECT intention, confidence, expected_return, regime, pattern_score, EXTRACT(EPOCH FROM created_at)
                FROM ai_decisions
                WHERE symbol = %s
                  AND created_at >= (NOW() - (%s * interval '1 second'))
                ORDER BY created_at DESC
                LIMIT 1
            """
            cur.execute(sql, (symbol, self.ai_decision_max_age_sec))
        else:
            sql = """
                SELECT intention, confidence, expected_return, regime, pattern_score, strftime('%s', created_at)
                FROM ai_decisions
                WHERE symbol = ?
                  AND created_at >= datetime('now', ?)
                ORDER BY created_at DESC
                LIMIT 1
            """
            cur.execute(sql, (symbol, f"-{self.ai_decision_max_age_sec} seconds"))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "intention": row[0],
            "confidence": float(row[1] or 0.0),
            "expected_return": float(row[2] or 0.0),
            "regime": row[3],
            "pattern_score": float(row[4] or 0.0),
            "created_at_ts": float(row[5] or 0.0),
        }

    def _latest_price(self, cur, symbol: str) -> Optional[float]:
        if self.paper_use_live_price:
            live = self._fetch_live_price(symbol)
            if live is not None and live > 0:
                return live
        cur.execute(self._q("SELECT latest_price FROM coin_monitor WHERE symbol = ?"), (symbol,))
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else None

    def _latest_atr(self, cur, symbol: str, timeframe: str = "1m") -> Optional[float]:
        try:
            cur.execute(self._q("SELECT atr FROM features WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT 1"), (symbol, timeframe))
            row = cur.fetchone()
            return float(row[0]) if row and row[0] is not None else None
        except Exception:
            return None

    def _volume_ok(self, cur, symbol: str, timeframe: str = "1m", lookback: int = 20) -> bool:
        """Check if current volume is at least a fraction of avg recent volume."""
        try:
            cur.execute(self._q("SELECT volume FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT 1"), (symbol, timeframe))
            row = cur.fetchone()
            cur_vol = float(row[0]) if row and row[0] is not None else None
            cur.execute(self._q("SELECT AVG(volume) FROM (SELECT volume FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT ?) sub"), (symbol, timeframe, lookback))
            row = cur.fetchone()
            avg_vol = float(row[0]) if row and row[0] is not None else None
            if cur_vol is None or avg_vol is None or avg_vol <= 0:
                return True  # no data, do not block
            if cur_vol >= avg_vol * self.coin_brain_vol_min_mult:
                return True
            return False
        except Exception:
            return True

    def set_binance_credentials(self, api_key: str, api_secret: str) -> bool:
        if not api_key or not api_secret:
            return False
        self.binance_api_key = api_key.strip()
        self.binance_api_secret = api_secret.strip()
        logging.info("Binance API credentials set for live trading.")
        return True

    def _binance_ready(self, api_key: Optional[str] = None, api_secret: Optional[str] = None) -> bool:
        key = api_key or self.binance_api_key
        secret = api_secret or self.binance_api_secret
        if not key or not secret:
            logging.error("Live trade attempted without Binance API credentials.")
            return False
        return True

    def _fmt_number(self, value: float) -> str:
        return f"{value:.8f}".rstrip("0").rstrip(".")

    def _floor_to_step(self, value: float, step: float) -> float:
        try:
            if step <= 0:
                return value
            d_value = Decimal(str(value))
            d_step = Decimal(str(step))
            steps = (d_value / d_step).to_integral_value(rounding=ROUND_DOWN)
            return float(steps * d_step)
        except Exception:
            return value

    def _ceil_to_step(self, value: float, step: float) -> float:
        try:
            if step <= 0:
                return value
            d_value = Decimal(str(value))
            d_step = Decimal(str(step))
            steps = (d_value / d_step).to_integral_value(rounding=ROUND_UP)
            return float(steps * d_step)
        except Exception:
            return value

    def _ema(self, values: List[float], period: int) -> Optional[float]:
        try:
            if not values:
                return None
            period = max(1, min(int(period), len(values)))
            alpha = 2.0 / (period + 1.0)
            ema = float(values[0])
            for v in values[1:]:
                ema = (float(v) - ema) * alpha + ema
            return ema
        except Exception:
            return None

    def _get_exchange_filters(self, symbol: str) -> Optional[dict]:
        """Fetch and cache Binance exchangeInfo filters for a symbol."""
        try:
            sym = symbol.upper()
            cached = self._exchange_info_cache.get(sym)
            now = time.time()
            if cached and (now - cached.get("ts", 0) < self.binance_exchangeinfo_ttl):
                return cached.get("filters")
            url = f"{self.binance_base_url}/api/v3/exchangeInfo"
            resp = requests.get(url, params={"symbol": sym}, timeout=self.binance_timeout_sec)
            if not resp.ok:
                logging.error(f"Binance exchangeInfo error {resp.status_code}: {resp.text}")
                return cached.get("filters") if cached else None
            data = resp.json()
            symbols = data.get("symbols") or []
            if not symbols:
                return cached.get("filters") if cached else None
            info = symbols[0]
            filters = {}
            for f in info.get("filters", []):
                if f.get("filterType") == "PRICE_FILTER":
                    filters["tickSize"] = float(f.get("tickSize") or 0)
                    filters["minPrice"] = float(f.get("minPrice") or 0)
                    filters["maxPrice"] = float(f.get("maxPrice") or 0)
                if f.get("filterType") == "LOT_SIZE":
                    filters["stepSize"] = float(f.get("stepSize") or 0)
                    filters["minQty"] = float(f.get("minQty") or 0)
                    filters["maxQty"] = float(f.get("maxQty") or 0)
                if f.get("filterType") == "MIN_NOTIONAL":
                    filters["minNotional"] = float(f.get("minNotional") or 0)
            self._exchange_info_cache[sym] = {"ts": now, "filters": filters}
            return filters
        except Exception as e:
            logging.error(f"Failed to fetch exchangeInfo for {symbol}: {e}")
            return None

    def _binance_signed_request(self, method: str, path: str, params: dict, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        if not self._binance_ready(api_key, api_secret):
            return None
        key = api_key or self.binance_api_key
        secret = api_secret or self.binance_api_secret
        payload = dict(params or {})
        payload["timestamp"] = int(time.time() * 1000)
        payload["recvWindow"] = self.binance_recv_window
        query = urllib.parse.urlencode(payload, doseq=True)
        signature = hmac.new(secret.encode("utf-8"), query.encode("utf-8"), hashlib.sha256).hexdigest()
        url = f"{self.binance_base_url}{path}?{query}&signature={signature}"
        headers = {"X-MBX-APIKEY": key}
        try:
            method = method.upper()
            if method == "POST":
                resp = requests.post(url, headers=headers, timeout=self.binance_timeout_sec)
            elif method == "DELETE":
                resp = requests.delete(url, headers=headers, timeout=self.binance_timeout_sec)
            else:
                resp = requests.get(url, headers=headers, timeout=self.binance_timeout_sec)
        except Exception as e:
            logging.error(f"Binance request error: {e}")
            return None
        if not resp.ok:
            logging.error(f"Binance API error {resp.status_code}: {resp.text}")
            return None
        try:
            return resp.json()
        except Exception:
            logging.error("Binance API returned non-JSON response.")
            return None

    def _binance_public_request(self, path: str, params: dict) -> Optional[dict]:
        url = f"{self.binance_base_url}{path}"
        try:
            resp = requests.get(url, params=params, timeout=self.binance_timeout_sec)
        except Exception as e:
            logging.error(f"Binance public request error: {e}")
            return None
        if not resp.ok:
            logging.error(f"Binance public API error {resp.status_code}: {resp.text}")
            return None
        try:
            return resp.json()
        except Exception:
            logging.error("Binance public API returned non-JSON response.")
            return None

    def _place_spot_order(
        self,
        symbol: str,
        side: str,
        quantity: Optional[float] = None,
        quote_qty: Optional[float] = None,
        order_type: str = "MARKET",
        price: Optional[float] = None,
        time_in_force: Optional[str] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
    ):
        filters = self._get_exchange_filters(symbol)
        params = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": order_type.upper(),
            "newOrderRespType": "FULL",
        }
        if order_type.upper() == "MARKET":
            if side.upper() == "BUY":
                if quote_qty is None or quote_qty <= 0:
                    logging.error("Binance BUY requires quoteOrderQty.")
                    return None
                if filters and filters.get("minNotional") and quote_qty < filters["minNotional"]:
                    logging.error(f"Binance BUY notional too small for {symbol}: {quote_qty} < {filters['minNotional']}")
                    return None
                params["quoteOrderQty"] = self._fmt_number(quote_qty)
            else:
                if quantity is None or quantity <= 0:
                    logging.error("Binance SELL requires quantity.")
                    return None
                if filters and filters.get("stepSize"):
                    quantity = self._floor_to_step(quantity, filters["stepSize"])
                if filters and filters.get("minQty") and quantity < filters["minQty"]:
                    logging.error(f"Binance SELL qty too small for {symbol}: {quantity} < {filters['minQty']}")
                    return None
                params["quantity"] = self._fmt_number(quantity)
        elif order_type.upper() == "LIMIT":
            if quantity is None or quantity <= 0 or price is None or price <= 0:
                logging.error("Binance LIMIT order requires quantity and price.")
                return None
            if filters and filters.get("stepSize"):
                quantity = self._floor_to_step(quantity, filters["stepSize"])
            if filters and filters.get("tickSize"):
                price = self._floor_to_step(price, filters["tickSize"])
            if filters and filters.get("minQty") and quantity < filters["minQty"]:
                logging.error(f"Binance LIMIT qty too small for {symbol}: {quantity} < {filters['minQty']}")
                return None
            if filters and filters.get("minNotional") and (price * quantity) < filters["minNotional"]:
                logging.error(f"Binance LIMIT notional too small for {symbol}: {price * quantity} < {filters['minNotional']}")
                return None
            params["quantity"] = self._fmt_number(quantity)
            params["price"] = self._fmt_number(price)
            params["timeInForce"] = time_in_force or "GTC"
        else:
            logging.error(f"Unsupported Binance order type: {order_type}")
            return None
        return self._binance_signed_request("POST", "/api/v3/order", params, api_key, api_secret)

    def _extract_order_fill(self, order: dict) -> dict:
        qty = float(order.get("executedQty") or 0)
        quote = float(order.get("cummulativeQuoteQty") or 0)
        price = (quote / qty) if qty > 0 else None
        return {"qty": qty, "quote": quote, "price": price}

    def _get_spot_order(self, symbol: str, order_id: str):
        params = {"symbol": symbol.upper(), "orderId": str(order_id)}
        return self._binance_signed_request("GET", "/api/v3/order", params)

    def _cancel_spot_order(self, symbol: str, order_id: str):
        params = {"symbol": symbol.upper(), "orderId": str(order_id)}
        return self._binance_signed_request("DELETE", "/api/v3/order", params)

    def _base_asset_from_symbol(self, symbol: str) -> Optional[str]:
        sym = (symbol or "").upper()
        if sym.endswith("USDT"):
            return sym[:-4]
        return None

    def _get_account_info(self):
        return self._binance_signed_request("GET", "/api/v3/account", {})

    def _get_asset_free_balance(self, asset: str) -> Optional[float]:
        if not asset:
            return None
        data = self._get_account_info()
        if not data or "balances" not in data:
            logging.error("Failed to fetch Binance account balances.")
            return None
        for bal in data.get("balances", []):
            if bal.get("asset") == asset:
                try:
                    return float(bal.get("free") or 0)
                except Exception:
                    return None
        return 0.0

    def _apply_sell_qty_buffer(self, symbol: str, qty: float) -> float:
        base_asset = self._base_asset_from_symbol(symbol)
        if not base_asset:
            return qty
        free = self._get_asset_free_balance(base_asset)
        if free is None:
            return qty
        available = min(qty, free)
        buffer_pct = max(0.0, float(self.binance_sell_qty_buffer or 0))
        sell_qty = available * (1.0 - buffer_pct)
        if sell_qty < available:
            logging.info(
                f"Adjusted sell qty for {symbol}: free={free} req={qty} buffer={buffer_pct} -> {sell_qty}"
            )
        return sell_qty

    def place_spot_order(
        self,
        symbol: str,
        side: str,
        amount: Optional[float] = None,
        quantity: Optional[float] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
    ):
        if side.upper() == "BUY":
            return self._place_spot_order(symbol, "BUY", quote_qty=amount, api_key=api_key, api_secret=api_secret)
        return self._place_spot_order(symbol, "SELL", quantity=quantity, api_key=api_key, api_secret=api_secret)

    def _paper_for_entry(self, entry_type: Optional[str]) -> bool:
        if entry_type == "intraday":
            return self.intraday_paper
        return self.paper_trading

    # ========== Coin Brain (single-symbol trading) ==========
    def start_coin_brain(self, symbol: str, paper: bool = True):
        """Start a dedicated loop trading a single symbol every 3s using a simple rise/fall logic."""
        self.stop_coin_brain()
        self.coin_brain_symbol = symbol
        self.coin_brain_paper = paper
        self.paper_trading = paper
        self.coin_brain_stop.clear()
        self.coin_brain_thread = threading.Thread(target=self._run_coin_brain, daemon=True)
        self.coin_brain_thread.start()
        self.strategy_mode = "coin"
        logging.info(f"Coin Brain started for {symbol} (paper={paper})")

    def stop_coin_brain(self):
        self.coin_brain_stop.set()
        if self.coin_brain_thread and self.coin_brain_thread.is_alive():
            self.coin_brain_thread.join(timeout=1.0)
        self.coin_brain_thread = None
        self.coin_brain_symbol = None

    def _run_coin_brain(self):
        """Every ~3 seconds, read latest price for the chosen symbol and trade on simple thresholds using paper positions."""
        symbol = self.coin_brain_symbol
        if not symbol:
            return
        interval = 3.0
        last_price = None
        while not self.coin_brain_stop.is_set():
            try:
                conn, cur = get_database_connection()
                price = self._latest_price(cur, symbol)
                if price is None or price <= 0:
                    try:
                        cur.close(); conn.close()
                    except Exception:
                        pass
                    time.sleep(interval); continue

                # Manage existing position if any
                positions = self._open_positions(cur)
                if symbol in positions:
                    pos = positions[symbol]
                    entry_price = float(pos["entry_price"])
                    # Simple take-profit / stop-loss on percent moves
                    if price >= entry_price * (1 + self.coin_brain_tp_pct):
                        self._close_position(cur, pos, price, "tp_pct_hit")
                        conn.commit()
                        last_price = price
                        try:
                            cur.close(); conn.close()
                        except Exception:
                            pass
                        time.sleep(interval)
                        continue
                    if price <= entry_price * (1 - self.coin_brain_sl_pct):
                        self._close_position(cur, pos, price, "sl_pct_hit")
                        conn.commit()
                        last_price = price
                        try:
                            cur.close(); conn.close()
                        except Exception:
                            pass
                        time.sleep(interval)
                        continue
                    # Ladder add if price dips further than last fill step
                    if self.coin_brain_ladder_enabled:
                        state = self._ladder_state.get(symbol, {"fills": 1, "last_fill_price": entry_price})
                        if state["fills"] < self.coin_brain_ladder_slices and price <= state["last_fill_price"] * (1 - self.coin_brain_ladder_step_pct):
                            if self._volume_ok(cur, symbol, timeframe="1m"):
                                if not self._can_trade(symbol):
                                    last_price = price
                                    try:
                                        cur.close(); conn.close()
                                    except Exception:
                                        pass
                                    time.sleep(interval); continue
                                added = self._add_to_position(cur, pos, price)
                                if added:
                                    conn.commit()
                                    self._register_trade()
                                    last_price = price
                                    try:
                                        cur.close(); conn.close()
                                    except Exception:
                                        pass
                                    time.sleep(interval)
                                    continue
                    dirty = self._manage_position(cur, positions[symbol], price)
                    if dirty:
                        conn.commit()
                    last_price = price
                    try:
                        cur.close(); conn.close()
                    except Exception:
                        pass
                    time.sleep(interval)
                    continue

                # No open position: look for entry based on small blips
                if last_price is None:
                    last_price = price
                    try:
                        cur.close(); conn.close()
                    except Exception:
                        pass
                    time.sleep(interval); continue

                # If there is any other open position (other symbol), do not open new ones
                if positions:
                    last_price = price
                    try:
                        cur.close(); conn.close()
                    except Exception:
                        pass
                    time.sleep(interval); continue

                buy_threshold = 0.0005  # +0.05% vs last
                change = (price - last_price) / last_price if last_price else 0.0
                # Re-entry after stop: if recent stop and quick momentum up
                now = time.time()
                reentry_window = self.coin_brain_reentry_bars * interval
                reentry_hit = False
                if symbol in self._last_stop_info:
                    info = self._last_stop_info[symbol]
                    if now - info.get("ts", 0) <= reentry_window:
                        stop_price = float(info.get("price", price))
                        if price >= stop_price * (1 + self.coin_brain_reentry_pct):
                            reentry_hit = True
                    else:
                        # window expired
                        self._last_stop_info.pop(symbol, None)

                allow_volume = self._volume_ok(cur, symbol, timeframe="1m")

                # Buy only on dips OR permitted re-entry, and only if volume gate passes
                if allow_volume and (reentry_hit or change <= -self.coin_brain_buy_dip_pct):
                    if not self._can_trade(symbol):
                        last_price = price
                        try:
                            cur.close(); conn.close()
                        except Exception:
                            pass
                        time.sleep(interval); continue
                    opened = self._open_position(cur, symbol, price, direction="LONG")
                    if opened is not None:
                        conn.commit()
                        self._register_trade()
                        if reentry_hit:
                            # Clear stop info after successful re-entry
                            self._last_stop_info.pop(symbol, None)
                last_price = price
                try:
                    cur.close(); conn.close()
                except Exception:
                    pass
            except Exception as e:
                logging.error(f"Coin Brain error for {symbol}: {e}")
            time.sleep(interval)

    def _record_trade(self, symbol: str, side: str, qty: float, price: float, pnl: float, balance_after: float, reason: str = "", status: str = "COMPLETED"):
        conn = None
        cur = None
        try:
            conn, cur = get_database_connection()
            sql = self._q(
                "INSERT INTO trade_logs(symbol, side, qty, price, pnl, balance_after, reason, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            )
            cur.execute(sql, (symbol, side, qty, price, pnl, balance_after, reason, status))
            conn.commit()
            logging.info(f"Recorded trade: {symbol} {side} qty={qty} price={price} reason={reason}")
        except Exception as e:
            logging.error(f"Error inserting trade log: {e}")
        finally:
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass

    def _register_trade(self):
        """Track trade timestamps for rate limiting."""
        now = time.time()
        self._coin_brain_trades.append(now)
        # Prune older than 1 hour
        cutoff = now - 3600
        self._coin_brain_trades = [t for t in self._coin_brain_trades if t >= cutoff]

    def _can_trade(self, symbol: str) -> bool:
        now = time.time()
        # cooldown after exits
        last_exit = self._last_exit_ts.get(symbol)
        if last_exit and (now - last_exit) < self.coin_brain_cooldown_sec:
            return False
        # rate limit per hour
        cutoff = now - 3600
        recent = [t for t in self._coin_brain_trades if t >= cutoff]
        self._coin_brain_trades = recent
        if self.coin_brain_max_trades_per_hour > 0 and len(recent) >= self.coin_brain_max_trades_per_hour:
            return False
        return True

    def _add_to_position(self, cur, pos: dict, price: float) -> bool:
        """Add a ladder slice to an existing position, updating blended avg, stop, TP."""
        symbol = pos["symbol"]
        state = self._ladder_state.get(symbol, {"fills": 1, "last_fill_price": pos["entry_price"]})
        if state["fills"] >= self.coin_brain_ladder_slices:
            return False
        cash = self._get_cash(cur)
        invested_est = float(pos["qty"]) * float(pos["entry_price"])
        remaining_cap = max(0.0, self.coin_brain_ladder_total_cap - invested_est)
        slice_notional = min(self.coin_brain_ladder_slice_usd, cash, remaining_cap)
        if slice_notional <= 0 or price <= 0:
            return False
        add_qty = slice_notional / price
        if not self.paper_trading:
            order = self._place_spot_order(symbol, "BUY", quote_qty=slice_notional)
            if not order:
                logging.error(f"Live ladder buy failed for {symbol}")
                return False
            fill = self._extract_order_fill(order)
            if fill["qty"] <= 0:
                logging.error(f"Live ladder buy returned 0 qty for {symbol}")
                return False
            add_qty = fill["qty"]
            if fill["price"]:
                price = fill["price"]
            if fill["quote"] > 0:
                slice_notional = fill["quote"]
        new_qty = float(pos["qty"]) + add_qty
        if new_qty <= 0:
            return False
        new_avg = (float(pos["entry_price"]) * float(pos["qty"]) + add_qty * price) / new_qty
        new_stop = new_avg * (1 - self.coin_brain_ladder_stop_pct)
        new_tp = new_avg * (1 + self.coin_brain_ladder_tp_pct)
        new_trail = max(float(pos.get("trailing_stop") or new_stop), new_stop)
        r_value = abs(new_avg - new_stop)

        # Update cash and position
        self._set_cash(cur, cash - slice_notional)
        cur.execute(
            self._q("UPDATE paper_positions SET qty = ?, entry_price = ?, stop_price = ?, take_profit_price = ?, trailing_stop = ?, r_value = ? WHERE id = ?"),
            (new_qty, new_avg, new_stop, new_tp, new_trail, r_value, pos["id"]),
        )
        cur.execute(
            self._q("INSERT INTO trade_logs(symbol, side, qty, price, pnl, balance_after, reason, status) VALUES (?, 'BUY', ?, ?, 0, ?, ?, 'OPEN')"),
            (symbol, add_qty, price, cash - slice_notional, "ladder_add"),
        )
        self._ladder_state[symbol] = {"fills": state["fills"] + 1, "last_fill_price": price}
        logging.info(f"Ladder add {symbol}: fills={state['fills'] + 1}/{self.coin_brain_ladder_slices}, avg={new_avg}, stop={new_stop}, tp={new_tp}")
        return True

    def _open_position(self, cur, symbol: str, price: float, direction: str = "LONG") -> Optional[int]:
        """Open a paper position with ATR/distal buffer sizing."""
        if direction != "LONG":
            return None
        cash = self._get_cash(cur)
        atr = self._latest_atr(cur, symbol)
        buffer = max(price * self.buffer_pct, (atr or 0) * self.atr_mult)
        stop_price = price - buffer
        r_value = abs(price - stop_price)
        if r_value <= 0:
            return None
        # If laddering is enabled, size by slice; else risk-based sizing
        if self.coin_brain_ladder_enabled:
            slice_notional = min(self.coin_brain_ladder_slice_usd, cash, self.coin_brain_ladder_total_cap, self.max_notional_per_trade)
            if slice_notional <= 0 or price <= 0:
                return None
            qty = slice_notional / price
        else:
            risk_amount = cash * (self.risk_perc_default / 100.0)
            qty = risk_amount / r_value
            if qty * price > cash:
                qty = (cash / price) * 0.99  # never exceed current cash
        if qty <= 0:
            return None
        notional = qty * price
        # Hard caps: do not open if notional still exceeds available cash or configured ceiling
        if notional > cash or notional > self.max_notional_per_trade or notional <= 0:
            return None
        if not self.paper_trading:
            order = self._place_spot_order(symbol, "BUY", quote_qty=notional)
            if not order:
                logging.error(f"Live buy failed for {symbol}")
                return None
            fill = self._extract_order_fill(order)
            if fill["qty"] <= 0:
                logging.error(f"Live buy returned 0 qty for {symbol}")
                return None
            qty = fill["qty"]
            if fill["price"]:
                price = fill["price"]
            if fill["quote"] > 0:
                notional = fill["quote"]

        buffer = max(price * self.buffer_pct, (atr or 0) * self.atr_mult)
        stop_price = price - buffer
        r_value = abs(price - stop_price)
        if r_value <= 0:
            return None
        tp_price = price + r_value * self.rr_target_default
        # Deduct cash
        new_cash = cash - qty * price
        self._set_cash(cur, new_cash)
        cur.execute(
            self._q(
                """
                INSERT INTO paper_positions(symbol, qty, entry_price, stop_price, take_profit_price, risk_perc, rr_target, entry_type, r_value, breakeven_set, partial_taken, trailing_stop)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            ),
            (
                symbol,
                qty,
                price,
                stop_price,
                tp_price,
                self.risk_perc_default,
                self.rr_target_default,
                "type2",
                r_value,
                0,
                0,
                stop_price,
            ),
        )
        # Seed ladder state
        if self.coin_brain_ladder_enabled:
            self._ladder_state[symbol] = {"fills": 1, "last_fill_price": price}
        # Log entry trade as OPEN
        try:
            cur.execute(
                self._q("INSERT INTO trade_logs(symbol, side, qty, price, pnl, balance_after, reason, status) VALUES (?, 'BUY', ?, ?, 0, ?, ?, 'OPEN')"),
                (symbol, qty, price, new_cash, "entry"),
            )
        except Exception:
            pass
        logging.info(f"Opened position {symbol} qty={qty:.4f} entry={price} stop={stop_price} tp={tp_price} risk%={self.risk_perc_default}")
        return cur.lastrowid if self._driver != 'postgres' else None

    def _close_position(self, cur, pos: dict, price: float, reason: str):
        """Close an open position and realize PnL."""
        qty = float(pos["qty"])
        entry_type = pos.get("entry_type")
        if not self._paper_for_entry(entry_type):
            qty = self._apply_sell_qty_buffer(pos["symbol"], qty)
            if qty <= 0:
                logging.error(f"Live sell qty invalid for {pos['symbol']} (qty={qty})")
                return
            order = self._place_spot_order(pos["symbol"], "SELL", quantity=qty)
            if not order:
                logging.error(f"Live sell failed for {pos['symbol']}")
                return
            fill = self._extract_order_fill(order)
            if fill["qty"] <= 0:
                logging.error(f"Live sell returned 0 qty for {pos['symbol']}")
                return
            qty = fill["qty"]
            if fill["price"]:
                price = fill["price"]
        self._finalize_position(cur, pos, qty, price, reason)

    def _finalize_position(self, cur, pos: dict, qty: float, price: float, reason: str):
        entry = float(pos["entry_price"])
        pnl = (price - entry) * qty
        cash = self._get_cash(cur)
        cash += price * qty
        self._set_cash(cur, cash)
        cur.execute(self._q("UPDATE paper_positions SET status = 'CLOSED' WHERE id = ?"), (pos["id"],))
        cur.execute(
            self._q("INSERT INTO trade_logs(symbol, side, qty, price, pnl, balance_after, reason, status) VALUES (?, ?, ?, ?, ?, ?, ?, 'COMPLETED')"),
            (pos["symbol"], "SELL", qty, price, pnl, cash, reason),
        )
        # Allow intraday symbols to be re-traded after a cooldown by resetting status
        if pos.get("entry_type") == "intraday":
            try:
                cur.execute(
                    self._q(
                        "UPDATE intraday_trading SET status = '0', mar3 = 0, mar5 = 0, mar10 = 0, mar20 = 0, purchase_price = NULL WHERE symbol = ?"
                    ),
                    (pos["symbol"],),
                )
            except Exception:
                pass
        # Track stop exits for potential re-entry logic
        try:
            if "stop" in (reason or "").lower() or "sl" in (reason or "").lower():
                self._last_stop_info[pos["symbol"]] = {"ts": time.time(), "price": price}
        except Exception:
            pass
        try:
            self._ladder_state.pop(pos["symbol"], None)
        except Exception:
            pass
        try:
            self._last_exit_ts[pos["symbol"]] = time.time()
        except Exception:
            pass
        logging.info(f"Closed position {pos['symbol']} qty={qty} entry={entry} exit={price} pnl={pnl} reason={reason}")

    def _manage_position(self, cur, pos: dict, price: float) -> bool:
        """Apply stop/tp/breakeven/trailing/partials to an open position."""
        entry = float(pos["entry_price"])
        stop = float(pos["stop_price"] or entry)
        tp = float(pos["take_profit_price"] or entry + pos.get("r_value", 0))
        r_value = float(pos.get("r_value") or abs(entry - stop))
        qty = float(pos["qty"])
        breakeven_set = int(pos.get("breakeven_set") or 0)
        partial_taken = int(pos.get("partial_taken") or 0)
        trailing_stop = float(pos.get("trailing_stop") or stop)

        # Stop/TP checks
        if price <= stop:
            self._close_position(cur, pos, price, "stop_hit")
            return True
        if price >= tp:
            self._close_position(cur, pos, price, "take_profit")
            return True

        # Breakeven at +1R
        dirty = False
        if not breakeven_set and (price - entry) >= r_value:
            stop = entry
            breakeven_set = 1
            cur.execute(self._q("UPDATE paper_positions SET stop_price = ?, breakeven_set = 1 WHERE id = ?"), (stop, pos["id"]))
            logging.info(f"Breakeven set for {pos['symbol']} at {stop}")
            dirty = True

        # Partial at +2R
        if not partial_taken and (price - entry) >= 2 * r_value and qty > 0:
            close_qty = qty * 0.5
            if not self._paper_for_entry(pos.get("entry_type")):
                order = self._place_spot_order(pos["symbol"], "SELL", quantity=close_qty)
                if not order:
                    logging.error(f"Live partial sell failed for {pos['symbol']}")
                    return False
                fill = self._extract_order_fill(order)
                if fill["qty"] <= 0:
                    logging.error(f"Live partial sell returned 0 qty for {pos['symbol']}")
                    return False
                close_qty = fill["qty"]
                if fill["price"]:
                    price = fill["price"]
            pnl = (price - entry) * close_qty
            cash = self._get_cash(cur) + price * close_qty
            self._set_cash(cur, cash)
            new_qty = qty - close_qty
            cur.execute(self._q("UPDATE paper_positions SET qty = ?, partial_taken = 1 WHERE id = ?"), (new_qty, pos["id"]))
            cur.execute(
                self._q("INSERT INTO trade_logs(symbol, side, qty, price, pnl, balance_after, reason, status) VALUES (?, ?, ?, ?, ?, ?, ?, 'COMPLETED')"),
                (pos["symbol"], "SELL", close_qty, price, pnl, cash, "partial_2R"),
            )
            logging.info(f"Partial taken for {pos['symbol']} qty={close_qty} at {price}")
            qty = new_qty
            partial_taken = 1
            dirty = True

        # Trailing stop based on ATR
        atr = self._latest_atr(cur, pos["symbol"])
        if atr is not None:
            candidate_trail = price - atr * self.atr_trail_mult
            if candidate_trail > trailing_stop:
                trailing_stop = candidate_trail
                stop = max(stop, trailing_stop, entry if breakeven_set else stop)
                cur.execute(self._q("UPDATE paper_positions SET trailing_stop = ?, stop_price = ? WHERE id = ?"), (trailing_stop, stop, pos["id"]))
                logging.info(f"Trailing stop raised for {pos['symbol']} to {stop}")
                dirty = True

        # Final stop breach after updates
        if price <= stop:
            self._close_position(cur, pos, price, "trailing_stop_hit")
            return True

        # Persist current values if changed
        return dirty

    # ======== Intraday helpers ========
    def _ensure_intraday_limits(self, cur, pg: bool):
        cur.execute("SELECT COUNT(1) FROM intraday_limits")
        count = cur.fetchone()[0]
        if int(count or 0) == 0:
            cur.execute(
                self._q(
                    """
                    INSERT INTO intraday_limits(
                        margin3count, margin5count, margin10count, margin20count,
                        profit, stoploss, stoploss_limit, amount, number_of_trades,
                        pump_pullback_enabled, pump_threshold_pct, pullback_atr_mult, pullback_range_mult,
                        bounce_pct, bounce_lookback, avoid_top_pct, trades_filter_enabled, min_trades_1m
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """
                ),
                (
                    self.intraday_default_limits["margin3count"],
                    self.intraday_default_limits["margin5count"],
                    self.intraday_default_limits["margin10count"],
                    self.intraday_default_limits["margin20count"],
                    self.intraday_default_limits["profit"],
                    self.intraday_default_limits["stoploss"],
                    self.intraday_default_limits["stoploss_limit"],
                    self.intraday_default_limits["amount"],
                    self.intraday_default_limits["number_of_trades"],
                    self.intraday_default_limits["pump_pullback_enabled"],
                    self.intraday_default_limits["pump_threshold_pct"],
                    self.intraday_default_limits["pullback_atr_mult"],
                    self.intraday_default_limits["pullback_range_mult"],
                    self.intraday_default_limits["bounce_pct"],
                    self.intraday_default_limits["bounce_lookback"],
                    self.intraday_default_limits["avoid_top_pct"],
                    self.intraday_default_limits["trades_filter_enabled"],
                    self.intraday_default_limits["min_trades_1m"],
                ),
            )
        elif self.intraday_sync_limits:
            cur.execute(
                self._q(
                    """
                    UPDATE intraday_limits
                    SET margin3count = ?, margin5count = ?, margin10count = ?, margin20count = ?,
                        profit = ?, stoploss = ?, stoploss_limit = ?, amount = ?, number_of_trades = ?,
                        pump_pullback_enabled = ?, pump_threshold_pct = ?, pullback_atr_mult = ?, pullback_range_mult = ?,
                        bounce_pct = ?, bounce_lookback = ?, avoid_top_pct = ?, trades_filter_enabled = ?, min_trades_1m = ?
                    """
                ),
                (
                    self.intraday_default_limits["margin3count"],
                    self.intraday_default_limits["margin5count"],
                    self.intraday_default_limits["margin10count"],
                    self.intraday_default_limits["margin20count"],
                    self.intraday_default_limits["profit"],
                    self.intraday_default_limits["stoploss"],
                    self.intraday_default_limits["stoploss_limit"],
                    self.intraday_default_limits["amount"],
                    self.intraday_default_limits["number_of_trades"],
                    self.intraday_default_limits["pump_pullback_enabled"],
                    self.intraday_default_limits["pump_threshold_pct"],
                    self.intraday_default_limits["pullback_atr_mult"],
                    self.intraday_default_limits["pullback_range_mult"],
                    self.intraday_default_limits["bounce_pct"],
                    self.intraday_default_limits["bounce_lookback"],
                    self.intraday_default_limits["avoid_top_pct"],
                    self.intraday_default_limits["trades_filter_enabled"],
                    self.intraday_default_limits["min_trades_1m"],
                ),
            )

    def get_intraday_limits(self) -> dict:
        conn = None
        cur = None
        try:
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            ensure_intraday_tables(cur, pg)
            self._ensure_intraday_limits(cur, pg)
            limits = self._load_intraday_limits(cur)
            return limits or {}
        except Exception as e:
            logging.error(f"Error loading intraday limits: {e}")
            return {}
        finally:
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass

    def set_intraday_limits(self, updates: dict) -> dict:
        conn = None
        cur = None
        try:
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            ensure_intraday_tables(cur, pg)
            self._ensure_intraday_limits(cur, pg)
            # Update defaults from provided values
            for key in [
                "margin3count",
                "margin5count",
                "margin10count",
                "margin20count",
                "profit",
                "stoploss",
                "stoploss_limit",
                "amount",
                "number_of_trades",
                "pump_pullback_enabled",
                "pump_threshold_pct",
                "pullback_atr_mult",
                "pullback_range_mult",
                "bounce_pct",
                "bounce_lookback",
                "avoid_top_pct",
                "trades_filter_enabled",
                "min_trades_1m",
            ]:
                if key in updates and updates[key] is not None:
                    try:
                        val = updates[key]
                        if key in ("pump_pullback_enabled", "bounce_lookback", "trades_filter_enabled", "min_trades_1m") or key.endswith("count") or key == "number_of_trades":
                            self.intraday_default_limits[key] = int(val)
                        else:
                            self.intraday_default_limits[key] = float(val)
                    except Exception:
                        pass

            cur.execute(
                self._q(
                    """
                    UPDATE intraday_limits
                    SET margin3count = ?, margin5count = ?, margin10count = ?, margin20count = ?,
                        profit = ?, stoploss = ?, stoploss_limit = ?, amount = ?, number_of_trades = ?,
                        pump_pullback_enabled = ?, pump_threshold_pct = ?, pullback_atr_mult = ?, pullback_range_mult = ?,
                        bounce_pct = ?, bounce_lookback = ?, avoid_top_pct = ?, trades_filter_enabled = ?, min_trades_1m = ?
                    """
                ),
                (
                    self.intraday_default_limits["margin3count"],
                    self.intraday_default_limits["margin5count"],
                    self.intraday_default_limits["margin10count"],
                    self.intraday_default_limits["margin20count"],
                    self.intraday_default_limits["profit"],
                    self.intraday_default_limits["stoploss"],
                    self.intraday_default_limits["stoploss_limit"],
                    self.intraday_default_limits["amount"],
                    self.intraday_default_limits["number_of_trades"],
                    self.intraday_default_limits["pump_pullback_enabled"],
                    self.intraday_default_limits["pump_threshold_pct"],
                    self.intraday_default_limits["pullback_atr_mult"],
                    self.intraday_default_limits["pullback_range_mult"],
                    self.intraday_default_limits["bounce_pct"],
                    self.intraday_default_limits["bounce_lookback"],
                    self.intraday_default_limits["avoid_top_pct"],
                    self.intraday_default_limits["trades_filter_enabled"],
                    self.intraday_default_limits["min_trades_1m"],
                ),
            )
            conn.commit()
            return self._load_intraday_limits(cur) or {}
        except Exception as e:
            logging.error(f"Error updating intraday limits: {e}")
            return {}
        finally:
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass

    def _load_intraday_limits(self, cur) -> Optional[dict]:
        cur.execute(
            """
            SELECT margin3count, margin5count, margin10count, margin20count, profit, stoploss, stoploss_limit, amount, number_of_trades,
                   pump_pullback_enabled, pump_threshold_pct, pullback_atr_mult, pullback_range_mult, bounce_pct, bounce_lookback,
                   avoid_top_pct, trades_filter_enabled, min_trades_1m
            FROM intraday_limits
            LIMIT 1
            """
        )
        row = cur.fetchone()
        if not row:
            return None
        return {
            "margin3count": int(row[0] or 0),
            "margin5count": int(row[1] or 0),
            "margin10count": int(row[2] or 0),
            "margin20count": int(row[3] or 0),
            "profit": float(row[4] or 0.0),
            "stoploss": float(row[5] or 0.0),
            "stoploss_limit": float(row[6] or 0.0),
            "amount": float(row[7] or 0.0),
            "number_of_trades": int(row[8] or 0),
            "pump_pullback_enabled": int(row[9] or 0),
            "pump_threshold_pct": float(row[10] or 0.0),
            "pullback_atr_mult": float(row[11] or 0.0),
            "pullback_range_mult": float(row[12] or 0.0),
            "bounce_pct": float(row[13] or 0.0),
            "bounce_lookback": int(row[14] or 0),
            "avoid_top_pct": float(row[15] or 0.0),
            "trades_filter_enabled": int(row[16] or 0),
            "min_trades_1m": int(row[17] or 0),
        }

    def _intraday_counts(self, cur) -> dict:
        cur.execute(
            """
            SELECT
                SUM(CASE WHEN mar3 THEN 1 ELSE 0 END) AS sum_mar3,
                SUM(CASE WHEN mar5 THEN 1 ELSE 0 END) AS sum_mar5,
                SUM(CASE WHEN mar10 THEN 1 ELSE 0 END) AS sum_mar10,
                SUM(CASE WHEN mar20 THEN 1 ELSE 0 END) AS sum_mar20
            FROM intraday_trading
            WHERE status = '1'
            """
        )
        row = cur.fetchone()
        return {
            "sum_mar3": int(row[0] or 0),
            "sum_mar5": int(row[1] or 0),
            "sum_mar10": int(row[2] or 0),
            "sum_mar20": int(row[3] or 0),
        }

    def _intraday_trend_ok(self, cur, symbol: str, price: float) -> bool:
        if self.intraday_trend_filter == "none":
            return True
        try:
            cur.execute(
                self._q("SELECT ema25, ema7 FROM features WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT 1"),
                (symbol, "1m"),
            )
            row = cur.fetchone()
            if not row:
                return False
            ema25 = float(row[0] or 0.0)
            ema7 = float(row[1] or 0.0)
            if ema25 <= 0:
                return False
            if self.intraday_trend_filter == "ema25" and price < ema25:
                return False
            if self.intraday_trend_filter == "ema7_ema25" and ema7 <= ema25:
                return False

            # Optional slope check to avoid drifting downtrends
            lookback = max(2, int(self.intraday_trend_slope_lookback or 0))
            slope_min = float(self.intraday_trend_slope_min or 0.0)
            if lookback > 1 and slope_min >= 0:
                cur.execute(
                    self._q(
                        "SELECT ema25 FROM features WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT ?"
                    ),
                    (symbol, "1m", lookback),
                )
                rows = cur.fetchall()
                vals = [float(r[0]) for r in rows if r and r[0] is not None]
                if len(vals) >= 2:
                    ema_now = vals[0]
                    ema_prev = vals[-1]
                    if ema_prev > 0:
                        slope_pct = (ema_now - ema_prev) / ema_prev * 100.0
                        if slope_pct < slope_min:
                            return False
            return True
        except Exception:
            return False

    def _intraday_bearish_pattern_from_ohlc(self, opens: List[float], closes: List[float], price: float, lookback: int) -> bool:
        try:
            if not opens or not closes:
                return False
            n = max(5, min(int(lookback), len(closes)))
            opens = opens[-n:]
            closes = closes[-n:]
            red = sum(1 for i in range(n) if closes[i] < opens[i])
            red_ratio = red / float(n) if n > 0 else 0.0
            ema_fast = self._ema(closes, 9)
            ema_slow = self._ema(closes, 25)
            if ema_fast is None or ema_slow is None:
                return False
            trend_bearish = ema_fast < ema_slow and price <= ema_slow
            slope_bearish = False
            if len(closes) >= 6:
                recent = sum(closes[-3:]) / 3.0
                prev = sum(closes[-6:-3]) / 3.0
                if prev > 0:
                    slope_bearish = (recent - prev) / prev < 0
            return trend_bearish and slope_bearish and red_ratio >= float(self.intraday_loss_red_ratio or 0.6)
        except Exception:
            return False

    def _intraday_bearish_block(self, cur, symbol: str, price: float) -> bool:
        if not self.intraday_bearish_block_enabled:
            return False
        try:
            lookback = max(5, int(self.intraday_bearish_lookback or 8))
            cur.execute(
                self._q(
                    "SELECT open, close FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT ?"
                ),
                (symbol, "1m", lookback),
            )
            rows = cur.fetchall()
            if not rows:
                return False
            opens = [float(r[0]) for r in rows if r and r[0] is not None]
            closes = [float(r[1]) for r in rows if r and r[1] is not None]
            if len(opens) < 5 or len(closes) < 5:
                return False
            # Reverse to oldest->newest for pattern detection
            opens = list(reversed(opens))
            closes = list(reversed(closes))
            return self._intraday_bearish_pattern_from_ohlc(opens, closes, price, lookback)
        except Exception:
            return False

    def _intraday_bearish_block_live(self, candles: List[list], price: float) -> bool:
        if not self.intraday_bearish_block_enabled:
            return False
        try:
            lookback = max(5, int(self.intraday_bearish_lookback or 8))
            chron = list(reversed(candles))  # oldest -> newest
            recent = chron[-lookback:] if len(chron) >= lookback else chron
            opens = [float(c[1]) for c in recent if c and len(c) > 1]
            closes = [float(c[4]) for c in recent if c and len(c) > 4]
            if len(opens) < 5 or len(closes) < 5:
                return False
            return self._intraday_bearish_pattern_from_ohlc(opens, closes, price, lookback)
        except Exception:
            return False

    def _intraday_volume_ok(self, cur, symbol: str) -> bool:
        try:
            cur.execute(
                self._q("SELECT volume FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT 1"),
                (symbol, "1m"),
            )
            row = cur.fetchone()
            cur_vol = float(row[0]) if row and row[0] is not None else None
            cur.execute(
                self._q(
                    "SELECT AVG(volume) FROM (SELECT volume FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT ?) sub"
                ),
                (symbol, "1m", 20),
            )
            row = cur.fetchone()
            avg_vol = float(row[0]) if row and row[0] is not None else None
            if cur_vol is None or avg_vol is None or avg_vol <= 0:
                return False
            return cur_vol >= avg_vol * self.intraday_volume_min_mult
        except Exception:
            return False

    def _intraday_trades_ok(self, cur, symbol: str, limits: dict) -> bool:
        """Require a minimum number of trades in the last 1 minute (orderflow table)."""
        try:
            if not limits.get("trades_filter_enabled"):
                return True
            min_trades = int(limits.get("min_trades_1m") or 0)
            if min_trades <= 0:
                return True
            cur.execute(
                self._q(
                    "SELECT buy_count, sell_count, ts FROM orderflow WHERE symbol = ? ORDER BY ts DESC LIMIT 1"
                ),
                (symbol,),
            )
            row = cur.fetchone()
            if not row:
                return False
            buy_cnt = int(row[0] or 0)
            sell_cnt = int(row[1] or 0)
            ts = int(row[2] or 0)
            now = int(time.time() * 1000)
            if ts <= 0 or (now - ts) > 70000:
                return False
            total = buy_cnt + sell_cnt
            return total >= min_trades
        except Exception:
            return False

    def _intraday_cooldown_ok(self, symbol: str) -> bool:
        """Block re-entry on the same symbol until cooldown expires."""
        try:
            last = self._last_exit_ts.get(symbol)
            if not last:
                return True
            return (time.time() - float(last)) >= float(self.intraday_cooldown_sec or 0)
        except Exception:
            return True

    def _intraday_loss_exit(self, cur, pos: dict, price: float) -> bool:
        """Exit early if price drifts down with a confirmed bearish pattern."""
        try:
            entry = float(pos.get("entry_price") or 0)
            if entry <= 0 or price <= 0:
                return False
            loss_pct = (entry - price) / entry * 100.0
            if loss_pct < float(self.intraday_loss_exit_pct or 0):
                return False
            n = max(5, int(self.intraday_loss_exit_candles or 5))
            cur.execute(
                self._q(
                    "SELECT open, close FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT ?"
                ),
                (pos["symbol"], "1m", n),
            )
            rows = cur.fetchall()
            opens = [float(r[0]) for r in rows if r and r[0] is not None]
            closes = [float(r[1]) for r in rows if r and r[1] is not None]
            if len(closes) < 5 or len(opens) < 5:
                return False
            opens = list(reversed(opens))
            closes = list(reversed(closes))
            lookback = min(n, len(closes))
            red = 0
            for i in range(-lookback, 0):
                if closes[i] < opens[i]:
                    red += 1
            red_ratio = red / float(lookback) if lookback > 0 else 0.0

            ema_fast = self._ema(closes, 9)
            ema_slow = self._ema(closes, 25)
            trend_bearish = False
            if ema_fast is not None and ema_slow is not None:
                trend_bearish = ema_fast < ema_slow
            # Short-term slope (last 3 vs previous 3)
            slope_bearish = False
            if len(closes) >= 6:
                recent = sum(closes[-3:]) / 3.0
                prev = sum(closes[-6:-3]) / 3.0
                if prev > 0:
                    slope_bearish = (recent - prev) / prev < 0
            price_below_slow = True
            if ema_slow is not None:
                price_below_slow = price <= ema_slow

            # Trade imbalance check (last 1m)
            cur.execute(
                self._q("SELECT buy_count, sell_count, ts FROM orderflow WHERE symbol = ? ORDER BY ts DESC LIMIT 1"),
                (pos["symbol"],),
            )
            row = cur.fetchone()
            trades_bearish = False
            if row:
                buy_cnt = float(row[0] or 0)
                sell_cnt = float(row[1] or 0)
                ts = int(row[2] or 0)
                now = int(time.time() * 1000)
                if ts > 0 and (now - ts) <= 70000 and buy_cnt >= 0 and sell_cnt >= 0:
                    if sell_cnt > buy_cnt * float(self.intraday_loss_sell_ratio or 1.2):
                        trades_bearish = True
            pattern_bearish = trend_bearish and slope_bearish and price_below_slow
            red_ok = red_ratio >= float(self.intraday_loss_red_ratio or 0.6)
            return pattern_bearish and (trades_bearish or red_ok)
        except Exception:
            return False

    def _intraday_trend_exit(self, cur, pos: dict, price: float) -> bool:
        """Exit sooner when a clear bearish pattern persists (smaller loss than stop)."""
        try:
            entry = float(pos.get("entry_price") or 0)
            if entry <= 0 or price <= 0:
                return False
            loss_pct = (entry - price) / entry * 100.0
            if loss_pct < float(self.intraday_trend_exit_pct or 0):
                return False
            lookback = max(5, int(self.intraday_loss_exit_candles or 5))
            cur.execute(
                self._q(
                    "SELECT open, close FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT ?"
                ),
                (pos["symbol"], "1m", lookback),
            )
            rows = cur.fetchall()
            if not rows:
                return False
            opens = [float(r[0]) for r in rows if r and r[0] is not None]
            closes = [float(r[1]) for r in rows if r and r[1] is not None]
            if len(opens) < 5 or len(closes) < 5:
                return False
            opens = list(reversed(opens))
            closes = list(reversed(closes))
            return self._intraday_bearish_pattern_from_ohlc(opens, closes, price, lookback)
        except Exception:
            return False

    def _intraday_recent_pump_ok(self, cur, symbol: str) -> bool:
        """Skip coins that pumped too much in last 5/30 minutes."""
        try:
            cur.execute(
                self._q("SELECT close FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT 30"),
                (symbol, "1m"),
            )
            rows = cur.fetchall()
            if not rows or len(rows) < 30:
                return True  # not enough data, don't block
            closes = [float(r[0]) for r in rows if r and r[0] is not None]
            if len(closes) < 30:
                return True
            last = closes[0]
            close_5 = closes[4]
            close_30 = closes[29]
            if close_5 <= 0 or close_30 <= 0:
                return True
            change_5 = (last - close_5) / close_5 * 100.0
            change_30 = (last - close_30) / close_30 * 100.0
            if self.intraday_pump_5m_pct > 0 and change_5 >= self.intraday_pump_5m_pct:
                return False
            if self.intraday_pump_30m_pct > 0 and change_30 >= self.intraday_pump_30m_pct:
                return False
            return True
        except Exception:
            return True

    def _intraday_recent_pump_ok_live(self, closes: List[float]) -> bool:
        try:
            if not closes or len(closes) < 30:
                return False
            last = closes[0]
            close_5 = closes[4]
            close_30 = closes[29]
            if close_5 <= 0 or close_30 <= 0:
                return False
            change_5 = (last - close_5) / close_5 * 100.0
            change_30 = (last - close_30) / close_30 * 100.0
            if self.intraday_pump_5m_pct > 0 and change_5 >= self.intraday_pump_5m_pct:
                return False
            if self.intraday_pump_30m_pct > 0 and change_30 >= self.intraday_pump_30m_pct:
                return False
            return True
        except Exception:
            return False

    def _intraday_pump_pullback_ok(self, cur, symbol: str, limits: dict) -> bool:
        """If a coin pumped, require a pullback + bounce before allowing a buy."""
        try:
            cur.execute(
                self._q("SELECT close, high, low FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT 30"),
                (symbol, "1m"),
            )
            rows = cur.fetchall()
            if not rows or len(rows) < 30:
                return True
            closes = [float(r[0]) for r in rows if r and r[0] is not None]
            highs = [float(r[1]) for r in rows if r and r[1] is not None]
            lows = [float(r[2]) for r in rows if r and r[2] is not None]
            if len(closes) < 30 or len(highs) < 30 or len(lows) < 30:
                return True
            current = closes[0]
            max_high = max(highs)
            min_low = min(lows)
            if min_low <= 0:
                return True
            pump_threshold = float(limits.get("pump_threshold_pct") or 0.0)
            if pump_threshold <= 0:
                return True
            pump_pct = (max_high - min_low) / min_low * 100.0
            if pump_pct < pump_threshold:
                return True

            avoid_top = float(limits.get("avoid_top_pct") or 0.0)
            if avoid_top > 0 and max_high > 0:
                if current >= max_high * (1 - avoid_top / 100.0):
                    return False

            atr = self._latest_atr(cur, symbol)
            pullback_abs = 0.0
            pullback_atr_mult = float(limits.get("pullback_atr_mult") or 0.0)
            pullback_range_mult = float(limits.get("pullback_range_mult") or 0.0)
            if atr is not None and pullback_atr_mult > 0:
                pullback_abs = max(pullback_abs, atr * pullback_atr_mult)
            range_val = max_high - min_low
            if range_val > 0 and pullback_range_mult > 0:
                pullback_abs = max(pullback_abs, range_val * pullback_range_mult)
            if pullback_abs > 0 and current > (max_high - pullback_abs):
                return False

            bounce_pct = float(limits.get("bounce_pct") or 0.0)
            if bounce_pct > 0:
                lookback = int(limits.get("bounce_lookback") or 0)
                if lookback <= 0:
                    lookback = 5
                recent_lows = lows[:lookback] if len(lows) >= lookback else lows
                if not recent_lows:
                    return True
                recent_low = min(recent_lows)
                if recent_low <= 0:
                    return True
                if current < recent_low * (1 + bounce_pct / 100.0):
                    return False
            return True
        except Exception:
            return True

    def _intraday_pump_pullback_ok_live(self, candles: List[list], limits: dict, current_price: Optional[float]) -> bool:
        """Live version using fresh Binance candles and optional current price."""
        try:
            if not candles or len(candles) < 30:
                return False
            closes = [float(c[4]) for c in candles if c and len(c) > 4]
            highs = [float(c[2]) for c in candles if c and len(c) > 2]
            lows = [float(c[3]) for c in candles if c and len(c) > 3]
            if len(closes) < 30 or len(highs) < 30 or len(lows) < 30:
                return False
            current = float(current_price) if current_price else closes[0]
            max_high = max(highs)
            min_low = min(lows)
            if min_low <= 0:
                return False
            pump_threshold = float(limits.get("pump_threshold_pct") or 0.0)
            if pump_threshold <= 0:
                return True
            pump_pct = (max_high - min_low) / min_low * 100.0
            if pump_pct < pump_threshold:
                return True

            avoid_top = float(limits.get("avoid_top_pct") or 0.0)
            if avoid_top > 0 and max_high > 0:
                if current >= max_high * (1 - avoid_top / 100.0):
                    return False

            # Compute a simple ATR from recent live candles (last 14)
            atr = None
            try:
                chron = list(reversed(candles))  # oldest -> newest
                recent = chron[-15:] if len(chron) >= 15 else chron
                if len(recent) >= 2:
                    trs = []
                    prev_close = float(recent[0][4])
                    for c in recent[1:]:
                        high = float(c[2])
                        low = float(c[3])
                        close = float(c[4])
                        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
                        trs.append(tr)
                        prev_close = close
                    if trs:
                        atr = sum(trs) / len(trs)
            except Exception:
                atr = None
            pullback_abs = 0.0
            pullback_atr_mult = float(limits.get("pullback_atr_mult") or 0.0)
            pullback_range_mult = float(limits.get("pullback_range_mult") or 0.0)
            if atr is not None and pullback_atr_mult > 0:
                pullback_abs = max(pullback_abs, atr * pullback_atr_mult)
            range_val = max_high - min_low
            if range_val > 0 and pullback_range_mult > 0:
                pullback_abs = max(pullback_abs, range_val * pullback_range_mult)
            if pullback_abs > 0 and current > (max_high - pullback_abs):
                return False

            bounce_pct = float(limits.get("bounce_pct") or 0.0)
            if bounce_pct > 0:
                lookback = int(limits.get("bounce_lookback") or 0)
                if lookback <= 0:
                    lookback = 5
                recent_lows = lows[:lookback] if len(lows) >= lookback else lows
                if not recent_lows:
                    return False
                recent_low = min(recent_lows)
                if recent_low <= 0:
                    return False
                if current < recent_low * (1 + bounce_pct / 100.0):
                    return False
            return True
        except Exception:
            return False

    def _fetch_live_candles(self, symbol: str, limit: int = 30) -> Optional[List[list]]:
        data = self._binance_public_request(
            "/api/v3/klines",
            {"symbol": symbol.upper(), "interval": "1m", "limit": limit},
        )
        if not isinstance(data, list):
            return None
        # Binance returns oldest->newest; reverse to keep newest first like DB queries
        return list(reversed(data))

    def _fetch_live_price(self, symbol: str) -> Optional[float]:
        data = self._binance_public_request("/api/v3/ticker/price", {"symbol": symbol.upper()})
        try:
            return float(data.get("price")) if isinstance(data, dict) else None
        except Exception:
            return None

    def _live_intraday_entry_ok(self, symbol: str, limits: dict) -> bool:
        candles = self._fetch_live_candles(symbol, limit=30)
        if not candles or len(candles) < 30:
            logging.warning(f"Live entry blocked for {symbol}: insufficient live candles")
            return False
        live_price = self._fetch_live_price(symbol)
        if live_price is None:
            logging.warning(f"Live entry blocked for {symbol}: missing live price")
            return False
        closes = [float(c[4]) for c in candles if c and len(c) > 4]
        if not closes or len(closes) < 30:
            logging.warning(f"Live entry blocked for {symbol}: insufficient close data")
            return False
        # Live trend filter using fresh EMA values
        ema25 = self._ema(closes, 25)
        ema7 = self._ema(closes, 7)
        if self.intraday_trend_filter != "none":
            if ema25 is None or ema25 <= 0:
                logging.warning(f"Live entry blocked for {symbol}: missing EMA25")
                return False
            if self.intraday_trend_filter == "ema25" and live_price < ema25:
                logging.info(f"Live entry blocked for {symbol}: price below EMA25")
                return False
            if self.intraday_trend_filter == "ema7_ema25":
                if ema7 is None or ema7 <= ema25:
                    logging.info(f"Live entry blocked for {symbol}: EMA7 <= EMA25")
                    return False
            # Optional slope check
            lookback = max(2, int(self.intraday_trend_slope_lookback or 0))
            slope_min = float(self.intraday_trend_slope_min or 0.0)
            if lookback > 1 and slope_min >= 0 and len(closes) > lookback:
                ema_prev = self._ema(closes[:-lookback], 25)
                if ema_prev and ema_prev > 0:
                    slope_pct = (ema25 - ema_prev) / ema_prev * 100.0
                    if slope_pct < slope_min:
                        logging.info(f"Live entry blocked for {symbol}: EMA25 slope {slope_pct:.3f}% < {slope_min}%")
                        return False
        # Bearish pattern block using live candles
        if self._intraday_bearish_block_live(candles, live_price):
            logging.info(f"Live entry blocked for {symbol}: bearish pattern (live)")
            return False
        # Always apply recent pump block using live candles
        if not self._intraday_recent_pump_ok_live(closes):
            logging.info(f"Live entry blocked for {symbol}: recent pump too strong")
            return False
        # If pump/pullback is enabled, validate using live candles + live price
        if limits.get("pump_pullback_enabled"):
            if not self._intraday_pump_pullback_ok_live(candles, limits, live_price):
                logging.info(f"Live entry blocked for {symbol}: pump/pullback not satisfied (live)")
                return False
        return True

    def _seed_intraday_state(self, cur, pg: bool, reset: bool):
        if reset:
            cur.execute("DELETE FROM intraday_trading")
        cur.execute("SELECT COUNT(1) FROM intraday_trading")
        count = cur.fetchone()[0]
        if int(count or 0) > 0:
            return
        cur.execute("SELECT symbol, latest_price FROM coin_monitor")
        rows = cur.fetchall()
        if not rows:
            return
        data = []
        for symbol, price in rows:
            if price is None or float(price) <= 0:
                continue
            price = float(price)
            data.append(
                (
                    symbol,
                    price,
                    price,
                    price,
                    price * (1 + self.intraday_margin3_pct / 100.0),
                    price * (1 + self.intraday_margin5_pct / 100.0),
                    price * (1 + self.intraday_margin10_pct / 100.0),
                    price * (1 + self.intraday_margin20_pct / 100.0),
                    None,
                    False,
                    False,
                    False,
                    False,
                    "0",
                )
            )
        cur.executemany(
            self._q(
                """
                INSERT INTO intraday_trading(
                    symbol, initial_price, high_price, last_price,
                    margin3, margin5, margin10, margin20,
                    purchase_price, mar3, mar5, mar10, mar20, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            ),
            data,
        )

    def _open_intraday_position(self, cur, symbol: str, price: float, amount: float, profit_pct: float, stop_pct: float, limits: dict) -> bool:
        cash = self._get_cash(cur)
        if cash <= 0 or amount <= 0 or price <= 0:
            return False
        if amount > cash:
            return False
        qty = amount / price
        if qty <= 0:
            return False
        tp_order_id = None
        tp_order_status = None
        if not self.intraday_paper:
            if self.intraday_live_confirm and not self._live_intraday_entry_ok(symbol, limits):
                return False
            order = self._place_spot_order(symbol, "BUY", quote_qty=amount)
            if not order:
                logging.error(f"Live intraday buy failed for {symbol}")
                return False
            fill = self._extract_order_fill(order)
            if fill["qty"] <= 0:
                logging.error(f"Live intraday buy returned 0 qty for {symbol}")
                return False
            qty = fill["qty"]
            if fill["price"]:
                price = fill["price"]
            if fill["quote"] > 0:
                amount = fill["quote"]
            adjusted_qty = self._apply_sell_qty_buffer(symbol, qty)
            if adjusted_qty > 0:
                qty = adjusted_qty
            else:
                logging.error(f"Unable to determine sellable qty for {symbol} after BUY; keeping filled qty.")
        stop_price = None
        if not self.intraday_disable_stop and stop_pct and stop_pct > 0:
            stop_price = price * (1 - stop_pct / 100.0)
        # Add fee buffer so a "0.5%" target can still net profit after fees.
        effective_profit = float(profit_pct or 0) + float(self.intraday_fee_buffer_pct or 0)
        raw_tp = price * (1 + effective_profit / 100.0)
        tp_price = raw_tp
        filters = self._get_exchange_filters(symbol)
        if filters and filters.get("tickSize"):
            tp_price = self._ceil_to_step(tp_price, filters["tickSize"])
        if tp_price < raw_tp:
            tp_price = raw_tp
        if not self.intraday_paper:
            tp_order = self._place_spot_order(
                symbol,
                "SELL",
                quantity=qty,
                order_type="LIMIT",
                price=tp_price,
                time_in_force="GTC",
            )
            if tp_order:
                tp_order_id = str(tp_order.get("orderId") or "")
                tp_order_status = tp_order.get("status") or "NEW"
                logging.info(f"Placed intraday TP limit for {symbol}: id={tp_order_id} price={tp_price} qty={qty}")
            else:
                logging.error(f"Failed to place intraday TP limit for {symbol} at {tp_price}")
        new_cash = cash - amount
        self._set_cash(cur, new_cash)
        cur.execute(
            self._q(
                """
                INSERT INTO paper_positions(symbol, qty, entry_price, stop_price, take_profit_price, entry_type, r_value, breakeven_set, partial_taken, trailing_stop, tp_order_id, tp_order_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """
            ),
            (
                symbol,
                qty,
                price,
                stop_price,
                tp_price,
                "intraday",
                abs(price - stop_price) if stop_price is not None else 0,
                0,
                0,
                stop_price,
                tp_order_id,
                tp_order_status,
            ),
        )
        cur.execute(
            self._q("INSERT INTO trade_logs(symbol, side, qty, price, pnl, balance_after, reason, status) VALUES (?, 'BUY', ?, ?, 0, ?, ?, 'OPEN')"),
            (symbol, qty, price, new_cash, "intraday_entry"),
        )
        logging.info(
            f"Intraday BUY {symbol} qty={qty:.6f} price={price} tp={tp_price} stop={stop_price} profit%={profit_pct} eff%={effective_profit}"
        )
        return True

    def _intraday_open_positions(self, cur) -> List[dict]:
        cur.execute(
            self._q(
                """
                SELECT id, symbol, qty, entry_price, stop_price, take_profit_price, entry_type, tp_order_id, tp_order_status
                FROM paper_positions
                WHERE status = 'OPEN' AND entry_type = 'intraday'
                """
            )
        )
        rows = cur.fetchall()
        cols = ["id", "symbol", "qty", "entry_price", "stop_price", "take_profit_price", "entry_type", "tp_order_id", "tp_order_status"]
        return [dict(zip(cols, r)) for r in rows]

    def _manage_intraday_positions(self, cur) -> bool:
        dirty = False
        positions = self._intraday_open_positions(cur)
        for pos in positions:
            price = self._latest_price(cur, pos["symbol"])
            if price is None:
                continue
            tp_order_active = False
            if not self.intraday_paper and pos.get("tp_order_id"):
                order = self._get_spot_order(pos["symbol"], pos["tp_order_id"])
                if order:
                    status = order.get("status")
                    if status and status != pos.get("tp_order_status"):
                        cur.execute(self._q("UPDATE paper_positions SET tp_order_status = ? WHERE id = ?"), (status, pos["id"]))
                        dirty = True
                    if status == "FILLED":
                        fill = self._extract_order_fill(order)
                        exec_qty = fill["qty"] if fill["qty"] > 0 else float(pos["qty"])
                        exec_price = fill["price"] or float(pos.get("take_profit_price") or price)
                        self._finalize_position(cur, pos, exec_qty, exec_price, "intraday_tp_filled")
                        dirty = True
                        continue
                    if status in ("CANCELED", "REJECTED", "EXPIRED"):
                        cur.execute(self._q("UPDATE paper_positions SET tp_order_id = NULL, tp_order_status = NULL WHERE id = ?"), (pos["id"],))
                        dirty = True
                    else:
                        tp_order_active = True
            # Downtrend loss exit (cancel TP and market sell)
            if self._intraday_trend_exit(cur, pos, price):
                if not self.intraday_paper and pos.get("tp_order_id"):
                    cancel = self._cancel_spot_order(pos["symbol"], pos["tp_order_id"])
                    if cancel:
                        cur.execute(self._q("UPDATE paper_positions SET tp_order_id = NULL, tp_order_status = ? WHERE id = ?"), ("CANCELED", pos["id"]))
                        dirty = True
                self._close_position(cur, pos, price, "intraday_trend_exit")
                dirty = True
                continue
            if self._intraday_loss_exit(cur, pos, price):
                if not self.intraday_paper and pos.get("tp_order_id"):
                    cancel = self._cancel_spot_order(pos["symbol"], pos["tp_order_id"])
                    if cancel:
                        cur.execute(self._q("UPDATE paper_positions SET tp_order_id = NULL, tp_order_status = ? WHERE id = ?"), ("CANCELED", pos["id"]))
                        dirty = True
                self._close_position(cur, pos, price, "intraday_loss_exit")
                dirty = True
                continue
            if not self.intraday_disable_stop and pos["stop_price"] is not None:
                if price <= float(pos["stop_price"]):
                    if not self.intraday_paper and pos.get("tp_order_id"):
                        cancel = self._cancel_spot_order(pos["symbol"], pos["tp_order_id"])
                        if cancel:
                            cur.execute(self._q("UPDATE paper_positions SET tp_order_id = NULL, tp_order_status = ? WHERE id = ?"), ("CANCELED", pos["id"]))
                            dirty = True
                    self._close_position(cur, pos, price, "intraday_stop")
                    dirty = True
                    continue
            if pos["take_profit_price"] is not None and price >= float(pos["take_profit_price"]) and not tp_order_active:
                self._close_position(cur, pos, price, "intraday_tp")
                dirty = True
                continue
        return dirty

    def _run_intraday_loop(self):
        seeded = False
        while not self._intraday_stop.is_set():
            if not self.intraday_enabled:
                time.sleep(self.intraday_loop_sec)
                continue
            try:
                conn, cur = get_database_connection()
                pg = is_pg(cur)
                # Ensure tables exist + defaults
                self._ensure_intraday_limits(cur, pg)
                if self.intraday_reset_on_start and not seeded:
                    self._seed_intraday_state(cur, pg, reset=True)
                    seeded = True
                else:
                    self._seed_intraday_state(cur, pg, reset=False)

                limits = self._load_intraday_limits(cur)
                if not limits:
                    try:
                        cur.close(); conn.close()
                    except Exception:
                        pass
                    time.sleep(self.intraday_loop_sec)
                    continue

                raw_trades = int(limits.get("number_of_trades") or 0)
                unlimited_trades = raw_trades <= 0
                trades_count = max(1, raw_trades)
                per_trade_amount = float(limits.get("amount") or 0.0) / (1 if unlimited_trades else trades_count)

                # Manage open intraday positions
                if self._manage_intraday_positions(cur):
                    conn.commit()

                # Enforce max open positions = number_of_trades
                open_positions = self._intraday_open_positions(cur)
                open_count = len(open_positions)
                open_symbols = {p.get("symbol") for p in open_positions if p.get("symbol")}
                if not unlimited_trades and open_count >= trades_count:
                    try:
                        cur.close(); conn.close()
                    except Exception:
                        pass
                    time.sleep(self.intraday_loop_sec)
                    continue

                counts = self._intraday_counts(cur)
                # Build price map once per loop
                cur.execute("SELECT symbol, latest_price FROM coin_monitor")
                price_map = {row[0]: float(row[1]) for row in cur.fetchall() if row and row[1] is not None}

                cur.execute(
                    """
                    SELECT symbol, margin3, margin5, margin10, margin20, mar3, mar5, mar10, mar20
                    FROM intraday_trading
                    WHERE status != '1' AND initial_price > 0
                    """
                )
                rows = cur.fetchall()
                for row in rows:
                    if open_count >= trades_count:
                        break
                    symbol, margin3, margin5, margin10, margin20, mar3, mar5, mar10, mar20 = row
                    price = price_map.get(symbol)
                    if price is None:
                        continue
                    if symbol in open_symbols:
                        continue
                    if not self._intraday_cooldown_ok(symbol):
                        continue
                    if not self._intraday_trend_ok(cur, symbol, price):
                        continue
                    if self._intraday_bearish_block(cur, symbol, price):
                        continue
                    if not self._intraday_volume_ok(cur, symbol):
                        continue
                    if not self._intraday_trades_ok(cur, symbol, limits):
                        continue
                    if limits.get("pump_pullback_enabled"):
                        if not self._intraday_pump_pullback_ok(cur, symbol, limits):
                            continue
                    else:
                        if not self._intraday_recent_pump_ok(cur, symbol):
                            continue

                    if counts["sum_mar3"] < limits["margin3count"]:
                        if price >= float(margin3 or 0) and not mar3:
                            if self._open_intraday_position(cur, symbol, price, per_trade_amount, limits["profit"], limits["stoploss"], limits):
                                cur.execute(self._q("UPDATE intraday_trading SET mar3 = TRUE, status = '1', purchase_price = ? WHERE symbol = ?"), (price, symbol))
                                counts["sum_mar3"] += 1
                                open_count += 1
                            continue

                    if counts["sum_mar5"] < limits["margin5count"]:
                        if price >= float(margin5 or 0) and not mar5:
                            if self._open_intraday_position(cur, symbol, price, per_trade_amount, limits["profit"], limits["stoploss"], limits):
                                cur.execute(self._q("UPDATE intraday_trading SET mar5 = TRUE, status = '1', purchase_price = ? WHERE symbol = ?"), (price, symbol))
                                counts["sum_mar5"] += 1
                                open_count += 1
                            continue

                    if counts["sum_mar10"] < limits["margin10count"]:
                        if price >= float(margin10 or 0) and not mar10:
                            if self._open_intraday_position(cur, symbol, price, per_trade_amount, limits["profit"], limits["stoploss"], limits):
                                cur.execute(self._q("UPDATE intraday_trading SET mar10 = TRUE, status = '1', purchase_price = ? WHERE symbol = ?"), (price, symbol))
                                counts["sum_mar10"] += 1
                                open_count += 1
                            continue

                    if counts["sum_mar20"] < limits["margin20count"]:
                        if price >= float(margin20 or 0) and not mar20:
                            if self._open_intraday_position(cur, symbol, price, per_trade_amount, limits["profit"], limits["stoploss"], limits):
                                cur.execute(self._q("UPDATE intraday_trading SET mar20 = TRUE, status = '1', purchase_price = ? WHERE symbol = ?"), (price, symbol))
                                counts["sum_mar20"] += 1
                                open_count += 1
                            continue

                conn.commit()
                try:
                    cur.close(); conn.close()
                except Exception:
                    pass
            except Exception as e:
                logging.error(f"Intraday loop error: {e}")
            time.sleep(self.intraday_loop_sec)

    def list_trades(self, limit: int = 200) -> List[dict]:
        conn = None
        cur = None
        try:
            limit = int(limit)
            conn, cur = get_database_connection()
            # Avoid driver-specific bind for LIMIT by inlining validated integer
            sql = "SELECT id, symbol, side, qty, price, pnl, balance_after, reason, status, created_at FROM trade_logs ORDER BY id DESC LIMIT " + str(limit)
            cur.execute(sql)
            rows = cur.fetchall()
            cols = ["id","symbol","side","qty","price","pnl","balance_after","reason","status","created_at"]
            return [dict(zip(cols, r)) for r in rows]
        except Exception as e:
            logging.error(f"Error fetching trade logs: {e}")
            return []
        finally:
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass

    def portfolio(self) -> dict:
        """Return current paper portfolio snapshot (cash, open positions, unrealized pnl)."""
        conn = None
        cur = None
        try:
            conn, cur = get_database_connection()
            open_pos = self._open_positions(cur)
            cash = self._get_cash(cur)
            unrealized = 0.0
            total_notional = 0.0
            positions_out = []
            for p in open_pos.values():
                entry = float(p["entry_price"])
                qty = float(p["qty"])
                price = self._latest_price(cur, p["symbol"]) or entry
                notional = entry * qty
                pnl = (price - entry) * qty
                unrealized += pnl
                total_notional += notional
                positions_out.append({
                    "symbol": p["symbol"],
                    "qty": qty,
                    "entry_price": entry,
                    "current_price": price,
                    "notional": notional,
                    "stop_price": p["stop_price"],
                    "take_profit_price": p["take_profit_price"],
                    "unrealized_pnl": pnl,
                    "unrealized_pct": (pnl / notional * 100.0) if notional > 0 else 0.0,
                    "rr_target": p.get("rr_target"),
                    "breakeven_set": p.get("breakeven_set"),
                    "partial_taken": p.get("partial_taken"),
                })
            unrealized_pct = (unrealized / total_notional * 100.0) if total_notional > 0 else 0.0
            return {
                "cash": cash,
                "open_positions": positions_out,
                "unrealized_pnl": unrealized,
                "total_notional": total_notional,
                "unrealized_pct": unrealized_pct,
            }
        except Exception as e:
            logging.error(f"Error building portfolio: {e}")
            return {"cash": self.initial_cash, "open_positions": [], "unrealized_pnl": 0.0}
        finally:
            try:
                if cur:
                    cur.close()
                if conn:
                    conn.close()
            except Exception:
                pass

    # Simple mode does not track cash/positions; keep helpers no-op
    def _get_cash(self, cur) -> float:
        cur.execute(self._q("SELECT cash FROM paper_portfolio ORDER BY id DESC LIMIT 1"))
        row = cur.fetchone()
        cash_val = float(row[0]) if row and row[0] is not None else None
        if cash_val is None:
            cash_val = self.initial_cash
            cur.execute(self._q("DELETE FROM paper_portfolio"))
            cur.execute(self._q("INSERT INTO paper_portfolio(cash) VALUES (?)"), (cash_val,))
            try:
                cur.connection.commit()
            except Exception:
                pass
        return cash_val

    def _set_cash(self, cur, cash: float):
        cur.execute("SELECT id FROM paper_portfolio ORDER BY id DESC LIMIT 1")
        row = cur.fetchone()
        if row:
            cur.execute(self._q("UPDATE paper_portfolio SET cash = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?"), (cash, row[0]))
        else:
            cur.execute(self._q("INSERT INTO paper_portfolio(cash) VALUES (?)"), (cash,))
        try:
            cur.connection.commit()
        except Exception:
            pass

    def _open_positions(self, cur) -> Dict[str, dict]:
        cur.execute(self._q("SELECT id, symbol, qty, entry_price, stop_price, take_profit_price, status, risk_perc, rr_target, entry_type, r_value, breakeven_set, partial_taken, trailing_stop FROM paper_positions WHERE status = 'OPEN'"))
        rows = cur.fetchall()
        cols = ["id","symbol","qty","entry_price","stop_price","take_profit_price","status","risk_perc","rr_target","entry_type","r_value","breakeven_set","partial_taken","trailing_stop"]
        out: Dict[str, dict] = {}
        for r in rows:
            d = dict(zip(cols, r))
            out[d["symbol"]] = d
            # If ladder state missing, initialize baseline
            if self.coin_brain_ladder_enabled and d["symbol"] not in self._ladder_state:
                self._ladder_state[d["symbol"]] = {"fills": 1, "last_fill_price": d["entry_price"]}
        return out

    def _run_loop(self):
        while not self._stop.is_set():
            if not self._enabled:
                time.sleep(self.interval_sec)
                continue
            if not self.hybrid_enabled:
                time.sleep(self.interval_sec)
                continue
            try:
                now = time.time()
                conn, cur = get_database_connection()
                symbols = get_all_coins()
                if not symbols:
                    try:
                        cur.close(); conn.close()
                    except Exception:
                        pass
                    time.sleep(self.interval_sec)
                    continue

                daily_pnl = self._daily_pnl(cur)
                daily_cap = -abs(self.daily_loss_cap_pct / 100.0 * self.initial_cash)

                positions = self._open_positions(cur)

                for sym in symbols:
                    price = self._latest_price(cur, sym)
                    if price is None or price <= 0:
                        continue

                    # Manage open positions first
                    if sym in positions:
                        dirty = self._manage_position(cur, positions[sym], price)
                        if dirty:
                            conn.commit()
                        continue

                    # Risk guardrail: daily loss cap
                    if daily_pnl <= daily_cap:
                        logging.info(f"Daily loss cap reached, skipping new trades for {sym}")
                        continue

                    last_ts = self._last_trade_ts.get(sym, 0)
                    if now - last_ts < self.cooldown_sec:
                        continue

                    # Try AI-driven intention first
                    ai = self._recent_ai_intention(cur, sym)
                    if ai and ai.get("confidence", 0.0) >= self.min_confidence and ai["intention"] == "BUY":
                        self._open_position(cur, sym, price, direction="LONG")
                        self._last_trade_ts[sym] = now
                        conn.commit()
                        continue
                    if ai and ai.get("confidence", 0.0) >= self.min_confidence and ai["intention"] == "SELL":
                        # No shorting; just skip entry but note intention
                        self._last_trade_ts[sym] = now
                        continue

                    # Fallback demo midpoint logic to open a risk-managed long on dips
                    cur.execute(self._q("SELECT low_price, high_price FROM coin_monitor WHERE symbol = ?"), (sym,))
                    row = cur.fetchone()
                    lowp, highp = row if row else (None, None)
                    midpoint = (float(lowp) + float(highp)) / 2.0 if lowp is not None and highp is not None else float(price)
                    diff = 0.0
                    try:
                        diff = (float(price) - float(midpoint)) / float(midpoint)
                    except Exception:
                        diff = 0.0

                    if diff <= -0.007:  # ~-0.7% dip: open long with stop sizing
                        self._open_position(cur, sym, price, direction="LONG")
                        self._last_trade_ts[sym] = now
                        conn.commit()
                        continue
                try:
                    cur.close(); conn.close()
                except Exception:
                    pass
            except Exception as e:
                logging.error(f"Trading loop error: {e}")
            finally:
                time.sleep(self.interval_sec)


# Global singleton instance used by FastAPI app
trading_manager = TradingManager()
