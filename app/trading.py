import threading
import time
import logging
import os
import hmac
import hashlib
import urllib.parse
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
            "pullback_atr_mult": float(os.getenv("INTRADAY_PULLBACK_ATR_MULT", "1.0")),
            "pullback_range_mult": float(os.getenv("INTRADAY_PULLBACK_RANGE_MULT", "0.4")),
            "bounce_pct": float(os.getenv("INTRADAY_BOUNCE_PCT", "0.5")),
            "bounce_lookback": int(os.getenv("INTRADAY_BOUNCE_LOOKBACK", "5")),
        }
        self.intraday_sync_limits = os.getenv("INTRADAY_SYNC_LIMITS", "true").lower() == "true"
        self.intraday_reset_on_start = os.getenv("INTRADAY_RESET_ON_START", "true").lower() == "true"
        self.intraday_disable_stop = os.getenv("INTRADAY_DISABLE_STOP", "false").lower() == "true"
        self.intraday_trend_filter = os.getenv("INTRADAY_TREND_FILTER", "ema25").lower()
        self.intraday_volume_min_mult = float(os.getenv("INTRADAY_VOL_MIN_MULT", "0.8"))
        self.intraday_pump_5m_pct = float(os.getenv("INTRADAY_PUMP_5M_PCT", "1.5"))
        self.intraday_pump_30m_pct = float(os.getenv("INTRADAY_PUMP_30M_PCT", "3.0"))
        # Binance live trading settings
        self.binance_api_key: Optional[str] = None
        self.binance_api_secret: Optional[str] = None
        self.binance_base_url = os.getenv("BINANCE_BASE_URL", "https://api.binance.com")
        self.binance_recv_window = int(os.getenv("BINANCE_RECV_WINDOW", "5000"))
        self.binance_timeout_sec = float(os.getenv("BINANCE_TIMEOUT_SEC", "10"))
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
            if method.upper() == "POST":
                resp = requests.post(url, headers=headers, timeout=self.binance_timeout_sec)
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

    def _place_spot_order(
        self,
        symbol: str,
        side: str,
        quantity: Optional[float] = None,
        quote_qty: Optional[float] = None,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
    ):
        params = {
            "symbol": symbol.upper(),
            "side": side.upper(),
            "type": "MARKET",
            "newOrderRespType": "FULL",
        }
        if side.upper() == "BUY":
            if quote_qty is None or quote_qty <= 0:
                logging.error("Binance BUY requires quoteOrderQty.")
                return None
            params["quoteOrderQty"] = self._fmt_number(quote_qty)
        else:
            if quantity is None or quantity <= 0:
                logging.error("Binance SELL requires quantity.")
                return None
            params["quantity"] = self._fmt_number(quantity)
        return self._binance_signed_request("POST", "/api/v3/order", params, api_key, api_secret)

    def _extract_order_fill(self, order: dict) -> dict:
        qty = float(order.get("executedQty") or 0)
        quote = float(order.get("cummulativeQuoteQty") or 0)
        price = (quote / qty) if qty > 0 else None
        return {"qty": qty, "quote": quote, "price": price}

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
                        bounce_pct, bounce_lookback
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        bounce_pct = ?, bounce_lookback = ?
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
            ]:
                if key in updates and updates[key] is not None:
                    try:
                        val = updates[key]
                        if key in ("pump_pullback_enabled", "bounce_lookback") or key.endswith("count") or key == "number_of_trades":
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
                        bounce_pct = ?, bounce_lookback = ?
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
                   pump_pullback_enabled, pump_threshold_pct, pullback_atr_mult, pullback_range_mult, bounce_pct, bounce_lookback
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
            if self.intraday_trend_filter == "ema25":
                return price >= ema25 if ema25 > 0 else False
            if self.intraday_trend_filter == "ema7_ema25":
                return ema7 > ema25 if ema25 > 0 else False
            return True
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

    def _open_intraday_position(self, cur, symbol: str, price: float, amount: float, profit_pct: float, stop_pct: float) -> bool:
        cash = self._get_cash(cur)
        if cash <= 0 or amount <= 0 or price <= 0:
            return False
        if amount > cash:
            return False
        qty = amount / price
        if qty <= 0:
            return False
        if not self.intraday_paper:
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
        stop_price = None
        if not self.intraday_disable_stop and stop_pct and stop_pct > 0:
            stop_price = price * (1 - stop_pct / 100.0)
        tp_price = price * (1 + profit_pct / 100.0)
        new_cash = cash - amount
        self._set_cash(cur, new_cash)
        cur.execute(
            self._q(
                """
                INSERT INTO paper_positions(symbol, qty, entry_price, stop_price, take_profit_price, entry_type, r_value, breakeven_set, partial_taken, trailing_stop)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ),
        )
        cur.execute(
            self._q("INSERT INTO trade_logs(symbol, side, qty, price, pnl, balance_after, reason, status) VALUES (?, 'BUY', ?, ?, 0, ?, ?, 'OPEN')"),
            (symbol, qty, price, new_cash, "intraday_entry"),
        )
        logging.info(f"Intraday BUY {symbol} qty={qty:.6f} price={price} tp={tp_price} stop={stop_price}")
        return True

    def _intraday_open_positions(self, cur) -> List[dict]:
        cur.execute(
            self._q(
                """
                SELECT id, symbol, qty, entry_price, stop_price, take_profit_price, entry_type
                FROM paper_positions
                WHERE status = 'OPEN' AND entry_type = 'intraday'
                """
            )
        )
        rows = cur.fetchall()
        cols = ["id", "symbol", "qty", "entry_price", "stop_price", "take_profit_price", "entry_type"]
        return [dict(zip(cols, r)) for r in rows]

    def _manage_intraday_positions(self, cur) -> bool:
        dirty = False
        positions = self._intraday_open_positions(cur)
        for pos in positions:
            price = self._latest_price(cur, pos["symbol"])
            if price is None:
                continue
            if not self.intraday_disable_stop and pos["stop_price"] is not None:
                if price <= float(pos["stop_price"]):
                    self._close_position(cur, pos, price, "intraday_stop")
                    dirty = True
                    continue
            if pos["take_profit_price"] is not None and price >= float(pos["take_profit_price"]):
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

                trades_count = max(1, int(limits.get("number_of_trades") or 1))
                per_trade_amount = float(limits.get("amount") or 0.0) / trades_count

                # Manage open intraday positions
                if self._manage_intraday_positions(cur):
                    conn.commit()

                # Enforce max open positions = number_of_trades
                open_positions = self._intraday_open_positions(cur)
                if len(open_positions) >= trades_count:
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
                    symbol, margin3, margin5, margin10, margin20, mar3, mar5, mar10, mar20 = row
                    price = price_map.get(symbol)
                    if price is None:
                        continue
                    if not self._intraday_trend_ok(cur, symbol, price):
                        continue
                    if not self._intraday_volume_ok(cur, symbol):
                        continue
                    if limits.get("pump_pullback_enabled"):
                        if not self._intraday_pump_pullback_ok(cur, symbol, limits):
                            continue
                    else:
                        if not self._intraday_recent_pump_ok(cur, symbol):
                            continue

                    if counts["sum_mar3"] < limits["margin3count"]:
                        if price >= float(margin3 or 0) and not mar3:
                            if self._open_intraday_position(cur, symbol, price, per_trade_amount, limits["profit"], limits["stoploss"]):
                                cur.execute(self._q("UPDATE intraday_trading SET mar3 = TRUE, status = '1', purchase_price = ? WHERE symbol = ?"), (price, symbol))
                                counts["sum_mar3"] += 1
                            continue

                    if counts["sum_mar5"] < limits["margin5count"]:
                        if price >= float(margin5 or 0) and not mar5:
                            if self._open_intraday_position(cur, symbol, price, per_trade_amount, limits["profit"], limits["stoploss"]):
                                cur.execute(self._q("UPDATE intraday_trading SET mar5 = TRUE, status = '1', purchase_price = ? WHERE symbol = ?"), (price, symbol))
                                counts["sum_mar5"] += 1
                            continue

                    if counts["sum_mar10"] < limits["margin10count"]:
                        if price >= float(margin10 or 0) and not mar10:
                            if self._open_intraday_position(cur, symbol, price, per_trade_amount, limits["profit"], limits["stoploss"]):
                                cur.execute(self._q("UPDATE intraday_trading SET mar10 = TRUE, status = '1', purchase_price = ? WHERE symbol = ?"), (price, symbol))
                                counts["sum_mar10"] += 1
                            continue

                    if counts["sum_mar20"] < limits["margin20count"]:
                        if price >= float(margin20 or 0) and not mar20:
                            if self._open_intraday_position(cur, symbol, price, per_trade_amount, limits["profit"], limits["stoploss"]):
                                cur.execute(self._q("UPDATE intraday_trading SET mar20 = TRUE, status = '1', purchase_price = ? WHERE symbol = ?"), (price, symbol))
                                counts["sum_mar20"] += 1
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
