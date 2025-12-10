import threading
import time
import logging
from typing import Optional, List, Dict

from .coin_price_monitor import get_database_connection, get_all_coins


class TradingManager:
    """
    Simple paper-trading manager that operates on live price data stored by the
    existing price monitor. When enabled, it checks prices periodically and
    emits paper trades based on a trivial demo strategy for clarity.

    Strategy (for demo clarity):
    - For each tracked symbol, once per cooldown window, if price moved >0.7% up
      from the 7/25 MA midpoint since last check, record a SELL fill; if moved
      >0.7% down, record a BUY fill. This is intentionally simple to keep
      behavior predictable and visible in the UI.

    All trades are immediately recorded as completed paper fills in `trade_logs`.
    """

    def __init__(self, interval_sec: float = 5.0, cooldown_sec: float = 60.0):
        self.interval_sec = interval_sec
        self.cooldown_sec = cooldown_sec
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self._enabled: bool = False
        # symbol -> last trade timestamp to prevent spamming
        self._last_trade_ts: Dict[str, float] = {}
        # determine DB driver once and ensure tables
        self._driver = 'sqlite'
        try:
            conn, cur = get_database_connection()
            mod = type(cur).__module__
            if 'psycopg2' in mod:
                self._driver = 'postgres'
        except Exception:
            pass
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass
        self._ensure_tables()

    @property
    def enabled(self) -> bool:
        return self._enabled

    def _ensure_tables(self):
        try:
            conn, cur = get_database_connection()
            mod = type(cur).__module__
            if 'psycopg2' in mod:
                # PostgreSQL DDL
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS trade_logs (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        side TEXT CHECK(side IN ('BUY','SELL')) NOT NULL,
                        qty DOUBLE PRECISION NOT NULL,
                        price DOUBLE PRECISION NOT NULL,
                        reason TEXT DEFAULT '',
                        status TEXT DEFAULT 'COMPLETED',
                        created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
                    )
                    """
                )
            else:
                # SQLite DDL
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS trade_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        side TEXT CHECK(side IN ('BUY','SELL')) NOT NULL,
                        qty REAL NOT NULL,
                        price REAL NOT NULL,
                        reason TEXT DEFAULT '',
                        status TEXT DEFAULT 'COMPLETED',
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
            conn.commit()
        except Exception as e:
            logging.error(f"Error ensuring trade_logs table: {e}")
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    def start(self):
        if self._thread and self._thread.is_alive():
            self._enabled = True
            return
        self._stop.clear()
        self._enabled = True
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        logging.info("TradingManager started")

    def stop(self):
        self._enabled = False
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1.0)
        logging.info("TradingManager stopped")

    def reset(self):
        # Clear all trade logs
        try:
            conn, cur = get_database_connection()
            cur.execute("DELETE FROM trade_logs")
            conn.commit()
        except Exception as e:
            logging.error(f"Error clearing trade_logs: {e}")
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass
        # also reset cooldowns
        self._last_trade_ts.clear()

    def _q(self, sql: str) -> str:
        # Translate SQLite-style placeholders to psycopg2 style when needed
        return sql.replace('?', '%s') if self._driver == 'postgres' else sql

    def _record_trade(self, symbol: str, side: str, qty: float, price: float, reason: str = ""):
        try:
            conn, cur = get_database_connection()
            sql = self._q(
                "INSERT INTO trade_logs(symbol, side, qty, price, reason, status) VALUES (?, ?, ?, ?, ?, 'COMPLETED')"
            )
            cur.execute(sql, (symbol, side, qty, price, reason))
            conn.commit()
            logging.info(f"Recorded trade: {symbol} {side} qty={qty} price={price} reason={reason}")
        except Exception as e:
            logging.error(f"Error inserting trade log: {e}")
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    def list_trades(self, limit: int = 200) -> List[dict]:
        try:
            limit = int(limit)
            conn, cur = get_database_connection()
            # Avoid driver-specific bind for LIMIT by inlining validated integer
            sql = "SELECT id, symbol, side, qty, price, reason, status, created_at FROM trade_logs ORDER BY id DESC LIMIT " + str(limit)
            cur.execute(sql)
            rows = cur.fetchall()
            cols = ["id","symbol","side","qty","price","reason","status","created_at"]
            return [dict(zip(cols, r)) for r in rows]
        except Exception as e:
            logging.error(f"Error fetching trade logs: {e}")
            return []
        finally:
            try:
                cur.close()
                conn.close()
            except Exception:
                pass

    def _run_loop(self):
        while not self._stop.is_set():
            if not self._enabled:
                time.sleep(self.interval_sec)
                continue
            try:
                # read candidate symbols from existing table
                symbols = get_all_coins()
                now = time.time()
                if not symbols:
                    time.sleep(self.interval_sec)
                    continue

                conn, cur = get_database_connection()
                for sym in symbols:
                    # Read the latest and low/high for rough context
                    sql_sel = self._q(
                        "SELECT latest_price, low_price, high_price FROM coin_monitor WHERE symbol = ?"
                    )
                    cur.execute(sql_sel, (sym,))
                    row = cur.fetchone()
                    if not row:
                        continue
                    latest, lowp, highp = row
                    if latest is None or latest <= 0:
                        continue

                    last_ts = self._last_trade_ts.get(sym, 0)
                    if now - last_ts < self.cooldown_sec:
                        continue

                    # Simple threshold relative to band midpoint for clarity
                    midpoint = (float(lowp) + float(highp)) / 2.0 if lowp is not None and highp is not None else float(latest)
                    diff = 0.0
                    try:
                        diff = (float(latest) - float(midpoint)) / float(midpoint)
                    except Exception:
                        diff = 0.0

                    qty = 1.0  # fixed demo size
                    if diff >= 0.007:  # ~+0.7%
                        self._record_trade(sym, 'SELL', qty, float(latest), reason='midpoint_up_0.7pct')
                        self._last_trade_ts[sym] = now
                    elif diff <= -0.007:  # ~-0.7%
                        self._record_trade(sym, 'BUY', qty, float(latest), reason='midpoint_down_0.7pct')
                        self._last_trade_ts[sym] = now
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
