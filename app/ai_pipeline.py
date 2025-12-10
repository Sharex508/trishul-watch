import logging
import os
import time
import threading
from typing import Dict, List, Tuple
import json

import requests

from .coin_price_monitor import get_database_connection

# Simple helper to detect driver style

def _is_postgres(cursor) -> bool:
    return 'psycopg2' in type(cursor).__module__


def _q(sql: str, pg: bool) -> str:
    return sql.replace('?', '%s') if pg else sql


class CandleIngestor:
    """
    Background ingestor that fetches 1m candles for tracked symbols from Binance
    and stores them into a `candles` table. Minimal version for enabling Patterns tab.
    """
    def __init__(self, interval_sec: int = 60, lookback: int = 50):
        self.interval_sec = interval_sec
        self.lookback = lookback
        self._stop = threading.Event()
        # Python 3.9 compatibility: avoid "X | None" type annotation
        from typing import Optional
        self._thread: Optional[threading.Thread] = None
        self.timeframe = '1m'

        self._ensure_tables()

    def _ensure_tables(self):
        try:
            conn, cur = get_database_connection()
            pg = _is_postgres(cur)
            # Candles
            if pg:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS candles (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        open DOUBLE PRECISION,
                        high DOUBLE PRECISION,
                        low DOUBLE PRECISION,
                        close DOUBLE PRECISION,
                        volume DOUBLE PRECISION,
                        ts BIGINT
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS candles_sym_tf_ts_idx ON candles(symbol, timeframe, ts)")
            else:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS candles (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        open REAL,
                        high REAL,
                        low REAL,
                        close REAL,
                        volume REAL,
                        ts INTEGER
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS candles_sym_tf_ts_idx ON candles(symbol, timeframe, ts)")
            conn.commit()
        except Exception as e:
            logging.error(f"Error ensuring candles table: {e}")
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logging.info("CandleIngestor started")

    def stop(self):
        self._stop.set()

    def _tracked_symbols(self) -> List[str]:
        # Read from coin_monitor
        try:
            conn, cur = get_database_connection()
            cur.execute("SELECT symbol FROM coin_monitor")
            syms = [r[0] for r in cur.fetchall()]
            return syms
        except Exception as e:
            logging.error(f"Error reading tracked symbols: {e}")
            return []
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _upsert_candles(self, symbol: str, rows: List[List]):
        # rows from Binance klines: [openTime, open, high, low, close, volume, closeTime, ...]
        try:
            conn, cur = get_database_connection()
            pg = _is_postgres(cur)
            ins = _q("INSERT INTO candles(symbol, timeframe, open, high, low, close, volume, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", pg)
            for r in rows:
                ts = int(r[0])
                o, h, l, c, v = float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])
                try:
                    cur.execute(ins, (symbol, self.timeframe, o, h, l, c, v, ts))
                except Exception:
                    # ignore duplicates if any
                    pass
            conn.commit()
        except Exception as e:
            logging.error(f"Error upserting candles for {symbol}: {e}")
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _fetch_klines(self, symbol: str) -> List[List]:
        url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={self.timeframe}&limit={self.lookback}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _run(self):
        while not self._stop.is_set():
            syms = self._tracked_symbols()
            for sym in syms:
                try:
                    data = self._fetch_klines(sym)
                    self._upsert_candles(sym, data)
                except Exception as e:
                    logging.warning(f"Candle fetch failed for {sym}: {e}")
            time.sleep(self.interval_sec)


class FeatureComputer:
    """
    Computes simple features from candles and stores into `features`.
    Initial set: returns (1/5/15), EMA(7/25), EMA slope.
    """
    def __init__(self, interval_sec: int = 60):
        self.interval_sec = interval_sec
        self._stop = threading.Event()
        from typing import Optional
        self._thread: Optional[threading.Thread] = None
        self.timeframe = '1m'
        self._ensure_tables()

    def _ensure_tables(self):
        try:
            conn, cur = get_database_connection()
            pg = _is_postgres(cur)
            if pg:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS features (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        ema7 DOUBLE PRECISION,
                        ema25 DOUBLE PRECISION,
                        ema_slope DOUBLE PRECISION,
                        ret_1 DOUBLE PRECISION,
                        ret_5 DOUBLE PRECISION,
                        ret_15 DOUBLE PRECISION,
                        ts BIGINT
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS features_sym_tf_ts_idx ON features(symbol, timeframe, ts)")
            else:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS features (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        timeframe TEXT NOT NULL,
                        ema7 REAL,
                        ema25 REAL,
                        ema_slope REAL,
                        ret_1 REAL,
                        ret_5 REAL,
                        ret_15 REAL,
                        ts INTEGER
                    )
                    """
                )
                cur.execute("CREATE INDEX IF NOT EXISTS features_sym_tf_ts_idx ON features(symbol, timeframe, ts)")
            # Placeholder tables for later phases
            if pg:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pattern_clusters (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT,
                        timeframe TEXT,
                        algo TEXT,
                        centroid_json TEXT,
                        cluster_size INTEGER,
                        avg_return DOUBLE PRECISION,
                        volatility DOUBLE PRECISION,
                        label TEXT,
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pattern_assignments (
                        id SERIAL PRIMARY KEY,
                        pattern_id INTEGER,
                        symbol TEXT,
                        timeframe TEXT,
                        start_ts BIGINT,
                        end_ts BIGINT,
                        features_json TEXT,
                        performance DOUBLE PRECISION
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS regime_states (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT,
                        timeframe TEXT,
                        ts BIGINT,
                        regime TEXT,
                        confidence DOUBLE PRECISION,
                        model_version TEXT
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ai_logs (
                        id SERIAL PRIMARY KEY,
                        ts BIGINT,
                        symbol TEXT,
                        module TEXT,
                        message TEXT,
                        meta_json TEXT
                    )
                    """
                )
            else:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pattern_clusters (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT,
                        timeframe TEXT,
                        algo TEXT,
                        centroid_json TEXT,
                        cluster_size INTEGER,
                        avg_return REAL,
                        volatility REAL,
                        label TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS pattern_assignments (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pattern_id INTEGER,
                        symbol TEXT,
                        timeframe TEXT,
                        start_ts INTEGER,
                        end_ts INTEGER,
                        features_json TEXT,
                        performance REAL
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS regime_states (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT,
                        timeframe TEXT,
                        ts INTEGER,
                        regime TEXT,
                        confidence REAL,
                        model_version TEXT
                    )
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS ai_logs (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        ts INTEGER,
                        symbol TEXT,
                        module TEXT,
                        message TEXT,
                        meta_json TEXT
                    )
                    """
                )
            conn.commit()
        except Exception as e:
            logging.error(f"Error ensuring features/patterns tables: {e}")
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logging.info("FeatureComputer started")

    def stop(self):
        self._stop.set()

    def _latest_candles(self, symbol: str, n: int = 100) -> List[Tuple[int, float]]:
        try:
            conn, cur = get_database_connection()
            pg = _is_postgres(cur)
            sql = "SELECT ts, close FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT %d" % n
            cur.execute(_q(sql, pg), (symbol, self.timeframe))
            rows = cur.fetchall()
            return [(int(ts), float(c)) for (ts, c) in rows][::-1]  # ascending
        except Exception as e:
            logging.error(f"Error fetching candles for features: {e}")
            return []
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    @staticmethod
    def _ema(series: List[float], period: int) -> List[float]:
        if not series:
            return []
        k = 2 / (period + 1)
        out = []
        ema_prev = series[0]
        for x in series:
            ema_prev = x * k + ema_prev * (1 - k)
            out.append(ema_prev)
        return out

    def _compute_and_store(self, symbol: str):
        data = self._latest_candles(symbol, n=150)
        if len(data) < 30:
            return
        ts_list = [ts for ts, _ in data]
        closes = [c for _, c in data]
        ema7 = self._ema(closes, 7)
        ema25 = self._ema(closes, 25)
        # slope as last ema7 minus ema7 3 bars ago (approximate)
        ema_slope = (ema7[-1] - ema7[-4]) / 3 if len(ema7) >= 4 else 0.0
        def ret(n):
            return (closes[-1] - closes[-n]) / closes[-n] if len(closes) > n else 0.0
        ret_1 = ret(1)
        ret_5 = ret(5)
        ret_15 = ret(15)
        ts = ts_list[-1]
        try:
            conn, cur = get_database_connection()
            pg = _is_postgres(cur)
            ins = _q("INSERT INTO features(symbol, timeframe, ema7, ema25, ema_slope, ret_1, ret_5, ret_15, ts) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", pg)
            cur.execute(ins, (symbol, self.timeframe, float(ema7[-1]), float(ema25[-1]), float(ema_slope), float(ret_1), float(ret_5), float(ret_15), int(ts)))
            conn.commit()
        except Exception as e:
            logging.error(f"Error inserting features for {symbol}: {e}")
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _symbols(self) -> List[str]:
        try:
            conn, cur = get_database_connection()
            cur.execute("SELECT symbol FROM coin_monitor")
            syms = [r[0] for r in cur.fetchall()]
            return syms
        except Exception:
            return []
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _run(self):
        while not self._stop.is_set():
            for sym in self._symbols():
                try:
                    self._compute_and_store(sym)
                except Exception as e:
                    logging.warning(f"Feature compute failed for {sym}: {e}")
            time.sleep(self.interval_sec)


from typing import Optional

# ========== Orderbook & Order-Flow Ingestor ==========
class OrderbookIngestor:
    """Polls Binance orderbook (depth) and recent aggregated trades to compute
    spread, orderbook imbalance, and buy/sell split. Stores into
    `orderbook_snapshots` and `orderflow` tables. Works with SQLite and Postgres.
    """
    def __init__(self, interval_sec: int = 5, depth_levels: int = 100):
        self.interval_sec = int(os.getenv('ORDERBOOK_SNAPSHOT_SEC', str(interval_sec)))
        self.depth_levels = int(os.getenv('ORDERBOOK_LEVELS', str(depth_levels)))
        self._stop = threading.Event()
        from typing import Optional
        self._thread: Optional[threading.Thread] = None
        self.timeframe = os.getenv('AI_TIMEFRAMES', '1m').split(',')[0].strip() or '1m'
        self._ensure_tables()

    def _ensure_tables(self):
        try:
            conn, cur = get_database_connection()
            pg = _is_postgres(cur)
            # orderbook_snapshots
            if pg:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        bids_json TEXT,
                        asks_json TEXT,
                        spread DOUBLE PRECISION,
                        imbalance DOUBLE PRECISION,
                        ts BIGINT
                    )
                    """
                )
            else:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        bids_json TEXT,
                        asks_json TEXT,
                        spread REAL,
                        imbalance REAL,
                        ts INTEGER
                    )
                    """
                )
            # orderflow aggregates
            if pg:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS orderflow (
                        id SERIAL PRIMARY KEY,
                        symbol TEXT NOT NULL,
                        buy_volume DOUBLE PRECISION,
                        sell_volume DOUBLE PRECISION,
                        buy_count INTEGER,
                        sell_count INTEGER,
                        ts BIGINT
                    )
                    """
                )
            else:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS orderflow (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        buy_volume REAL,
                        sell_volume REAL,
                        buy_count INTEGER,
                        sell_count INTEGER,
                        ts INTEGER
                    )
                    """
                )
            conn.commit()
        except Exception as e:
            logging.error(f"Error ensuring orderbook/orderflow tables: {e}")
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logging.info("OrderbookIngestor started")

    def stop(self):
        self._stop.set()

    def _symbols(self) -> List[str]:
        try:
            conn, cur = get_database_connection()
            cur.execute("SELECT symbol FROM coin_monitor")
            syms = [r[0] for r in cur.fetchall()]
            return syms
        except Exception:
            return []
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _fetch_depth(self, symbol: str) -> dict:
        url = f"https://api.binance.com/api/v3/depth?symbol={symbol}&limit={min(self.depth_levels, 1000)}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def _fetch_agg_trades(self, symbol: str, lookback_ms: int = 60000) -> List[dict]:
        end = int(time.time() * 1000)
        start = end - lookback_ms
        url = f"https://api.binance.com/api/v3/aggTrades?symbol={symbol}&startTime={start}&endTime={end}"
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data if isinstance(data, list) else []

    def _compute_spread_imbalance(self, depth: dict) -> tuple:
        try:
            best_bid = float(depth['bids'][0][0]) if depth.get('bids') else 0.0
            best_ask = float(depth['asks'][0][0]) if depth.get('asks') else 0.0
            spread = best_ask - best_bid if best_ask and best_bid else 0.0
            # volume imbalance on top 20 levels (or available)
            n = min(20, len(depth.get('bids', [])), len(depth.get('asks', [])))
            sb = sum(float(b[1]) for b in depth.get('bids', [])[:n])
            sa = sum(float(a[1]) for a in depth.get('asks', [])[:n])
            tot = sb + sa
            imb = (sb - sa) / tot if tot else 0.0
            return spread, imb
        except Exception:
            return 0.0, 0.0

    def _aggregate_trades(self, trades: List[dict]) -> tuple:
        # In aggTrades, field 'm' is True if buyer is the maker. Commonly treat m=False as buy-initiated.
        buy_vol = sell_vol = 0.0
        buy_cnt = sell_cnt = 0
        for t in trades:
            qty = float(t.get('q', 0.0))
            m = bool(t.get('m', False))
            if not m:  # buy-initiated
                buy_vol += qty; buy_cnt += 1
            else:
                sell_vol += qty; sell_cnt += 1
        return buy_vol, sell_vol, buy_cnt, sell_cnt

    def _run(self):
        while not self._stop.is_set():
            syms = self._symbols()
            for sym in syms:
                try:
                    depth = self._fetch_depth(sym)
                    spread, imb = self._compute_spread_imbalance(depth)
                    bids = depth.get('bids', [])[:20]
                    asks = depth.get('asks', [])[:20]
                    now_ts = int(time.time() * 1000)
                    # persist snapshot
                    try:
                        conn, cur = get_database_connection()
                        pg = _is_postgres(cur)
                        ins = _q("INSERT INTO orderbook_snapshots(symbol, bids_json, asks_json, spread, imbalance, ts) VALUES (?, ?, ?, ?, ?, ?)", pg)
                        cur.execute(ins, (sym, json.dumps(bids), json.dumps(asks), float(spread), float(imb), int(now_ts)))
                        conn.commit()
                    except Exception as e:
                        logging.warning(f"orderbook insert failed for {sym}: {e}")
                    finally:
                        try:
                            cur.close(); conn.close()
                        except Exception:
                            pass
                    # recent trades aggregation
                    try:
                        trades = self._fetch_agg_trades(sym, lookback_ms=60000)
                        bvol, svol, bcnt, scnt = self._aggregate_trades(trades)
                        conn, cur = get_database_connection()
                        pg = _is_postgres(cur)
                        ins2 = _q("INSERT INTO orderflow(symbol, buy_volume, sell_volume, buy_count, sell_count, ts) VALUES (?, ?, ?, ?, ?, ?)", pg)
                        cur.execute(ins2, (sym, float(bvol), float(svol), int(bcnt), int(scnt), int(now_ts)))
                        conn.commit()
                    except Exception as e:
                        logging.warning(f"orderflow insert failed for {sym}: {e}")
                    finally:
                        try:
                            cur.close(); conn.close()
                        except Exception:
                            pass
                except Exception as e:
                    logging.warning(f"Orderbook fetch failed for {sym}: {e}")
            time.sleep(self.interval_sec)


# ========== Pattern Discovery (k-means) and simple Regime Updater ==========
try:
    import numpy as np
    from sklearn.cluster import KMeans
except Exception as e:  # keep import failure non-fatal; we log and skip discovery
    np = None
    KMeans = None
    logging.warning(f"Scientific stack not fully available: {e}")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


class PatternDiscovery:
    def __init__(self):
        self.k = _env_int('PATTERN_CLUSTER_COUNT', 5)
        self.lookback = _env_int('PATTERN_LOOKBACK_WINDOWS', 300)
        self.window_size = _env_int('PATTERN_WINDOW_SIZE', 1)  # point-wise clustering by default
        self.interval_sec = _env_int('PATTERN_DISCOVERY_SEC', 1800)
        self.timeframe = os.getenv('AI_TIMEFRAMES', '1m').split(',')[0].strip() or '1m'
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logging.info("PatternDiscovery started")

    def stop(self):
        self._stop.set()

    def _symbols(self) -> List[str]:
        try:
            conn, cur = get_database_connection()
            cur.execute("SELECT symbol FROM coin_monitor")
            syms = [r[0] for r in cur.fetchall()]
            return syms
        except Exception:
            return []
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _fetch_features(self, symbol: str, n: int) -> List[Tuple[int, Tuple[float,...]]]:
        try:
            conn, cur = get_database_connection()
            pg = _is_postgres(cur)
            sql = f"SELECT ts, ema7, ema25, ema_slope, ret_1, ret_5, ret_15 FROM features WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT {int(n)}"
            cur.execute(_q(sql, pg), (symbol, self.timeframe))
            rows = cur.fetchall()
            out = []
            for r in rows:
                ts = int(r[0])
                vals = tuple(float(x) if x is not None else 0.0 for x in r[1:])
                out.append((ts, vals))
            return list(reversed(out))
        except Exception as e:
            logging.error(f"Fetch features failed for {symbol}: {e}")
            return []
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _forward_return(self, symbol: str, ts: int, horizon: int = 5) -> float:
        """Compute forward N-bar return using candles after ts (approx).
        Returns 0.0 if not enough data.
        """
        try:
            conn, cur = get_database_connection()
            pg = _is_postgres(cur)
            # pick the close at or just before ts, then the close horizon bars after
            sql = _q("SELECT ts, close FROM candles WHERE symbol = ? AND timeframe = ? AND ts <= ? ORDER BY ts DESC LIMIT 1", pg)
            cur.execute(sql, (symbol, self.timeframe, int(ts)))
            row0 = cur.fetchone()
            if not row0:
                return 0.0
            base_ts, base_close = int(row0[0]), float(row0[1])
            sql2 = _q("SELECT ts, close FROM candles WHERE symbol = ? AND timeframe = ? AND ts > ? ORDER BY ts ASC LIMIT ?", pg)
            cur.execute(sql2, (symbol, self.timeframe, base_ts, int(horizon)))
            rows = cur.fetchall()
            if len(rows) < horizon:
                return 0.0
            fut_close = float(rows[-1][1])
            return (fut_close - base_close) / base_close if base_close else 0.0
        except Exception:
            return 0.0
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _persist(self, symbol: str, assignments: List[Tuple[int,int,float]], centroids: List[List[float]]):
        """Persist clusters and assignments. assignments: list of (ts, cluster_idx, perf)."""
        try:
            conn, cur = get_database_connection()
            pg = _is_postgres(cur)
            # Insert clusters
            cluster_ids: List[int] = []
            for c in centroids:
                centroid_json = json.dumps({"ema7": c[0], "ema25": c[1], "ema_slope": c[2], "ret_1": c[3], "ret_5": c[4], "ret_15": c[5]})
                ins = _q("INSERT INTO pattern_clusters(symbol, timeframe, algo, centroid_json, cluster_size, avg_return, volatility, label) VALUES (?, ?, 'kmeans', ?, 0, 0, 0, '') RETURNING id" if pg else "INSERT INTO pattern_clusters(symbol, timeframe, algo, centroid_json, cluster_size, avg_return, volatility, label) VALUES (?, ?, 'kmeans', ?, 0, 0, 0, '')", pg)
                if pg:
                    cur.execute(ins, (symbol, self.timeframe, centroid_json))
                    cid = cur.fetchone()[0]
                else:
                    cur.execute(ins, (symbol, self.timeframe, centroid_json))
                    cid = cur.lastrowid
                cluster_ids.append(cid)
            # compute per-cluster metrics from assignments
            perfs_by_cluster: Dict[int, List[float]] = {i: [] for i in range(len(centroids))}
            for _, idx, perf in assignments:
                perfs_by_cluster[idx].append(perf)
            # update metrics and compute cluster_size
            for idx, cid in enumerate(cluster_ids):
                perfs = perfs_by_cluster.get(idx, [])
                size = len(perfs)
                avg_ret = float(sum(perfs)/size) if size else 0.0
                vol = float((sum((p-avg_ret)**2 for p in perfs)/size)**0.5) if size else 0.0
                upd = _q("UPDATE pattern_clusters SET cluster_size = ?, avg_return = ?, volatility = ?, label = ? WHERE id = ?", pg)
                label = 'momentum' if avg_ret > 0 and abs(avg_ret) > vol else ('mean-rev' if avg_ret < 0 and abs(avg_ret) > vol else '')
                cur.execute(upd, (size, avg_ret, vol, label, cid))
            # insert assignments
            ins_a = _q("INSERT INTO pattern_assignments(pattern_id, symbol, timeframe, start_ts, end_ts, features_json, performance) VALUES (?, ?, ?, ?, ?, ?, ?)", pg)
            for ts, idx, perf in assignments:
                feats_json = '{}'  # keep minimal
                cur.execute(ins_a, (cluster_ids[idx], symbol, self.timeframe, int(ts), int(ts), feats_json, float(perf)))
            conn.commit()
        except Exception as e:
            logging.error(f"Persist patterns failed for {symbol}: {e}")
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def discover_for(self, symbol: str) -> int:
        if np is None or KMeans is None:
            logging.warning("Pattern discovery skipped: numpy/scikit-learn not available")
            return 0
        data = self._fetch_features(symbol, self.lookback)
        if len(data) < max(self.k * 5, 50):
            logging.info(f"Not enough feature rows for {symbol} to run k-means (have {len(data)})")
            return 0
        # Build matrix X
        X = np.array([list(vals) for (_, vals) in data], dtype=float)
        # Normalize per-feature (z-score)
        mu = X.mean(axis=0)
        sigma = X.std(axis=0)
        sigma[sigma == 0] = 1.0
        Z = (X - mu) / sigma
        # Run KMeans
        k = max(2, self.k)
        km = KMeans(n_clusters=k, n_init=10, random_state=42)
        labels = km.fit_predict(Z)
        # Centroids back in original scale
        cents_z = km.cluster_centers_
        cents = cents_z * sigma + mu
        # Compute forward returns for performance metric
        assignments: List[Tuple[int,int,float]] = []
        for (ts, _vals), idx in zip(data, labels):
            perf = self._forward_return(symbol, ts, horizon=5)
            assignments.append((ts, int(idx), float(perf)))
        # Persist
        self._persist(symbol, assignments, centroids=cents.tolist())
        logging.info(f"Pattern discovery: symbol={symbol} k={k} rows={len(data)}")
        return k

    def _run(self):
        while not self._stop.is_set():
            for sym in self._symbols():
                try:
                    self.discover_for(sym)
                except Exception as e:
                    logging.warning(f"Pattern discovery failed for {sym}: {e}")
            time.sleep(self.interval_sec)


class RegimeUpdater:
    """Heuristic regime labeling using EMA slope and short-term volatility."""
    def __init__(self, interval_sec: int = 120):
        self.interval_sec = _env_int('REGIME_UPDATE_SEC', interval_sec)
        self.timeframe = os.getenv('AI_TIMEFRAMES', '1m').split(',')[0].strip() or '1m'
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None


class MoversManager:
    """Periodically selects top gainers/losers from Binance 24h tickers and ensures
    those symbols are present in coin_monitor (widens the universe automatically).
    Limits max symbols via AI_MAX_SYMBOLS to keep laptop load reasonable.
    """
    def __init__(self, interval_sec: int = 300, max_symbols: Optional[int] = None):
        self.interval_sec = _env_int('MOVERS_REFRESH_SEC', interval_sec)
        try:
            self.max_symbols = int(os.getenv('AI_MAX_SYMBOLS', str(max_symbols or 30)))
        except Exception:
            self.max_symbols = 30
        self._stop = threading.Event()
        from typing import Optional
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logging.info("MoversManager started")

    def stop(self):
        self._stop.set()

    def _current_symbols(self) -> List[str]:
        try:
            conn, cur = get_database_connection()
            cur.execute("SELECT symbol FROM coin_monitor")
            return [r[0] for r in cur.fetchall()]
        except Exception:
            return []
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _ensure_symbol(self, symbol: str):
        # Insert into coin_monitor if missing using current price as initial/low/high/latest
        try:
            conn, cur = get_database_connection()
            cur.execute("SELECT 1 FROM coin_monitor WHERE symbol = ?", (symbol,))
            if cur.fetchone():
                return
            # fetch current price
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            price = float(resp.json()['price'])
            cur.execute(
                """
                INSERT INTO coin_monitor(symbol, initial_price, low_price, high_price, latest_price)
                VALUES (?, ?, ?, ?, ?)
                """,
                (symbol, price, price, price, price)
            )
            conn.commit()
            logging.info(f"MoversManager added symbol: {symbol}")
        except Exception as e:
            logging.warning(f"Unable to ensure symbol {symbol}: {e}")
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _fetch_24h_tickers(self) -> List[dict]:
        url = "https://api.binance.com/api/v3/ticker/24hr"
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        return resp.json()

    def _pick_symbols(self, tickers: List[dict]) -> List[str]:
        # Focus on USDT pairs; pick top gainers and losers by priceChangePercent
        usdt = [t for t in tickers if isinstance(t.get('symbol'), str) and t['symbol'].endswith('USDT')]
        # sort by change percent descending for gainers and ascending for losers
        def pct(t):
            try:
                return float(t.get('priceChangePercent', 0.0))
            except Exception:
                return 0.0
        gainers = sorted(usdt, key=pct, reverse=True)[: self.max_symbols // 2]
        losers = sorted(usdt, key=pct)[: self.max_symbols // 2]
        syms = [t['symbol'] for t in gainers + losers]
        # de-duplicate preserving order
        seen = set(); out = []
        for s in syms:
            if s not in seen:
                seen.add(s); out.append(s)
        return out[: self.max_symbols]

    def _run(self):
        while not self._stop.is_set():
            try:
                tickers = self._fetch_24h_tickers()
                target_syms = self._pick_symbols(tickers)
                existing = set(self._current_symbols())
                for s in target_syms:
                    if s not in existing:
                        self._ensure_symbol(s)
            except Exception as e:
                logging.warning(f"MoversManager tickers error: {e}")
            time.sleep(self.interval_sec)

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logging.info("RegimeUpdater started")

    def stop(self):
        self._stop.set()

    def _symbols(self) -> List[str]:
        try:
            conn, cur = get_database_connection()
            cur.execute("SELECT symbol FROM coin_monitor")
            syms = [r[0] for r in cur.fetchall()]
            return syms
        except Exception:
            return []
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _latest_features(self, symbol: str, n: int = 30):
        try:
            conn, cur = get_database_connection()
            pg = _is_postgres(cur)
            sql = f"SELECT ts, ema_slope, ret_1 FROM features WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT {int(n)}"
            cur.execute(_q(sql, pg), (symbol, self.timeframe))
            rows = cur.fetchall()
            return [(int(a), float(b or 0.0), float(c or 0.0)) for (a, b, c) in rows]
        except Exception:
            return []
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _label(self, ema_slope: float, vol: float) -> Tuple[str, float]:
        if abs(ema_slope) < 1e-9:
            ema_slope = 0.0
        if vol < 0.0005 and abs(ema_slope) < 0.0001:
            return ("low-vol grind", 0.6)
        if ema_slope > 0 and vol < 0.002:
            return ("trend-up", 0.6)
        if ema_slope < 0 and vol < 0.002:
            return ("trend-down", 0.6)
        if vol >= 0.005 and abs(ema_slope) < 0.0005:
            return ("range", 0.55)
        if vol >= 0.01 and ema_slope > 0:
            return ("breakout", 0.6)
        if vol >= 0.01 and ema_slope < 0:
            return ("panic", 0.6)
        return ("range", 0.5)

    def _run(self):
        while not self._stop.is_set():
            for sym in self._symbols():
                feats = self._latest_features(sym, n=30)
                if not feats:
                    continue
                ema_slope = feats[0][1]
                vol = float(sum(abs(x[2]) for x in feats) / len(feats))  # avg |ret_1|
                regime, conf = self._label(ema_slope, vol)
                # upsert row
                try:
                    conn, cur = get_database_connection()
                    pg = _is_postgres(cur)
                    now_ts = int(time.time() * 1000)
                    ins = _q("INSERT INTO regime_states(symbol, timeframe, ts, regime, confidence, model_version) VALUES (?, ?, ?, ?, ?, ?)", pg)
                    cur.execute(ins, (sym, self.timeframe, now_ts, regime, conf, 'heuristic-v1'))
                    conn.commit()
                except Exception as e:
                    logging.warning(f"Regime upsert failed for {sym}: {e}")
                finally:
                    try:
                        cur.close(); conn.close()
                    except Exception:
                        pass
            time.sleep(self.interval_sec)


# Global singletons
candle_ingestor = CandleIngestor()
feature_computer = FeatureComputer()
orderbook_ingestor = OrderbookIngestor()
pattern_discovery = PatternDiscovery()
regime_updater = RegimeUpdater()
movers_manager = MoversManager()


def start_ai_background_jobs():
    try:
        candle_ingestor.start()
        feature_computer.start()
        orderbook_ingestor.start()
        pattern_discovery.start()
        regime_updater.start()
        movers_manager.start()
    except Exception as e:
        logging.error(f"Failed starting AI background jobs: {e}")
