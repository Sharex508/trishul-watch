import logging
import os
import time
import threading
from typing import Dict, List, Tuple
import json

import requests

from .coin_price_monitor import get_database_connection
from .db_schema import (
    is_pg,
    ensure_candles,
    ensure_features,
    ensure_pattern_tables,
    ensure_orderbook,
    ensure_all_schema,
)
from .zone_engine import compute_curve_location_from_zones, compute_trend_from_zones

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
            pg = is_pg(cur)
            ensure_candles(cur, pg)
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
            pg = is_pg(cur)
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
            pg = is_pg(cur)
            ensure_features(cur, pg)
            ensure_pattern_tables(cur, pg)
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

    def _latest_candles(self, symbol: str, n: int = 100) -> List[Tuple]:
        try:
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            sql = "SELECT ts, open, high, low, close, volume FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT %d" % n
            cur.execute(_q(sql, pg), (symbol, self.timeframe))
            rows = cur.fetchall()
            return rows[::-1]  # ascending, tuples of full OHLCV
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
        data = self._latest_candles(symbol, n=400)
        if len(data) < 50 or pd is None:
            return
        try:
            df_full = pd.DataFrame(data, columns=["ts", "open", "high", "low", "close", "volume"])
        except Exception as e:
            logging.warning(f"Feature compute skipped for {symbol}: bad candle shape ({e})")
            return
        df_full[['open','high','low','close','volume']] = df_full[['open','high','low','close','volume']].astype(float)
        df = df_full[['ts','close']].copy()
        df_full[['open','high','low','close','volume']] = df_full[['open','high','low','close','volume']].astype(float)
        def body_pct(row):
            rng = row['high'] - row['low']
            return (abs(row['close'] - row['open']) / rng) if rng != 0 else 0.0
        df_full['body_pct'] = df_full.apply(body_pct, axis=1)
        df_full['is_boring'] = (df_full['body_pct'] <= 0.5).astype(int)
        df['ret'] = df['close'].pct_change()
        df['volatility'] = df['ret'].rolling(60, min_periods=20).std()
        df['ret_z'] = (df['ret'] - df['ret'].rolling(60, min_periods=20).mean()) / df['volatility']
        # Price-based EMAs
        df['ema7'] = df['close'].ewm(span=7, adjust=False).mean()
        df['ema25'] = df['close'].ewm(span=25, adjust=False).mean()
        df['ema_slope'] = df['ema7'].diff(3) / 3.0
        # Returns windows
        df['ret_1'] = df['close'].pct_change(1)
        df['ret_5'] = df['close'].pct_change(5)
        df['ret_15'] = df['close'].pct_change(15)
        df['ret_z1'] = df['ret_1'] / (df['volatility'] + 1e-9)
        df['ret_z5'] = df['ret_5'] / (df['volatility'] + 1e-9)
        df['ret_z15'] = df['ret_15'] / (df['volatility'] + 1e-9)
        # RSI
        delta = df['close'].diff()
        gain = delta.clip(lower=0).rolling(14, min_periods=5).mean()
        loss = (-delta.clip(upper=0)).rolling(14, min_periods=5).mean()
        rs = gain / (loss + 1e-9)
        df['rsi'] = 100 - (100 / (1 + rs))
        # MACD
        ema12 = df['close'].ewm(span=12, adjust=False).mean()
        ema26 = df['close'].ewm(span=26, adjust=False).mean()
        df['macd'] = ema12 - ema26
        df['macd_signal'] = df['macd'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd'] - df['macd_signal']
        # Bollinger width
        mid = df['close'].rolling(20, min_periods=10).mean()
        std = df['close'].rolling(20, min_periods=10).std()
        upper = mid + 2 * std
        lower = mid - 2 * std
        df['boll_width'] = (upper - lower) / (mid + 1e-9)
        # ATR (14)
        # Need highs/lows for ATR; approximate using close deltas if missing
        close = df['close']
        prev_close = close.shift(1)
        tr = pd.concat([
            (close - prev_close).abs(),
            (close - close.shift(1)).abs(),
            (prev_close - close).abs()
        ], axis=1).max(axis=1)
        df['atr'] = tr.rolling(14, min_periods=5).mean()
        # Volume z-score approximation using close deltas as proxy (volume not present here)
        df['vol_z'] = df['ret'].rolling(30, min_periods=10).apply(lambda s: (s.iloc[-1] - s.mean()) / (s.std() + 1e-9), raw=False)

        latest = df.iloc[-1]
        latest_full = df_full.iloc[-1]
        ts = int(latest['ts'])
        try:
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            ins = _q(
                """
                INSERT INTO features(
                    symbol, timeframe, ema7, ema25, ema_slope,
                    ret_1, ret_5, ret_15,
                    ret_z1, ret_z5, ret_z15,
                    volatility, vol_z, rsi,
                    macd, macd_signal, macd_hist,
                    boll_width, atr, body_pct, is_boring, ts
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                pg,
            )
            cur.execute(
                ins,
                (
                    symbol, self.timeframe,
                    float(latest['ema7']), float(latest['ema25']), float(latest.get('ema_slope', 0.0) or 0.0),
                    float(latest['ret_1'] or 0.0), float(latest['ret_5'] or 0.0), float(latest['ret_15'] or 0.0),
                    float(latest['ret_z1'] or 0.0), float(latest['ret_z5'] or 0.0), float(latest['ret_z15'] or 0.0),
                    float(latest['volatility'] or 0.0), float(latest['vol_z'] or 0.0), float(latest['rsi'] or 0.0),
                    float(latest['macd'] or 0.0), float(latest['macd_signal'] or 0.0), float(latest['macd_hist'] or 0.0),
                    float(latest['boll_width'] or 0.0), float(latest['atr'] or 0.0),
                    float(latest_full['body_pct'] or 0.0),
                    bool(latest_full['is_boring']),
                    ts
                )
            )
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
            pg = is_pg(cur)
            ensure_orderbook(cur, pg)
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
                        pg = is_pg(cur)
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
                        pg = is_pg(cur)
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
    from sklearn.ensemble import RandomForestClassifier
except Exception as e:  # keep import failure non-fatal; we log and skip discovery
    np = None
    KMeans = None
    RandomForestClassifier = None
    logging.warning(f"Scientific stack not fully available: {e}")

try:
    import pandas as pd
except Exception as e:
    pd = None
    logging.warning(f"pandas not available for pattern engine: {e}")


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
            pg = is_pg(cur)
            sql = f"""
                SELECT ts, ema_slope, ret_z1, ret_z5, ret_z15, volatility, vol_z, rsi, macd_hist, boll_width
                FROM features
                WHERE symbol = ? AND timeframe = ?
                ORDER BY ts DESC LIMIT {int(n)}
            """
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
            pg = is_pg(cur)
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
            pg = is_pg(cur)
            # Insert clusters
            cluster_ids: List[int] = []
            for c in centroids:
                centroid_json = json.dumps({
                    "ema_slope": c[0],
                    "ret_z1": c[1],
                    "ret_z5": c[2],
                    "ret_z15": c[3],
                    "volatility": c[4],
                    "vol_z": c[5],
                    "rsi": c[6],
                    "macd_hist": c[7],
                    "boll_width": c[8],
                })
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
            pg = is_pg(cur)
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
                curve_loc = compute_curve_location_from_zones(sym, base_tf=self.timeframe)
                trend_loc = compute_trend_from_zones(sym, base_tf=self.timeframe)
                # upsert row
                try:
                    conn, cur = get_database_connection()
                    pg = is_pg(cur)
                    now_ts = int(time.time() * 1000)
                    ins = _q("INSERT INTO regime_states(symbol, timeframe, ts, regime, confidence, model_version, curve_location, trend) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", pg)
                    cur.execute(ins, (sym, self.timeframe, now_ts, regime, conf, 'heuristic-v1', curve_loc, trend_loc))
                    conn.commit()
                    logging.info(f"Regime {sym} {self.timeframe}: regime={regime} conf={conf:.2f} curve={curve_loc} trend={trend_loc}")
                except Exception as e:
                    logging.warning(f"Regime upsert failed for {sym}: {e}")
                finally:
                    try:
                        cur.close(); conn.close()
                    except Exception:
                        pass
            time.sleep(self.interval_sec)


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
            pg = is_pg(cur)
            cur.execute(_q("SELECT 1 FROM coin_monitor WHERE symbol = ?", pg), (symbol,))
            if cur.fetchone():
                return
            # fetch current price
            url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            price = float(resp.json()['price'])
            cur.execute(
                _q(
                    "INSERT INTO coin_monitor(symbol, initial_price, low_price, high_price, latest_price) VALUES (?, ?, ?, ?, ?)",
                    pg,
                ),
                (symbol, price, price, price, price),
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


class PatternEngine:
    """
    Detects incremental (slow grind up) and decremental (slow grind down) patterns
    across multiple timeframes using recent 1m candles as the base. Stores matches
    into `pattern_events` for downstream routing/visualization.
    """
    def __init__(self, interval_sec: int = 300):
        self.interval_sec = _env_int('PATTERN_ENGINE_SEC', interval_sec)
        self.timeframes = [1, 5, 15, 30, 60]
        self.window_bars = _env_int('PATTERN_WINDOW_BARS', 30)  # bars per timeframe
        self.min_score = _env_float('PATTERN_MIN_SCORE', 0.65)
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logging.info("PatternEngine started")

    def stop(self):
        self._stop.set()

    def _symbols(self) -> List[str]:
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

    def _top_movers(self, limit: int = 50) -> List[str]:
        """Pick top USDT gainers by 24h change and volume."""
        try:
            tickers = MoversManager()._fetch_24h_tickers()
            usdt = [t for t in tickers if isinstance(t.get('symbol'), str) and t['symbol'].endswith('USDT')]
            def change(t):
                try:
                    return float(t.get('priceChangePercent', 0.0))
                except Exception:
                    return 0.0
            sorted_syms = [t['symbol'] for t in sorted(usdt, key=change, reverse=True)[:limit]]
            return sorted_syms
        except Exception as e:
            logging.warning(f"Top movers fetch failed: {e}")
            return []

    def _fetch_1m_candles(self, symbol: str, limit: int = 600):
        try:
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            sql = f"SELECT ts, open, high, low, close, volume FROM candles WHERE symbol = ? AND timeframe = '1m' ORDER BY ts DESC LIMIT {int(limit)}"
            cur.execute(_q(sql, pg), (symbol,))
            rows = cur.fetchall()
            return [tuple(r) for r in rows][::-1]  # ascending
        except Exception as e:
            logging.warning(f"fetch 1m candles failed for {symbol}: {e}")
            return []
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _resample(self, rows: List[tuple], tf_min: int):
        if pd is None or not rows:
            return None
        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        df['ts_dt'] = pd.to_datetime(df['ts'], unit='ms')
        df = df.set_index('ts_dt')
        if tf_min == 1:
            return df
        rule = f"{tf_min}min"
        agg = df.resample(rule).agg({
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "sum",
            "ts": "last"
        }).dropna()
        return agg

    @staticmethod
    def _feature_dict(df) -> Optional[dict]:
        try:
            if df is None or len(df) < 5:
                return None
            close = df['close']
            ret = close.pct_change().dropna()
            if ret.empty:
                return None
            pct_change = float((close.iloc[-1] - close.iloc[0]) / close.iloc[0]) if close.iloc[0] else 0.0
            volatility = float(ret.std() or 0.0)
            consistency = float((ret > 0).mean())
            mean_ret = float(ret.mean() or 0.0)
            slope = float((close.iloc[-1] - close.iloc[0]) / max(len(close) - 1, 1))
            vol_mean = float(df['volume'].mean() or 0.0)
            vol_std = float(df['volume'].std() or 1.0)
            volume_z = float((df['volume'].iloc[-1] - vol_mean) / (vol_std if vol_std else 1.0))
            return {
                "pct_change": pct_change,
                "volatility": volatility,
                "consistency": consistency,
                "mean_ret": mean_ret,
                "slope": slope,
                "volume_z": volume_z,
                "bars": len(df)
            }
        except Exception:
            return None

    def _score(self, feats: dict) -> Optional[Tuple[str, float]]:
        if not feats:
            return None
        vol = max(feats.get("volatility", 0.0), 1e-6)
        pct = feats.get("pct_change", 0.0)
        cons = feats.get("consistency", 0.0)
        slope = feats.get("slope", 0.0)
        vol_z = feats.get("volume_z", 0.0)
        # Normalize strength by volatility to avoid overreacting to 0.1% in noisy coins
        strength = pct / vol
        volume_term = max(min(vol_z / 4.0, 1.0), -1.0)
        if pct > 0:
            score = 0.35 * min(max(strength / 4.0, -2.0), 2.0) + 0.35 * cons + 0.2 * volume_term + 0.1 * slope
            if score >= self.min_score and cons >= 0.55:
                return ("incremental", float(score))
        if pct < 0:
            score = 0.35 * min(max(-strength / 4.0, -2.0), 2.0) + 0.35 * (1 - cons) + 0.2 * volume_term + 0.1 * (-slope)
            if score >= self.min_score and (1 - cons) >= 0.55:
                return ("decremental", float(score))
        return None

    def _persist_event(self, symbol: str, timeframe: str, direction: str, score: float, feats: dict):
        try:
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            now_ts = int(time.time() * 1000)
            ins = _q("INSERT INTO pattern_events(symbol, timeframe, direction, score, pct_change, consistency, volatility, volume_z, detected_at, features_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", pg)
            cur.execute(
                ins,
                (
                    symbol,
                    timeframe,
                    direction,
                    float(score),
                    float(feats.get("pct_change", 0.0)),
                    float(feats.get("consistency", 0.0)),
                    float(feats.get("volatility", 0.0)),
                    float(feats.get("volume_z", 0.0)),
                    now_ts,
                    json.dumps(feats)
                )
            )
            conn.commit()
        except Exception as e:
            logging.warning(f"Persist pattern event failed for {symbol}:{timeframe}: {e}")
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _evaluate_symbol(self, symbol: str) -> int:
        rows = self._fetch_1m_candles(symbol, limit=self.window_bars * max(self.timeframes))
        if not rows or pd is None:
            return 0
        events = 0
        for tf in self.timeframes:
            df = self._resample(rows, tf)
            if df is None:
                continue
            df = df.tail(self.window_bars)
            feats = self._feature_dict(df)
            decision = self._score(feats)
            if decision:
                direction, score = decision
                self._persist_event(symbol, f"{tf}m", direction, score, feats)
                events += 1
        return events

    def _run(self):
        while not self._stop.is_set():
            syms = self._symbols()
            movers = set(self._top_movers(limit=60))
            candidates = [s for s in syms if s in movers] if movers else syms
            for sym in candidates:
                try:
                    self._evaluate_symbol(sym)
                except Exception as e:
                    logging.warning(f"PatternEngine evaluate failed for {sym}: {e}")
            time.sleep(self.interval_sec)


class RegimeClassifier:
    """Lightweight supervised regime classifier (RandomForest) trained on recent normalized features."""
    def __init__(self, interval_sec: int = 900, timeframe: str = "1m", lookback_rows: int = 2000):
        self.interval_sec = _env_int("REGIME_CLF_SEC", interval_sec)
        self.timeframe = timeframe
        self.lookback_rows = lookback_rows
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.model = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logging.info("RegimeClassifier started")

    def stop(self):
        self._stop.set()

    def _symbols(self) -> List[str]:
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

    def _fetch_feature_rows(self, symbol: str, n: int):
        try:
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            sql = f"""
                SELECT ret_z1, ret_z5, ret_z15, volatility, vol_z, rsi, macd_hist, boll_width, atr
                FROM features
                WHERE symbol = ? AND timeframe = ?
                ORDER BY ts DESC LIMIT {int(n)}
            """
            cur.execute(_q(sql, pg), (symbol, self.timeframe))
            rows = cur.fetchall()
            return [tuple(float(x or 0.0) for x in r) for r in rows]
        except Exception:
            return []
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _label_rows(self, rows: List[tuple]) -> List[int]:
        labels = []
        for r in rows:
            retz1, retz5, retz15, vol, volz, rsi, macd_hist, bw, atr = r
            # heuristic labels: 0=range,1=up,2=down,3=breakout,4=panic
            if vol > 0.01 and retz5 > 0.5 and macd_hist > 0:
                labels.append(3)
            elif vol > 0.01 and retz5 < -0.5 and macd_hist < 0:
                labels.append(4)
            elif retz5 > 0.2 and rsi > 55:
                labels.append(1)
            elif retz5 < -0.2 and rsi < 45:
                labels.append(2)
            else:
                labels.append(0)
        return labels

    def _train(self):
        if RandomForestClassifier is None:
            return
        X = []
        y = []
        for sym in self._symbols():
            rows = self._fetch_feature_rows(sym, self.lookback_rows // 5)
            if len(rows) < 30:
                continue
            labels = self._label_rows(rows)
            X.extend(rows)
            y.extend(labels)
        if len(set(y)) < 2 or len(X) < 50:
            return
        clf = RandomForestClassifier(
            n_estimators=80,
            max_depth=6,
            random_state=42,
            n_jobs=-1
        )
        clf.fit(np.array(X), np.array(y))
        self.model = clf
        logging.info(f"RegimeClassifier trained with {len(X)} samples")

    def _predict_and_store(self):
        if self.model is None:
            return
        for sym in self._symbols():
            rows = self._fetch_feature_rows(sym, 1)
            if not rows:
                continue
            x = np.array(rows[0]).reshape(1, -1)
            pred = int(self.model.predict(x)[0])
            proba = self.model.predict_proba(x)[0]
            conf = float(max(proba))
            label_map = {0: "range", 1: "trend-up", 2: "trend-down", 3: "breakout", 4: "panic"}
            regime = label_map.get(pred, "range")
            try:
                conn, cur = get_database_connection()
                pg = is_pg(cur)
                now_ts = int(time.time() * 1000)
                curve_loc = compute_curve_location_from_zones(sym, base_tf=self.timeframe)
                trend_loc = compute_trend_from_zones(sym, base_tf=self.timeframe)
                ins = _q("INSERT INTO regime_states(symbol, timeframe, ts, regime, confidence, model_version, curve_location, trend) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", pg)
                cur.execute(ins, (sym, self.timeframe, now_ts, regime, conf, 'rf-v1', curve_loc, trend_loc))
                conn.commit()
            except Exception as e:
                logging.warning(f"RegimeClassifier store failed for {sym}: {e}")
            finally:
                try:
                    cur.close(); conn.close()
                except Exception:
                    pass

    def _run(self):
        while not self._stop.is_set():
            try:
                self._train()
                self._predict_and_store()
            except Exception as e:
                logging.warning(f"RegimeClassifier run error: {e}")
            time.sleep(self.interval_sec)


class RiskManager:
    """Simple risk guardrails for AI decisions (not executing orders)."""
    def __init__(self):
        self.max_open_trades = _env_int("RISK_MAX_OPEN_TRADES", 10)
        self.max_daily_drawdown = _env_float("RISK_MAX_DD_PCT", 5.0)
        self.max_notional_per_trade = _env_float("RISK_MAX_NOTIONAL", 1000.0)

    def _open_trades(self) -> int:
        try:
            conn, cur = get_database_connection()
            cur.execute("SELECT COUNT(1) FROM trade_logs WHERE status = 'OPEN'")
            row = cur.fetchone()
            return int(row[0] or 0)
        except Exception:
            return 0
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _daily_pnl(self) -> float:
        try:
            conn, cur = get_database_connection()
            cur.execute("SELECT side, qty, price, created_at FROM trade_logs WHERE DATE(created_at)=DATE('now')")
            rows = cur.fetchall()
            pnl = 0.0
            for side, qty, price, _ in rows:
                sign = 1 if side == 'SELL' else -1
                pnl += sign * float(qty or 0) * float(price or 0)
            return pnl
        except Exception:
            return 0.0
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def allow(self, expected_notional: float) -> bool:
        if self._open_trades() >= self.max_open_trades:
            return False
        if expected_notional > self.max_notional_per_trade:
            return False
        dd = self._daily_pnl()
        if dd < 0:
            dd_pct = abs(dd) / max(self.max_notional_per_trade * self.max_open_trades, 1e-9) * 100
            if dd_pct > self.max_daily_drawdown:
                return False
        return True


class ExpertEnsemble:
    """Multiple experts producing BUY/SELL/HOLD intentions, aggregated with regime weights and risk checks."""
    def __init__(self, interval_sec: int = 120):
        self.interval_sec = _env_int("EXPERT_SEC", interval_sec)
        self.timeframe = "1m"
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.risk = RiskManager()

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logging.info("ExpertEnsemble started")

    def stop(self):
        self._stop.set()

    def _symbols(self):
        try:
            conn, cur = get_database_connection()
            cur.execute("SELECT symbol, latest_price FROM coin_monitor")
            return [(r[0], float(r[1] or 0.0)) for r in cur.fetchall()]
        except Exception:
            return []
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _latest_feature(self, symbol: str):
        try:
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            sql = """
                SELECT ts, ret_z1, ret_z5, ret_z15, ema_slope, volatility, vol_z, rsi, macd_hist, boll_width
                FROM features WHERE symbol = ? AND timeframe = ?
                ORDER BY ts DESC LIMIT 1
            """
            cur.execute(_q(sql, pg), (symbol, self.timeframe))
            row = cur.fetchone()
            if not row:
                return None
            cols = ["ts","ret_z1","ret_z5","ret_z15","ema_slope","volatility","vol_z","rsi","macd_hist","boll_width"]
            return dict(zip(cols, row))
        except Exception:
            return None
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _latest_regime(self, symbol: str):
        try:
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            sql = "SELECT regime, confidence FROM regime_states WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT 1"
            cur.execute(_q(sql, pg), (symbol, self.timeframe))
            row = cur.fetchone()
            if not row:
                return (None, 0.0)
            return (row[0], float(row[1] or 0.0))
        except Exception:
            return (None, 0.0)
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _expert_scores(self, feats: dict) -> Dict[str, float]:
        if not feats:
            return {"momentum":0.0,"mean_rev":0.0,"breakout":0.0,"vol_harvest":0.0}
        retz1 = float(feats.get("ret_z1") or 0.0)
        retz5 = float(feats.get("ret_z5") or 0.0)
        ema_slope = float(feats.get("ema_slope") or 0.0)
        vol = float(feats.get("volatility") or 0.0)
        macd_hist = float(feats.get("macd_hist") or 0.0)
        bw = float(feats.get("boll_width") or 0.0)
        rsi = float(feats.get("rsi") or 50.0)
        scores = {
            "momentum": max(0.0, retz1*0.6 + ema_slope*100 + macd_hist*2),
            "mean_rev": max(0.0, -retz1*0.5 + (50 - abs(rsi-50))/50),
            "breakout": max(0.0, retz5*0.5 + bw*5 + macd_hist*2),
            "vol_harvest": max(0.0, vol*5 - abs(retz1))
        }
        return scores

    def _aggregate(self, scores: Dict[str,float], regime: str):
        weights = {
            "trend-up": {"momentum":0.5,"breakout":0.3,"mean_rev":0.1,"vol_harvest":0.1},
            "trend-down": {"momentum":0.2,"breakout":0.1,"mean_rev":0.5,"vol_harvest":0.2},
            "breakout": {"momentum":0.3,"breakout":0.5,"mean_rev":0.05,"vol_harvest":0.15},
            "panic": {"momentum":0.1,"breakout":0.2,"mean_rev":0.5,"vol_harvest":0.2},
            "range": {"momentum":0.2,"breakout":0.1,"mean_rev":0.5,"vol_harvest":0.2},
            None: {"momentum":0.25,"breakout":0.25,"mean_rev":0.25,"vol_harvest":0.25},
        }
        w = weights.get(regime, weights[None])
        total = sum(scores.get(k,0)*w.get(k,0) for k in w)
        if total <= 0.05:
            return ("HOLD", 0.0)
        # Decide side by relative expert dominance
        buy_score = scores.get("momentum",0)*w.get("momentum",0) + scores.get("breakout",0)*w.get("breakout",0)
        sell_score = scores.get("mean_rev",0)*w.get("mean_rev",0) + scores.get("vol_harvest",0)*w.get("vol_harvest",0)
        if buy_score > sell_score and buy_score > 0.05:
            conf = min(0.99, buy_score / max(buy_score+sell_score, 1e-6))
            return ("BUY", conf)
        if sell_score > buy_score and sell_score > 0.05:
            conf = min(0.99, sell_score / max(buy_score+sell_score, 1e-6))
            return ("SELL", conf)
        return ("HOLD", 0.0)

    def _persist(self, symbol: str, intention: str, confidence: float, expected_return: float, regime: str, pattern_score: float, risk_blocked: bool):
        try:
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            ins = _q("""
                INSERT INTO ai_decisions(symbol, timeframe, intention, confidence, expected_return, regime, pattern_score, risk_blocked)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, pg)
            cur.execute(ins, (symbol, self.timeframe, intention, confidence, expected_return, regime, pattern_score, risk_blocked))
            conn.commit()
        except Exception as e:
            logging.warning(f"Persist decision failed for {symbol}: {e}")
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _latest_pattern_score(self, symbol: str) -> float:
        try:
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            sql = "SELECT score FROM pattern_events WHERE symbol = ? ORDER BY detected_at DESC LIMIT 1"
            cur.execute(_q(sql, pg), (symbol,))
            row = cur.fetchone()
            return float(row[0] or 0.0) if row else 0.0
        except Exception:
            return 0.0
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _run(self):
        while not self._stop.is_set():
            for sym, last_price in self._symbols():
                feats = self._latest_feature(sym)
                regime, regime_conf = self._latest_regime(sym)
                scores = self._expert_scores(feats)
                intention, conf = self._aggregate(scores, regime)
                pattern_score = self._latest_pattern_score(sym)
                # expected return proxy: ret_z5 scaled
                expected_return = float(feats.get("ret_z5", 0.0) or 0.0) if feats else 0.0
                risk_ok = self.risk.allow(expected_notional=last_price)
                final_intention = intention if risk_ok else "HOLD"
                self._persist(sym, final_intention, conf*regime_conf, expected_return, regime, pattern_score, not risk_ok)
            time.sleep(self.interval_sec)


class Backtester:
    """Nightly/lightweight backtest to track regime model health."""
    def __init__(self, interval_sec: int = 21600, timeframe: str = "1m", horizon: int = 5):
        self.interval_sec = _env_int("BACKTEST_SEC", interval_sec)
        self.timeframe = timeframe
        self.horizon = horizon
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logging.info("Backtester started")

    def stop(self):
        self._stop.set()

    def _symbols(self):
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

    def _forward_return(self, symbol: str, ts: int):
        try:
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            cur.execute(_q("SELECT close FROM candles WHERE symbol = ? AND timeframe = ? AND ts <= ? ORDER BY ts DESC LIMIT 1", pg), (symbol, self.timeframe, ts))
            row0 = cur.fetchone()
            if not row0:
                return None
            base = float(row0[0])
            cur.execute(_q("SELECT close FROM candles WHERE symbol = ? AND timeframe = ? AND ts > ? ORDER BY ts ASC LIMIT ?", pg), (symbol, self.timeframe, ts, self.horizon))
            rows = cur.fetchall()
            if len(rows) < self.horizon:
                return None
            fut = float(rows[-1][0])
            return (fut - base) / base if base else None
        except Exception:
            return None
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _run_test(self):
        rets = []
        for sym in self._symbols():
            try:
                conn, cur = get_database_connection()
                pg = is_pg(cur)
                cur.execute(_q("SELECT ts, ret_z5 FROM features WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT 200", pg), (sym, self.timeframe))
                rows = cur.fetchall()
                for ts, rz5 in rows:
                    if rz5 is None:
                        continue
                    if float(rz5) > 0.2:  # simple long condition
                        fwd = self._forward_return(sym, int(ts))
                        if fwd is not None:
                            rets.append(fwd)
            except Exception:
                pass
            finally:
                try:
                    cur.close(); conn.close()
                except Exception:
                    pass
        if not rets:
            return None
        avg = float(np.mean(rets))
        win_rate = float((np.array(rets) > 0).mean())
        sharpe = float(avg / (np.std(rets) + 1e-9))
        return {"avg": avg, "win_rate": win_rate, "sharpe": sharpe, "samples": len(rets)}

    def _persist(self, metrics: dict):
        try:
            conn, cur = get_database_connection()
            pg = is_pg(cur)
            ins = _q("INSERT INTO backtest_runs(model_name, completed_at, samples, sharpe, win_rate, avg_return, notes) VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?)", pg)
            cur.execute(ins, ("regime_rf_v1", int(metrics["samples"]), float(metrics["sharpe"]), float(metrics["win_rate"]), float(metrics["avg"]), json.dumps(metrics)))
            conn.commit()
        except Exception as e:
            logging.warning(f"Backtest persist failed: {e}")
        finally:
            try:
                cur.close(); conn.close()
            except Exception:
                pass

    def _run(self):
        while not self._stop.is_set():
            metrics = self._run_test()
            if metrics:
                self._persist(metrics)
            time.sleep(self.interval_sec)


# Global singletons
candle_ingestor = CandleIngestor()
feature_computer = FeatureComputer()
orderbook_ingestor = OrderbookIngestor()
pattern_discovery = PatternDiscovery()
regime_updater = RegimeUpdater()
movers_manager = MoversManager()
pattern_engine = PatternEngine()
regime_classifier = RegimeClassifier()
expert_ensemble = ExpertEnsemble()
backtester = Backtester()


def start_ai_background_jobs():
    try:
        candle_ingestor.start()
        feature_computer.start()
        orderbook_ingestor.start()
        pattern_discovery.start()
        regime_updater.start()
        movers_manager.start()
        pattern_engine.start()
        regime_classifier.start()
        expert_ensemble.start()
        backtester.start()
    except Exception as e:
        logging.error(f"Failed starting AI background jobs: {e}")
