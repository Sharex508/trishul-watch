"""
Centralized database schema helpers. Each function creates tables idempotently
for both SQLite and Postgres. Callers pass an open cursor and whether the
driver is Postgres.
"""
from typing import Optional
import logging


def is_pg(cursor) -> bool:
    return "psycopg2" in type(cursor).__module__


def column_exists(cur, table: str, column: str, pg: bool) -> bool:
    try:
        if pg:
            cur.execute(
                "SELECT 1 FROM information_schema.columns WHERE table_name=%s AND column_name=%s LIMIT 1",
                (table, column),
            )
            return cur.fetchone() is not None
        else:
            cur.execute(f"PRAGMA table_info({table})")
            rows = cur.fetchall()
            for r in rows:
                if len(r) > 1 and r[1] == column:
                    return True
            return False
    except Exception:
        return False


def add_col(cur, table: str, col: str, type_sql: str, default_sql: Optional[str] = None, pg: bool = False):
    """Add column if missing; safe across Postgres/SQLite."""
    if column_exists(cur, table, col, pg):
        return
    stmt = f"ALTER TABLE {table} ADD COLUMN {col} {type_sql}"
    if default_sql:
        stmt += f" {default_sql}"
    cur.execute(stmt)


def ensure_coin_monitor(cur, pg: bool):
    if pg:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS coin_monitor (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL UNIQUE,
                initial_price DOUBLE PRECISION NOT NULL,
                low_price DOUBLE PRECISION NOT NULL,
                high_price DOUBLE PRECISION NOT NULL,
                latest_price DOUBLE PRECISION NOT NULL,
                low_price_1 DOUBLE PRECISION DEFAULT 0.0,
                high_price_1 DOUBLE PRECISION DEFAULT 0.0,
                low_price_2 DOUBLE PRECISION DEFAULT 0.0,
                high_price_2 DOUBLE PRECISION DEFAULT 0.0,
                low_price_3 DOUBLE PRECISION DEFAULT 0.0,
                high_price_3 DOUBLE PRECISION DEFAULT 0.0,
                low_price_4 DOUBLE PRECISION DEFAULT 0.0,
                high_price_4 DOUBLE PRECISION DEFAULT 0.0,
                low_price_5 DOUBLE PRECISION DEFAULT 0.0,
                high_price_5 DOUBLE PRECISION DEFAULT 0.0,
                low_price_6 DOUBLE PRECISION DEFAULT 0.0,
                high_price_6 DOUBLE PRECISION DEFAULT 0.0,
                low_price_7 DOUBLE PRECISION DEFAULT 0.0,
                high_price_7 DOUBLE PRECISION DEFAULT 0.0,
                low_price_8 DOUBLE PRECISION DEFAULT 0.0,
                high_price_8 DOUBLE PRECISION DEFAULT 0.0,
                low_price_9 DOUBLE PRECISION DEFAULT 0.0,
                high_price_9 DOUBLE PRECISION DEFAULT 0.0,
                low_price_10 DOUBLE PRECISION DEFAULT 0.0,
                high_price_10 DOUBLE PRECISION DEFAULT 0.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    else:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS coin_monitor (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL UNIQUE,
                initial_price REAL NOT NULL,
                low_price REAL NOT NULL,
                high_price REAL NOT NULL,
                latest_price REAL NOT NULL,
                low_price_1 REAL DEFAULT 0.0,
                high_price_1 REAL DEFAULT 0.0,
                low_price_2 REAL DEFAULT 0.0,
                high_price_2 REAL DEFAULT 0.0,
                low_price_3 REAL DEFAULT 0.0,
                high_price_3 REAL DEFAULT 0.0,
                low_price_4 REAL DEFAULT 0.0,
                high_price_4 REAL DEFAULT 0.0,
                low_price_5 REAL DEFAULT 0.0,
                high_price_5 REAL DEFAULT 0.0,
                low_price_6 REAL DEFAULT 0.0,
                high_price_6 REAL DEFAULT 0.0,
                low_price_7 REAL DEFAULT 0.0,
                high_price_7 REAL DEFAULT 0.0,
                low_price_8 REAL DEFAULT 0.0,
                high_price_8 REAL DEFAULT 0.0,
                low_price_9 REAL DEFAULT 0.0,
                high_price_9 REAL DEFAULT 0.0,
                low_price_10 REAL DEFAULT 0.0,
                high_price_10 REAL DEFAULT 0.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def ensure_price_history(cur, pg: bool):
    if pg:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS price_history (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                timestamp TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS price_history_symbol_timestamp_idx ON price_history(symbol, timestamp)")
    else:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS price_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                price REAL NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS price_history_symbol_timestamp_idx ON price_history(symbol, timestamp)")


def ensure_candles(cur, pg: bool):
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


def ensure_features(cur, pg: bool):
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
                ret_z1 DOUBLE PRECISION,
                ret_z5 DOUBLE PRECISION,
                ret_z15 DOUBLE PRECISION,
                volatility DOUBLE PRECISION,
                vol_z DOUBLE PRECISION,
                rsi DOUBLE PRECISION,
                macd DOUBLE PRECISION,
                macd_signal DOUBLE PRECISION,
                macd_hist DOUBLE PRECISION,
                boll_width DOUBLE PRECISION,
                atr DOUBLE PRECISION,
                body_pct DOUBLE PRECISION,
                is_boring BOOLEAN,
                ts BIGINT
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS features_sym_tf_ts_idx ON features(symbol, timeframe, ts)")
        if not column_exists(cur, "features", "body_pct", pg):
            cur.execute("ALTER TABLE features ADD COLUMN body_pct DOUBLE PRECISION")
        if not column_exists(cur, "features", "is_boring", pg):
            cur.execute("ALTER TABLE features ADD COLUMN is_boring BOOLEAN")
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
                ret_z1 REAL,
                ret_z5 REAL,
                ret_z15 REAL,
                volatility REAL,
                vol_z REAL,
                rsi REAL,
                macd REAL,
                macd_signal REAL,
                macd_hist REAL,
                boll_width REAL,
                atr REAL,
                body_pct REAL,
                is_boring INTEGER,
                ts INTEGER
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS features_sym_tf_ts_idx ON features(symbol, timeframe, ts)")


def ensure_pattern_tables(cur, pg: bool):
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
        # legacy columns safeguard
        if not column_exists(cur, "features", "body_pct", pg):
            cur.execute("ALTER TABLE features ADD COLUMN body_pct REAL")
        if not column_exists(cur, "features", "is_boring", pg):
            cur.execute("ALTER TABLE features ADD COLUMN is_boring INTEGER")
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
                model_version TEXT,
                curve_location TEXT,
                trend TEXT
            )
            """
        )
        for col in ["curve_location", "trend"]:
            if not column_exists(cur, "regime_states", col, pg):
                cur.execute(f"ALTER TABLE regime_states ADD COLUMN {col} TEXT")
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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_decisions (
                id SERIAL PRIMARY KEY,
                symbol TEXT,
                timeframe TEXT,
                intention TEXT,
                confidence DOUBLE PRECISION,
                expected_return DOUBLE PRECISION,
                regime TEXT,
                pattern_score DOUBLE PRECISION,
                risk_blocked BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_runs (
                id SERIAL PRIMARY KEY,
                model_name TEXT,
                started_at TIMESTAMP DEFAULT NOW(),
                completed_at TIMESTAMP,
                samples INTEGER,
                sharpe DOUBLE PRECISION,
                win_rate DOUBLE PRECISION,
                avg_return DOUBLE PRECISION,
                notes TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pattern_events (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                direction TEXT NOT NULL,
                score DOUBLE PRECISION,
                pct_change DOUBLE PRECISION,
                consistency DOUBLE PRECISION,
                volatility DOUBLE PRECISION,
                volume_z DOUBLE PRECISION,
                detected_at BIGINT,
                features_json TEXT
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS pattern_events_sym_tf_idx ON pattern_events(symbol, timeframe, detected_at)")
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
                model_version TEXT,
                curve_location TEXT,
                trend TEXT
            )
            """
        )
        for col in ["curve_location", "trend"]:
            if not column_exists(cur, "regime_states", col, pg):
                cur.execute(f"ALTER TABLE regime_states ADD COLUMN {col} TEXT")
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
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT,
                timeframe TEXT,
                intention TEXT,
                confidence REAL,
                expected_return REAL,
                regime TEXT,
                pattern_score REAL,
                risk_blocked INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS backtest_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                model_name TEXT,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                completed_at TIMESTAMP,
                samples INTEGER,
                sharpe REAL,
                win_rate REAL,
                avg_return REAL,
                notes TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS pattern_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                direction TEXT NOT NULL,
                score REAL,
                pct_change REAL,
                consistency REAL,
                volatility REAL,
                volume_z REAL,
                detected_at INTEGER,
                features_json TEXT
            )
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS pattern_events_sym_tf_idx ON pattern_events(symbol, timeframe, detected_at)")


def ensure_orderbook(cur, pg: bool):
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


def ensure_trading_tables(cur, pg: bool):
    if pg:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_logs (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                side TEXT CHECK(side IN ('BUY','SELL')) NOT NULL,
                qty DOUBLE PRECISION NOT NULL,
                price DOUBLE PRECISION NOT NULL,
                pnl DOUBLE PRECISION DEFAULT 0,
                reason TEXT DEFAULT '',
                status TEXT DEFAULT 'COMPLETED',
                balance_after DOUBLE PRECISION DEFAULT 0,
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_portfolio (
                id SERIAL PRIMARY KEY,
                cash DOUBLE PRECISION NOT NULL,
                updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_positions (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                qty DOUBLE PRECISION NOT NULL,
                entry_price DOUBLE PRECISION NOT NULL,
                stop_price DOUBLE PRECISION,
                take_profit_price DOUBLE PRECISION,
                opened_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW(),
                status TEXT DEFAULT 'OPEN'
            )
            """
        )
        add_col(cur, "paper_positions", "risk_perc", "DOUBLE PRECISION", pg=pg)
        add_col(cur, "paper_positions", "rr_target", "DOUBLE PRECISION", pg=pg)
        add_col(cur, "paper_positions", "entry_type", "TEXT", pg=pg)
        add_col(cur, "paper_positions", "r_value", "DOUBLE PRECISION", pg=pg)
        add_col(cur, "paper_positions", "breakeven_set", "INTEGER", "DEFAULT 0", pg=pg)
        add_col(cur, "paper_positions", "partial_taken", "INTEGER", "DEFAULT 0", pg=pg)
        add_col(cur, "paper_positions", "trailing_stop", "DOUBLE PRECISION", pg=pg)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS entry_plans (
                id SERIAL PRIMARY KEY,
                zone_id INTEGER,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                entry_type TEXT,
                entry_price DOUBLE PRECISION,
                stop_price DOUBLE PRECISION,
                take_profit_price DOUBLE PRECISION,
                rr_target DOUBLE PRECISION,
                risk_perc DOUBLE PRECISION,
                balance DOUBLE PRECISION,
                position_size DOUBLE PRECISION,
                risk_amount DOUBLE PRECISION,
                atr_used DOUBLE PRECISION,
                buffer_used DOUBLE PRECISION,
                status TEXT DEFAULT 'planned',
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            )
            """
        )
    else:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                side TEXT CHECK(side IN ('BUY','SELL')) NOT NULL,
                qty REAL NOT NULL,
                price REAL NOT NULL,
                pnl REAL DEFAULT 0,
                reason TEXT DEFAULT '',
                status TEXT DEFAULT 'COMPLETED',
                balance_after REAL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_portfolio (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cash REAL NOT NULL,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS paper_positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                qty REAL NOT NULL,
                entry_price REAL NOT NULL,
                stop_price REAL,
                take_profit_price REAL,
                opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status TEXT DEFAULT 'OPEN'
            )
            """
        )
        add_col(cur, "paper_positions", "risk_perc", "REAL", pg=pg)
        add_col(cur, "paper_positions", "rr_target", "REAL", pg=pg)
        add_col(cur, "paper_positions", "entry_type", "TEXT", pg=pg)
        add_col(cur, "paper_positions", "r_value", "REAL", pg=pg)
        add_col(cur, "paper_positions", "breakeven_set", "INTEGER", "DEFAULT 0", pg=pg)
        add_col(cur, "paper_positions", "partial_taken", "INTEGER", "DEFAULT 0", pg=pg)
        add_col(cur, "paper_positions", "trailing_stop", "REAL", pg=pg)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS entry_plans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                zone_id INTEGER,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                entry_type TEXT,
                entry_price REAL,
                stop_price REAL,
                take_profit_price REAL,
                rr_target REAL,
                risk_perc REAL,
                balance REAL,
                position_size REAL,
                risk_amount REAL,
                atr_used REAL,
                buffer_used REAL,
                status TEXT DEFAULT 'planned',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def ensure_intraday_tables(cur, pg: bool):
    if pg:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS intraday_limits (
                id SERIAL PRIMARY KEY,
                margin3count INTEGER,
                margin5count INTEGER,
                margin10count INTEGER,
                margin20count INTEGER,
                profit DOUBLE PRECISION,
                stoploss DOUBLE PRECISION,
                stoploss_limit DOUBLE PRECISION,
                amount DOUBLE PRECISION,
                number_of_trades INTEGER,
                pump_pullback_enabled INTEGER DEFAULT 0,
                pump_threshold_pct DOUBLE PRECISION,
                pullback_atr_mult DOUBLE PRECISION,
                pullback_range_mult DOUBLE PRECISION,
                bounce_pct DOUBLE PRECISION,
                bounce_lookback INTEGER
            )
            """
        )
        add_col(cur, "intraday_limits", "number_of_trades", "INTEGER", pg=pg)
        add_col(cur, "intraday_limits", "pump_pullback_enabled", "INTEGER", "DEFAULT 0", pg=pg)
        add_col(cur, "intraday_limits", "pump_threshold_pct", "DOUBLE PRECISION", pg=pg)
        add_col(cur, "intraday_limits", "pullback_atr_mult", "DOUBLE PRECISION", pg=pg)
        add_col(cur, "intraday_limits", "pullback_range_mult", "DOUBLE PRECISION", pg=pg)
        add_col(cur, "intraday_limits", "bounce_pct", "DOUBLE PRECISION", pg=pg)
        add_col(cur, "intraday_limits", "bounce_lookback", "INTEGER", pg=pg)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS intraday_trading (
                symbol TEXT PRIMARY KEY,
                initial_price DOUBLE PRECISION,
                high_price DOUBLE PRECISION,
                last_price DOUBLE PRECISION,
                margin3 DOUBLE PRECISION,
                margin5 DOUBLE PRECISION,
                margin10 DOUBLE PRECISION,
                margin20 DOUBLE PRECISION,
                purchase_price DOUBLE PRECISION,
                mar3 BOOLEAN DEFAULT FALSE,
                mar5 BOOLEAN DEFAULT FALSE,
                mar10 BOOLEAN DEFAULT FALSE,
                mar20 BOOLEAN DEFAULT FALSE,
                status TEXT DEFAULT '0',
                created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
            )
            """
        )
        add_col(cur, "intraday_trading", "purchase_price", "DOUBLE PRECISION", pg=pg)
        add_col(cur, "intraday_trading", "mar3", "BOOLEAN", "DEFAULT FALSE", pg=pg)
        add_col(cur, "intraday_trading", "mar5", "BOOLEAN", "DEFAULT FALSE", pg=pg)
        add_col(cur, "intraday_trading", "mar10", "BOOLEAN", "DEFAULT FALSE", pg=pg)
        add_col(cur, "intraday_trading", "mar20", "BOOLEAN", "DEFAULT FALSE", pg=pg)
        add_col(cur, "intraday_trading", "status", "TEXT", "DEFAULT '0'", pg=pg)
    else:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS intraday_limits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                margin3count INTEGER,
                margin5count INTEGER,
                margin10count INTEGER,
                margin20count INTEGER,
                profit REAL,
                stoploss REAL,
                stoploss_limit REAL,
                amount REAL,
                number_of_trades INTEGER,
                pump_pullback_enabled INTEGER DEFAULT 0,
                pump_threshold_pct REAL,
                pullback_atr_mult REAL,
                pullback_range_mult REAL,
                bounce_pct REAL,
                bounce_lookback INTEGER
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS intraday_trading (
                symbol TEXT PRIMARY KEY,
                initial_price REAL,
                high_price REAL,
                last_price REAL,
                margin3 REAL,
                margin5 REAL,
                margin10 REAL,
                margin20 REAL,
                purchase_price REAL,
                mar3 INTEGER DEFAULT 0,
                mar5 INTEGER DEFAULT 0,
                mar10 INTEGER DEFAULT 0,
                mar20 INTEGER DEFAULT 0,
                status TEXT DEFAULT '0',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )


def ensure_zones(cur, pg: bool):
    if pg:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS zones (
                id SERIAL PRIMARY KEY,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                zone_type TEXT NOT NULL,
                formation TEXT NOT NULL,
                proximal DOUBLE PRECISION NOT NULL,
                distal DOUBLE PRECISION NOT NULL,
                base_start_ts BIGINT,
                base_end_ts BIGINT,
                leg_in_ts BIGINT,
                leg_out_ts BIGINT,
                quality_basic DOUBLE PRECISION,
                quality_adv DOUBLE PRECISION,
                freshness INTEGER DEFAULT 0,
                tests INTEGER DEFAULT 0,
                rr_est DOUBLE PRECISION,
                quality_label TEXT,
                probability_label TEXT,
                curve_location TEXT,
                trend TEXT,
                opposing_dist DOUBLE PRECISION,
                opposing_zone_id INTEGER,
                confluence INTEGER DEFAULT 0,
                lotl INTEGER DEFAULT 0,
                trap INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW(),
                last_tested_at TIMESTAMP
            )
            """
        )
        # Add columns if table already existed
        for col in ["confluence", "lotl", "trap"]:
            add_col(cur, "zones", col, "INTEGER", "DEFAULT 0", pg=pg)
    else:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS zones (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                zone_type TEXT NOT NULL,
                formation TEXT NOT NULL,
                proximal REAL NOT NULL,
                distal REAL NOT NULL,
                base_start_ts INTEGER,
                base_end_ts INTEGER,
                leg_in_ts INTEGER,
                leg_out_ts INTEGER,
                quality_basic REAL,
                quality_adv REAL,
                freshness INTEGER DEFAULT 0,
                tests INTEGER DEFAULT 0,
                rr_est REAL,
                quality_label TEXT,
                probability_label TEXT,
                curve_location TEXT,
                trend TEXT,
                opposing_dist REAL,
                opposing_zone_id INTEGER,
                confluence INTEGER DEFAULT 0,
                lotl INTEGER DEFAULT 0,
                trap INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_tested_at TIMESTAMP
            )
            """
        )
        for col in ["confluence", "lotl", "trap"]:
            add_col(cur, "zones", col, "INTEGER", "DEFAULT 0", pg=pg)

def seed_portfolio(cur, pg: bool, initial_cash: float):
    cur.execute("SELECT COUNT(1) FROM paper_portfolio")
    count = cur.fetchone()[0] if cur else 0
    if int(count or 0) == 0:
        cur.execute("INSERT INTO paper_portfolio(cash) VALUES (?)" if not pg else "INSERT INTO paper_portfolio(cash) VALUES (%s)", (float(initial_cash),))


def ensure_all_schema(cur, pg: bool):
    """Helper to create all known tables; safe to call at startup for convenience."""
    ensure_coin_monitor(cur, pg)
    ensure_price_history(cur, pg)
    ensure_candles(cur, pg)
    ensure_features(cur, pg)
    ensure_pattern_tables(cur, pg)
    ensure_orderbook(cur, pg)
    ensure_intraday_tables(cur, pg)
    ensure_trading_tables(cur, pg)
    ensure_zones(cur, pg)
