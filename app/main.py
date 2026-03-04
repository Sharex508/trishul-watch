from fastapi import FastAPI, HTTPException, Body, BackgroundTasks, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List
from pydantic import BaseModel
import logging
import os
import json
import hmac
import hashlib
import time
from typing import Optional
import requests

from .coin_monitor import (
    get_all_coin_monitors, 
    get_coin_monitor_by_symbol, 
    update_coin_monitor, 
    update_latest_prices,
    get_coin_price_history,
    get_recent_trades,
    CoinMonitor,
    CoinMonitorUpdate
)
from .coin_price_monitor import start_price_monitor, add_coin, force_update_all_price_histories, update_initial_prices, get_database_connection, get_price, seed_coin_monitor_if_empty
from .trading import trading_manager
from .ai_pipeline import start_ai_background_jobs, is_pg, _q, pattern_discovery
from .zone_engine import run_zone_detection, get_zone_by_id, plan_entry_for_zone, persist_entry_plan
from .db_schema import ensure_orderbook
import pathlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("api.log"),
        logging.StreamHandler()
    ]
)
# Uvicorn may configure logging before this module loads, which can prevent
# basicConfig from adding file handlers. Ensure api.log is always used.
_root_logger = logging.getLogger()
_log_path = pathlib.Path("api.log")
try:
    _log_path.touch(exist_ok=True)
except Exception:
    pass
_has_file = any(
    isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", "") == str(_log_path.resolve())
    for h in _root_logger.handlers
)
if not _has_file:
    _fh = logging.FileHandler(str(_log_path))
    _fh.setLevel(logging.INFO)
    _fh.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    _root_logger.addHandler(_fh)

# Create FastAPI app
app = FastAPI(title="Trishul Watch API", description="API for monitoring cryptocurrency prices with AI Patterns")

# Configure CORS
origins = [
    "http://localhost",
    "http://localhost:3000",
    "http://localhost:8000",
    "*"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class AddCoinRequest(BaseModel):
    symbol: str

class TradeRequest(BaseModel):
    symbol: str
    amount: float
    api_key: Optional[str] = None
    api_secret: Optional[str] = None

class BinanceCredentialsRequest(BaseModel):
    api_key: str
    api_secret: str

class EntryPlanRequest(BaseModel):
    zone_id: int
    balance: float = 1000.0
    risk_perc: float = 1.0
    rr_target: float = 2.0

# Background task for updating coin prices
price_monitor_thread = None

@app.on_event("startup")
def startup_price_update():
    """Start the background task when the app starts."""
    global price_monitor_thread
    # Start the price monitor that updates every 2 seconds
    price_monitor_thread = start_price_monitor()
    try:
        # Ensure we have symbols to work with; seed top USDT movers if empty
        seeded = seed_coin_monitor_if_empty()
        if seeded:
            logging.info(f"Seeded {seeded} symbols into coin_monitor")
    except Exception as e:
        logging.warning(f"Seeding coin_monitor failed at startup: {e}")
    # Start AI background jobs (candles + features)
    try:
        start_ai_background_jobs()
        logging.info("Started AI background jobs (candles/features)")
    except Exception as e:
        logging.error(f"Failed to start AI background jobs: {e}")
    logging.info("Started coin price monitor thread")

@app.on_event("shutdown")
def shutdown_price_update():
    """Stop the background task when the app shuts down."""
    # The thread is a daemon thread, so it will be terminated when the app shuts down
    logging.info("Coin price monitor thread will be stopped when the app shuts down")

# Trading control endpoints (migrated in spirit from Trishul: start/stop/reset + logs)
@app.get("/api/trading/status", response_model=dict)
def trading_status():
    """Return whether the paper-trading loop is enabled."""
    try:
        return {
            "enabled": trading_manager.enabled,
            "coin_brain_symbol": trading_manager.coin_brain_symbol,
            "coin_brain_paper": trading_manager.coin_brain_paper if trading_manager.coin_brain_symbol else None,
            "strategy_mode": trading_manager.strategy_mode,
            "hybrid_enabled": getattr(trading_manager, "hybrid_enabled", True),
            "intraday_enabled": getattr(trading_manager, "intraday_enabled", False),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/trading/start", response_model=dict)
def trading_start():
    """Start paper-trading loop that operates on live prices updated by the monitor."""
    try:
        trading_manager.start()
        return {"enabled": True, "strategy_mode": trading_manager.strategy_mode, "hybrid_enabled": getattr(trading_manager, "hybrid_enabled", True)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/trading/intraday-start", response_model=dict)
def trading_intraday_start(payload: dict = Body(default={})):
    """Start intraday loop modeled after the legacy trade.py flow."""
    try:
        paper = bool(payload.get("paper", True))
        trading_manager.start_intraday(paper=paper)
        return {"enabled": trading_manager.enabled, "strategy_mode": trading_manager.strategy_mode, "hybrid_enabled": getattr(trading_manager, "hybrid_enabled", True)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/trading/stop", response_model=dict)
def trading_stop():
    """Stop paper-trading loop."""
    try:
        trading_manager.stop()
        trading_manager.stop_coin_brain()
        return {"enabled": False, "strategy_mode": trading_manager.strategy_mode, "hybrid_enabled": getattr(trading_manager, "hybrid_enabled", True)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/trading/reset", response_model=dict)
def trading_reset():
    """Clear all paper trade entries (trade_logs) and reset cooldowns."""
    try:
        trading_manager.reset()
        trading_manager.stop_coin_brain()
        return {"ok": True, "hybrid_enabled": getattr(trading_manager, "hybrid_enabled", True)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class BinanceCredentialsRequest(BaseModel):
    api_key: str
    api_secret: str

@app.post("/api/trading/credentials", response_model=dict)
def trading_credentials(payload: BinanceCredentialsRequest = Body(...)):
    """
    Store Binance API key/secret for live spot trading.
    """
    try:
        ok = trading_manager.set_binance_credentials(payload.api_key, payload.api_secret)
        if not ok:
            raise HTTPException(status_code=400, detail="API key/secret required")
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/trading/coin-start", response_model=dict)
def trading_coin_start(payload: dict = Body(...)):
    """
    Start single-symbol coin brain trading with optional paper flag.
    Body: { "symbol": "BTCUSDT", "paper": true }
    """
    try:
        symbol = payload.get("symbol")
        paper = bool(payload.get("paper", True))
        if not symbol:
            raise HTTPException(status_code=400, detail="symbol is required")
        trading_manager.start()
        trading_manager.start_coin_brain(symbol, paper=paper)
        return {"enabled": True, "symbol": symbol, "paper": paper, "strategy_mode": trading_manager.strategy_mode, "hybrid_enabled": getattr(trading_manager, "hybrid_enabled", True)}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/trading/portfolio", response_model=dict)
def trading_portfolio():
    """Paper portfolio snapshot (cash, open positions, unrealized pnl)."""
    try:
        return trading_manager.portfolio()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/trading/intraday-limits", response_model=dict)
def get_intraday_limits():
    try:
        return trading_manager.get_intraday_limits()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/trading/intraday-limits", response_model=dict)
def set_intraday_limits(payload: dict = Body(...)):
    try:
        return trading_manager.set_intraday_limits(payload or {})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/trade-logs", response_model=list)
def get_trade_logs(limit: int = 200):
    """Return recent completed paper trade entries."""
    try:
        return trading_manager.list_trades(limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/coin-monitors", response_model=List[dict])
def read_coin_monitors():
    """
    Endpoint to get all coin monitor records.
    """
    try:
        return get_all_coin_monitors()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/coin-monitors/{symbol}", response_model=dict)
def read_coin_monitor(symbol: str):
    """
    Endpoint to get a specific coin's monitoring data by symbol.
    """
    try:
        coin_monitor = get_coin_monitor_by_symbol(symbol)
        if not coin_monitor:
            raise HTTPException(status_code=404, detail=f"Coin monitor for symbol {symbol} not found")
        return coin_monitor
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/api/coin-monitors/{symbol}", response_model=dict)
def update_coin_monitor_endpoint(symbol: str, data: CoinMonitorUpdate = Body(...)):
    """
    Endpoint to update a coin's monitoring data.
    """
    try:
        # Convert Pydantic model to dict, excluding None values
        update_data = {k: v for k, v in data.dict().items() if v is not None}
        result = update_coin_monitor(symbol, update_data)
        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/coin-monitors/update-prices", response_model=dict)
def update_all_prices():
    """
    Endpoint to refresh all coins with current market prices.
    """
    try:
        return update_latest_prices()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/coin-monitors/{symbol}/history", response_model=dict)
def get_coin_history(symbol: str):
    """
    Endpoint to get the price history for a specific coin.

    This endpoint returns a structured representation of the price history,
    including the initial price, current prices (low, high, latest),
    and historical prices (up to 10 sets of low and high prices).

    The history is updated when there's a significant change in price
    (default 0.5% drop from high price).
    """
    try:
        history = get_coin_price_history(symbol)
        if not history:
            raise HTTPException(status_code=404, detail=f"Price history for symbol {symbol} not found")
        return history
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/coin-monitors/{symbol}/recent-trades", response_model=dict)
def get_coin_recent_trades(symbol: str):
    """
    Endpoint to get recent trade statistics for a specific coin.

    This endpoint returns a structured representation of the recent trade activity,
    including the number of buy and sell trades, volumes, and a trend analysis
    based on the last 3 minutes of trading data from Binance.

    It also provides a direct link to the trading pair on Binance.
    """
    try:
        trades = get_recent_trades(symbol)
        # If upstream call failed, still return a neutral payload so UI doesn't break
        if "error" in trades:
            logging.warning(f"recent-trades fallback for {symbol}: {trades.get('error')}")
        return trades
    except Exception as e:
        logging.warning(f"recent-trades failed for {symbol}: {e}")
        return {
            "symbol": symbol,
            "period": "30 seconds",
            "total_trades": 0,
            "buy_trades": 0,
            "sell_trades": 0,
            "buy_volume": 0,
            "sell_volume": 0,
            "buy_percentage": 0,
            "sell_percentage": 0,
            "average_trade_size": 0,
            "trend": "Neutral",
            "error": str(e),
            "binance_link": f"https://www.binance.com/en/trade/{symbol.replace('USDT', '_USDT')}"
        }

@app.get("/api/trade-activity", response_model=list)
def api_trade_activity(limit: int = Query(50, ge=1, le=200), max_age_sec: int = Query(120, ge=10, le=600)):
    """
    Return a ranked list of symbols by latest 1m trade counts from orderflow.
    """
    conn = None
    cur = None
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        ensure_orderbook(cur, pg)
        now_ms = int(time.time() * 1000)
        min_ts = now_ms - (max_age_sec * 1000)
        sql = _q(
            """
            SELECT o.symbol, o.buy_count, o.sell_count, o.buy_volume, o.sell_volume, o.ts
            FROM orderflow o
            JOIN (
                SELECT symbol, MAX(ts) AS max_ts
                FROM orderflow
                GROUP BY symbol
            ) latest
              ON o.symbol = latest.symbol AND o.ts = latest.max_ts
            WHERE o.ts >= ?
            ORDER BY (o.buy_count + o.sell_count) DESC
            LIMIT ?
            """,
            pg,
        )
        cur.execute(sql, (min_ts, limit))
        rows = cur.fetchall()
        results = []
        for r in rows:
            symbol, buy_count, sell_count, buy_volume, sell_volume, ts = r
            total = int(buy_count or 0) + int(sell_count or 0)
            results.append(
                {
                    "symbol": symbol,
                    "total_trades": total,
                    "buy_count": int(buy_count or 0),
                    "sell_count": int(sell_count or 0),
                    "buy_volume": float(buy_volume or 0),
                    "sell_volume": float(sell_volume or 0),
                    "ts": int(ts or 0),
                }
            )
        return results
    except Exception as e:
        logging.error(f"trade-activity failed: {e}")
        return []
    finally:
        try:
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass
@app.post("/api/coin-monitors/add", response_model=dict)
def add_new_coin(request: AddCoinRequest = Body(...)):
    """
    Endpoint to add a new coin to monitor.
    """
    try:
        # Fetch current price (cache-first helper)
        price = get_price(request.symbol)
        if price is None:
            raise HTTPException(status_code=500, detail="Unable to fetch price for symbol")

        # Add the coin
        success = add_coin(request.symbol, price)

        if success:
            return {"message": f"Added {request.symbol} to monitoring with initial price {price}"}
        else:
            return {"message": f"Coin {request.symbol} already exists or could not be added"}
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            raise HTTPException(status_code=400, detail=f"Invalid symbol: {request.symbol}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/coin-monitors/force-update-history", response_model=dict)
def force_update_history():
    """
    Endpoint to force update all coins' price history with varied values.
    This is useful for fixing all coins at once if they have the same values for all cycles.
    """
    try:
        updated_count = force_update_all_price_histories()
        return {"message": f"Successfully updated price history for {updated_count} coins with varied values"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/coin-monitors/update-initial-prices", response_model=dict)
def update_initial_prices_endpoint():
    """
    Endpoint to update the initial prices for all coins to match the current prices from the Binance API.
    This is useful after a Docker restart to ensure that the initial prices match the current prices.
    """
    try:
        updated_count = update_initial_prices()
        return {"message": f"Successfully updated initial prices for {updated_count} coins to match current prices"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/trade/buy", response_model=dict)
def buy_coin(request: TradeRequest = Body(...)):
    """
    Endpoint to buy a coin using Binance API.
    Requires api_key and api_secret for authentication.
    """
    try:
        if request.api_key and request.api_secret:
            trading_manager.set_binance_credentials(request.api_key, request.api_secret)
        api_key = request.api_key or trading_manager.binance_api_key
        api_secret = request.api_secret or trading_manager.binance_api_secret
        if not api_key or not api_secret:
            logging.error("Live BUY attempted without Binance API credentials.")
            raise HTTPException(status_code=400, detail="Binance API key/secret required")

        current_price = get_price(request.symbol)
        if current_price is None:
            raise HTTPException(status_code=500, detail="Unable to fetch price for symbol")

        # Calculate quantity based on amount and current price
        quantity = request.amount / current_price
        order_response = trading_manager.place_spot_order(
            request.symbol,
            "BUY",
            amount=request.amount,
            api_key=api_key,
            api_secret=api_secret,
        )
        if not order_response:
            logging.error(f"Live BUY failed for {request.symbol}")
            raise HTTPException(status_code=500, detail="Binance order failed")
        fill = trading_manager._extract_order_fill(order_response)
        if fill["qty"] > 0:
            quantity = fill["qty"]
            if fill["price"]:
                current_price = fill["price"]

        return {
            "success": True,
            "message": f"Successfully bought {quantity:.8f} {request.symbol} at ${current_price:.8f}",
            "order": order_response
        }
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            raise HTTPException(status_code=400, detail=f"Invalid symbol: {request.symbol}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/trade/sell", response_model=dict)
def sell_coin(request: TradeRequest = Body(...)):
    """
    Endpoint to sell a coin using Binance API.
    Requires api_key and api_secret for authentication.
    """
    try:
        if request.api_key and request.api_secret:
            trading_manager.set_binance_credentials(request.api_key, request.api_secret)
        api_key = request.api_key or trading_manager.binance_api_key
        api_secret = request.api_secret or trading_manager.binance_api_secret
        if not api_key or not api_secret:
            logging.error("Live SELL attempted without Binance API credentials.")
            raise HTTPException(status_code=400, detail="Binance API key/secret required")

        current_price = get_price(request.symbol)
        if current_price is None:
            raise HTTPException(status_code=500, detail="Unable to fetch price for symbol")

        # Calculate quantity based on amount and current price
        quantity = request.amount / current_price
        order_response = trading_manager.place_spot_order(
            request.symbol,
            "SELL",
            quantity=quantity,
            api_key=api_key,
            api_secret=api_secret,
        )
        if not order_response:
            logging.error(f"Live SELL failed for {request.symbol}")
            raise HTTPException(status_code=500, detail="Binance order failed")
        fill = trading_manager._extract_order_fill(order_response)
        if fill["qty"] > 0:
            quantity = fill["qty"]
            if fill["price"]:
                current_price = fill["price"]

        return {
            "success": True,
            "message": f"Successfully sold {quantity:.8f} {request.symbol} at ${current_price:.8f}",
            "order": order_response
        }
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 400:
            raise HTTPException(status_code=400, detail=f"Invalid symbol: {request.symbol}")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)


# ========== AI Patterns & Market Data Endpoints ==========
from fastapi import Query

@app.get("/api/market/candles/latest", response_model=list)
def api_latest_candles(symbol: str = Query(...), timeframe: str = Query("1m"), limit: int = Query(100, ge=1, le=1000)):
    """Return recent candles for a symbol/timeframe from the `candles` table."""
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        # inline validated integer for LIMIT to avoid driver-specific placeholders
        sql = f"SELECT ts, open, high, low, close, volume FROM candles WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT {int(limit)}"
        cur.execute(_q(sql, pg), (symbol, timeframe))
        rows = cur.fetchall()
        cols = ["ts","open","high","low","close","volume"]
        data = [dict(zip(cols, r)) for r in rows]
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass


@app.get("/api/ai/patterns/recent", response_model=list)
def api_recent_patterns(symbol: Optional[str] = Query(None), direction: Optional[str] = Query(None), limit: int = Query(50, ge=1, le=500)):
    """Return recent incremental/decremental pattern detections."""
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        base = "SELECT symbol, timeframe, direction, score, pct_change, consistency, volatility, volume_z, detected_at, features_json FROM pattern_events"
        clauses = []
        params = []
        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol)
        if direction:
            clauses.append("direction = ?")
            params.append(direction)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"{base}{where} ORDER BY detected_at DESC LIMIT {int(limit)}"
        cur.execute(_q(sql, pg), params)
        cols = ["symbol", "timeframe", "direction", "score", "pct_change", "consistency", "volatility", "volume_z", "detected_at", "features_json"]
        rows = cur.fetchall()
        out = []
        for r in rows:
            item = dict(zip(cols, r))
            try:
                item["features"] = json.loads(item.pop("features_json") or "{}")
            except Exception:
                item["features"] = {}
            out.append(item)
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass


@app.get("/api/ai/regime/latest", response_model=list)
def api_regime_latest(symbol: Optional[str] = Query(None), limit: int = Query(100, ge=1, le=500)):
    """Return latest regime labels for symbols."""
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        base = "SELECT symbol, timeframe, ts, regime, confidence, model_version, curve_location, trend FROM regime_states"
        clauses = []
        params = []
        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"{base}{where} ORDER BY ts DESC LIMIT {int(limit)}"
        cur.execute(_q(sql, pg), params)
        cols = ["symbol", "timeframe", "ts", "regime", "confidence", "model_version", "curve_location", "trend"]
        rows = cur.fetchall()
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass


@app.get("/api/ai/decisions/recent", response_model=list)
def api_ai_decisions(symbol: Optional[str] = Query(None), limit: int = Query(100, ge=1, le=500)):
    """Recent AI ensemble decisions with risk flags."""
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        base = "SELECT symbol, timeframe, intention, confidence, expected_return, regime, pattern_score, risk_blocked, created_at FROM ai_decisions"
        clauses = []
        params = []
        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"{base}{where} ORDER BY created_at DESC LIMIT {int(limit)}"
        cur.execute(_q(sql, pg), params)
        cols = ["symbol","timeframe","intention","confidence","expected_return","regime","pattern_score","risk_blocked","created_at"]
        rows = cur.fetchall()
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass


@app.get("/api/ai/backtests", response_model=list)
def api_ai_backtests(limit: int = Query(50, ge=1, le=200)):
    """Recent backtest runs for AI models."""
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        sql = f"SELECT id, model_name, started_at, completed_at, samples, sharpe, win_rate, avg_return, notes FROM backtest_runs ORDER BY id DESC LIMIT {int(limit)}"
        cur.execute(_q(sql, pg))
        cols = ["id","model_name","started_at","completed_at","samples","sharpe","win_rate","avg_return","notes"]
        rows = cur.fetchall()
        out = []
        for r in rows:
            item = dict(zip(cols, r))
            out.append(item)
        return out
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass


@app.post("/api/zones/refresh", response_model=dict)
def api_zones_refresh(timeframe: str = Query("1m")):
    """Run a lightweight zone detection for all tracked symbols and persist to zones table."""
    try:
        conn, cur = get_database_connection()
        cur.execute("SELECT symbol FROM coin_monitor")
        syms = [r[0] for r in cur.fetchall()]
        cur.close(); conn.close()
        count = run_zone_detection(syms, timeframe=timeframe)
        return {"inserted": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/zones", response_model=list)
def api_zones(symbol: Optional[str] = Query(None), timeframe: Optional[str] = Query(None), limit: int = Query(200, ge=1, le=1000)):
    """List zones with optional filters."""
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        base = (
            "SELECT id, symbol, timeframe, zone_type, formation, proximal, distal, "
            "base_start_ts, base_end_ts, leg_in_ts, leg_out_ts, quality_basic, "
            "quality_adv, quality_label, probability_label, rr_est, curve_location, "
            "trend, freshness, tests, opposing_dist, opposing_zone_id, confluence, lotl, trap, created_at, last_tested_at FROM zones"
        )
        clauses = []
        params = []
        if symbol:
            clauses.append("symbol = ?")
            params.append(symbol)
        if timeframe:
            clauses.append("timeframe = ?")
            params.append(timeframe)
        where_clause = ""
        if clauses:
            where_clause = " WHERE " + " AND ".join(clauses)

        sql = f"{base}{where_clause} ORDER BY created_at DESC LIMIT {int(limit)}"
        cur.execute(_q(sql, pg), params)
        cols = ["id","symbol","timeframe","zone_type","formation","proximal","distal","base_start_ts","base_end_ts","leg_in_ts","leg_out_ts","quality_basic","quality_adv","quality_label","probability_label","rr_est","curve_location","trend","freshness","tests","opposing_dist","opposing_zone_id","confluence","lotl","trap","created_at","last_tested_at"]
        rows = cur.fetchall()
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass


@app.post("/api/trading/plan", response_model=dict)
def api_trading_plan(request: EntryPlanRequest = Body(...)):
    """Suggest an entry/stop/tp and size for a given zone."""
    zone = get_zone_by_id(request.zone_id)
    if not zone:
        raise HTTPException(status_code=404, detail="Zone not found")
    try:
        plan = plan_entry_for_zone(zone, balance=request.balance, risk_perc=request.risk_perc, rr_target=request.rr_target)
        plan_id = persist_entry_plan(plan)
        if plan_id:
            plan["entry_plan_id"] = plan_id
        return plan
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/entry-plans", response_model=list)
def api_entry_plans(symbol: Optional[str] = Query(None), limit: int = Query(100, ge=1, le=1000)):
    """List entry plans (most recent first)."""
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        base = "SELECT id, zone_id, symbol, timeframe, entry_type, entry_price, stop_price, take_profit_price, rr_target, risk_perc, balance, position_size, risk_amount, atr_used, buffer_used, status, created_at FROM entry_plans"
        params = []
        where = ""
        if symbol:
            where = " WHERE symbol = ?"
            params.append(symbol)
        sql = f"{base}{where} ORDER BY id DESC LIMIT {int(limit)}"
        cur.execute(_q(sql, pg), params)
        cols = ["id","zone_id","symbol","timeframe","entry_type","entry_price","stop_price","take_profit_price","rr_target","risk_perc","balance","position_size","risk_amount","atr_used","buffer_used","status","created_at"]
        rows = cur.fetchall()
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass

@app.get("/api/market/features/latest", response_model=list)
def api_latest_features(symbol: str = Query(...), timeframe: str = Query("1m"), limit: int = Query(100, ge=1, le=1000)):
    """Return recent computed features for a symbol/timeframe from the `features` table."""
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        sql = f"""
            SELECT ts, ema7, ema25, ema_slope,
                   ret_1, ret_5, ret_15,
                   ret_z1, ret_z5, ret_z15,
                   volatility, vol_z, rsi, macd, macd_signal, macd_hist,
                   boll_width, atr, body_pct, is_boring
            FROM features
            WHERE symbol = ? AND timeframe = ?
            ORDER BY ts DESC LIMIT {int(limit)}
        """
        cur.execute(_q(sql, pg), (symbol, timeframe))
        rows = cur.fetchall()
        cols = ["ts","ema7","ema25","ema_slope",
                "ret_1","ret_5","ret_15",
                "ret_z1","ret_z5","ret_z15",
                "volatility","vol_z","rsi","macd","macd_signal","macd_hist",
                "boll_width","atr","body_pct","is_boring"]
        data = [dict(zip(cols, r)) for r in rows]
        return data
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass

@app.get("/api/patterns", response_model=list)
def api_patterns(limit: int = Query(200, ge=1, le=1000)):
    """List discovered pattern clusters (placeholder until discovery job populates)."""
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        sql = f"SELECT id, symbol, timeframe, algo, centroid_json, cluster_size, avg_return, volatility, label, created_at FROM pattern_clusters ORDER BY id DESC LIMIT {int(limit)}"
        cur.execute(_q(sql, pg))
        rows = cur.fetchall()
        cols = ["id","symbol","timeframe","algo","centroid_json","cluster_size","avg_return","volatility","label","created_at"]
        return [dict(zip(cols, r)) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass

@app.get("/api/patterns/active", response_model=dict)
def api_patterns_active(symbol: str = Query(...), timeframe: str = Query("1m")):
    """Return the latest assignment for a symbol/timeframe, if any (placeholder)."""
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        # Choose the row with the latest end_ts or start_ts if end_ts is NULL
        sql = """
            SELECT pattern_id, symbol, timeframe, start_ts, end_ts, features_json, performance
            FROM pattern_assignments
            WHERE symbol = ? AND timeframe = ?
            ORDER BY COALESCE(end_ts, start_ts) DESC
            LIMIT 1
        """
        cur.execute(_q(sql, pg), (symbol, timeframe))
        row = cur.fetchone()
        if not row:
            return {}
        cols = ["pattern_id","symbol","timeframe","start_ts","end_ts","features_json","performance"]
        return dict(zip(cols, row))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass

@app.post("/api/patterns/discover", response_model=dict)
def api_patterns_discover(symbol: str = Query(...)):
    """Run k-means discovery immediately for the given symbol (current timeframe)."""
    try:
        n = pattern_discovery.discover_for(symbol)
        return {"ok": True, "clusters": n}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/regime/current", response_model=dict)
def api_regime_current(symbol: str = Query(...), timeframe: str = Query("1m")):
    """Return latest regime state row for symbol/timeframe (placeholder until classifier runs)."""
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        sql = "SELECT ts, regime, confidence, model_version, curve_location, trend FROM regime_states WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT 1"
        cur.execute(_q(sql, pg), (symbol, timeframe))
        row = cur.fetchone()
        if not row:
            return {}
        cols = ["ts","regime","confidence","model_version","curve_location","trend"]
        return dict(zip(cols, row))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass


@app.get("/api/predictions", response_model=list)
def api_predictions(timeframe: str = Query("1m"), limit: int = Query(100, ge=1, le=1000)):
    """
    Rank symbols by a simple bullish momentum score derived from the latest active
    pattern assignment and its cluster metrics.

    Returns list of dicts: { symbol, timeframe, pattern_id, avg_return, volatility,
    cluster_size, win_rate, regime, score_pct } sorted by score desc.
    """
    try:
        lim = int(limit)
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        # 1) Get list of symbols we know about
        cur.execute(_q("SELECT symbol FROM coin_monitor", pg))
        syms = [r[0] for r in cur.fetchall()]
        results = []
        for sym in syms:
            # Latest assignment for this symbol/timeframe
            sql_ass = _q(
                """
                SELECT pattern_id, start_ts, end_ts, performance
                FROM pattern_assignments
                WHERE symbol = ? AND timeframe = ?
                ORDER BY end_ts DESC LIMIT 1
                """,
                pg,
            )
            cur.execute(sql_ass, (sym, timeframe))
            row = cur.fetchone()
            if not row:
                continue
            pattern_id, start_ts, end_ts, perf = row
            # Cluster metrics
            cur.execute(_q("SELECT avg_return, volatility, cluster_size, label FROM pattern_clusters WHERE id = ?", pg), (pattern_id,))
            cl = cur.fetchone()
            if not cl:
                continue
            avg_ret, vol, size, label = cl
            # Latest regime for context
            cur.execute(
                _q("SELECT regime FROM regime_states WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT 1", pg),
                (sym, timeframe),
            )
            row_reg = cur.fetchone()
            regime = row_reg[0] if row_reg else None
            # heuristic score
            score = (avg_ret or 0.0) - (vol or 0.0) * 0.5 + (size or 0) * 0.001
            results.append(
                {
                    "symbol": sym,
                    "timeframe": timeframe,
                    "pattern_id": pattern_id,
                    "avg_return": avg_ret,
                    "volatility": vol,
                    "cluster_size": size,
                    "win_rate": perf,
                    "regime": regime,
                    "score_pct": round(score * 100, 2) if score is not None else None,
                    "label": label,
                }
            )
        # sort and trim
        results = sorted(results, key=lambda x: x.get("score_pct") or 0, reverse=True)[:lim]
        return results
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass

@app.get("/api/logs/recent", response_model=list)
def api_logs_recent(lines: int = Query(200, ge=1, le=1000), filter_text: Optional[str] = Query(None)):
    """
    Return tail of api.log. Optional filter_text performs simple substring filter.
    """
    log_path = pathlib.Path("api.log")
    if not log_path.exists():
        return []
    try:
        data = log_path.read_text(errors="ignore").splitlines()
        tail = data[-int(lines):]
        if filter_text:
            tail = [ln for ln in tail if filter_text.lower() in ln.lower()]
        return tail
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/health/summary", response_model=dict)
def api_health_summary():
    """
    Lightweight status/metrics for UI monitoring.
    """
    try:
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        # counts
        cur.execute("SELECT COUNT(1) FROM trade_logs")
        trades = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT COUNT(1) FROM zones")
        zones = int(cur.fetchone()[0] or 0)
        cur.execute("SELECT COUNT(1) FROM entry_plans")
        plans = int(cur.fetchone()[0] or 0)
        # latest candle ts
        cur.execute("SELECT symbol, MAX(ts) FROM candles GROUP BY symbol")
        latest = {row[0]: int(row[1]) for row in cur.fetchall()}
        status = {
            "trading_enabled": trading_manager.enabled,
            "strategy_mode": trading_manager.strategy_mode,
            "coin_brain_symbol": trading_manager.coin_brain_symbol,
            "hybrid_enabled": getattr(trading_manager, "hybrid_enabled", True),
            "trade_logs": trades,
            "zones": zones,
            "entry_plans": plans,
            "latest_candles": latest,
        }
        return status
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass


@app.post("/api/trading/hybrid", response_model=dict)
def api_trading_hybrid(payload: dict = Body(...)):
    """Enable/disable the hybrid multi-symbol loop."""
    enabled = payload.get("enabled")
    if enabled is None:
        raise HTTPException(status_code=400, detail="enabled is required")
    try:
        trading_manager.set_hybrid_enabled(bool(enabled))
        return {"hybrid_enabled": trading_manager.hybrid_enabled}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    try:
        lim = int(limit)
        conn, cur = get_database_connection()
        pg = is_pg(cur)
        # 1) Get list of symbols we know about
        cur.execute("SELECT symbol FROM coin_monitor")
        syms = [r[0] for r in cur.fetchall()]
        results = []
        for sym in syms:
            # Latest assignment for this symbol/timeframe
            sql_ass = _q(
                """
                SELECT pattern_id, start_ts, end_ts, performance
                FROM pattern_assignments
                WHERE symbol = ? AND timeframe = ?
                ORDER BY COALESCE(end_ts, start_ts) DESC
                LIMIT 1
                """,
                pg,
            )
            cur.execute(sql_ass, (sym, timeframe))
            a = cur.fetchone()
            if not a:
                continue
            pattern_id = int(a[0])
            perf_latest = float(a[3] or 0.0)

            # Cluster stats
            sql_pc = _q(
                "SELECT avg_return, volatility, cluster_size FROM pattern_clusters WHERE id = ?",
                pg,
            )
            cur.execute(sql_pc, (pattern_id,))
            pc = cur.fetchone()
            if not pc:
                continue
            avg_ret = float(pc[0] or 0.0)
            vol = float(pc[1] or 0.0)
            csize = int(pc[2] or 0)

            # Win-rate for this pattern
            sql_wr_num = _q(
                "SELECT COUNT(1) FROM pattern_assignments WHERE pattern_id = ? AND performance > 0",
                pg,
            )
            sql_wr_den = _q(
                "SELECT COUNT(1) FROM pattern_assignments WHERE pattern_id = ?",
                pg,
            )
            cur.execute(sql_wr_num, (pattern_id,))
            wr_num = int(cur.fetchone()[0] or 0)
            cur.execute(sql_wr_den, (pattern_id,))
            wr_den = int(cur.fetchone()[0] or 1)
            win_rate = (wr_num / wr_den) if wr_den else 0.0

            # Latest regime for context
            sql_reg = _q(
                "SELECT regime FROM regime_states WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT 1",
                pg,
            )
            cur.execute(sql_reg, (sym, timeframe))
            row_reg = cur.fetchone()
            regime = row_reg[0] if row_reg else None

            # Scoring: sigmoid(avg_ret / (vol+eps)) * (0.5 + 0.5*win_rate) * size_weight
            eps = 1e-9
            denom = vol if vol and vol > 0 else eps
            ratio = avg_ret / denom
            # simple sigmoid
            try:
                import math
                sig = 1.0 / (1.0 + math.exp(-ratio))
            except Exception:
                sig = 0.5
            size_w = 0.5 + 0.5 * min(1.0, csize / 100.0)  # weight up to size 100
            score = sig * (0.5 + 0.5 * win_rate) * size_w
            score_pct = round(max(0.0, min(1.0, score)) * 100.0, 2)

            results.append({
                "symbol": sym,
                "timeframe": timeframe,
                "pattern_id": pattern_id,
                "avg_return": avg_ret,
                "volatility": vol,
                "cluster_size": csize,
                "win_rate": round(win_rate, 4),
                "regime": regime,
                "score_pct": score_pct,
                "perf_latest": perf_latest,
            })
        # Sort desc by score and trim
        results.sort(key=lambda x: x.get("score_pct", 0), reverse=True)
        return results[:lim]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        try:
            cur.close(); conn.close()
        except Exception:
            pass
