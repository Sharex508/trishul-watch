from fastapi import FastAPI, HTTPException, Body, BackgroundTasks
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
from .coin_price_monitor import start_price_monitor, add_coin, force_update_all_price_histories, update_initial_prices, get_database_connection
from .trading import trading_manager
from .ai_pipeline import start_ai_background_jobs, _is_postgres, _q, pattern_discovery

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("api.log"),
        logging.StreamHandler()
    ]
)

# Create FastAPI app
app = FastAPI(title="Coin Price Monitor API", description="API for monitoring cryptocurrency prices with AI Patterns")

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

# Background task for updating coin prices
price_monitor_thread = None

@app.on_event("startup")
def startup_price_update():
    """Start the background task when the app starts."""
    global price_monitor_thread
    # Start the price monitor that updates every 2 seconds
    price_monitor_thread = start_price_monitor()
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
        return {"enabled": trading_manager.enabled}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/trading/start", response_model=dict)
def trading_start():
    """Start paper-trading loop that operates on live prices updated by the monitor."""
    try:
        trading_manager.start()
        return {"enabled": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/trading/stop", response_model=dict)
def trading_stop():
    """Stop paper-trading loop."""
    try:
        trading_manager.stop()
        return {"enabled": False}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/trading/reset", response_model=dict)
def trading_reset():
    """Clear all paper trade entries (trade_logs) and reset cooldowns."""
    try:
        trading_manager.reset()
        return {"ok": True}
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
        if "error" in trades:
            raise HTTPException(status_code=500, detail=trades["error"])
        return trades
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class AddCoinRequest(BaseModel):
    symbol: str

class TradeRequest(BaseModel):
    symbol: str
    amount: float
    client_id: str
    client_secret: str

@app.post("/api/coin-monitors/add", response_model=dict)
def add_new_coin(request: AddCoinRequest = Body(...)):
    """
    Endpoint to add a new coin to monitor.
    """
    try:
        # Fetch current price from Binance API
        import requests
        response = requests.get(f'https://api.binance.com/api/v3/ticker/price?symbol={request.symbol}', timeout=10)
        response.raise_for_status()
        price_data = response.json()
        price = float(price_data['price'])

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
    Requires client_id and client_secret for authentication.
    """
    try:
        # Get current price from Binance API
        response = requests.get(f'https://api.binance.com/api/v3/ticker/price?symbol={request.symbol}', timeout=10)
        response.raise_for_status()
        price_data = response.json()
        current_price = float(price_data['price'])

        # Calculate quantity based on amount and current price
        quantity = request.amount / current_price

        # In a real implementation, we would use the Binance API to place an order
        # For now, we'll just simulate a successful order

        # Generate a timestamp for the Binance API
        timestamp = int(time.time() * 1000)

        # Create a simulated order response
        order_response = {
            "symbol": request.symbol,
            "orderId": f"simulated_{timestamp}",
            "clientOrderId": f"simulated_{timestamp}",
            "transactTime": timestamp,
            "price": str(current_price),
            "origQty": str(quantity),
            "executedQty": str(quantity),
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "MARKET",
            "side": "BUY"
        }

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
    Requires client_id and client_secret for authentication.
    """
    try:
        # Get current price from Binance API
        response = requests.get(f'https://api.binance.com/api/v3/ticker/price?symbol={request.symbol}', timeout=10)
        response.raise_for_status()
        price_data = response.json()
        current_price = float(price_data['price'])

        # Calculate quantity based on amount and current price
        quantity = request.amount / current_price

        # In a real implementation, we would use the Binance API to place an order
        # For now, we'll just simulate a successful order

        # Generate a timestamp for the Binance API
        timestamp = int(time.time() * 1000)

        # Create a simulated order response
        order_response = {
            "symbol": request.symbol,
            "orderId": f"simulated_{timestamp}",
            "clientOrderId": f"simulated_{timestamp}",
            "transactTime": timestamp,
            "price": str(current_price),
            "origQty": str(quantity),
            "executedQty": str(quantity),
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "MARKET",
            "side": "SELL"
        }

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
        pg = _is_postgres(cur)
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

@app.get("/api/market/features/latest", response_model=list)
def api_latest_features(symbol: str = Query(...), timeframe: str = Query("1m"), limit: int = Query(100, ge=1, le=1000)):
    """Return recent computed features for a symbol/timeframe from the `features` table."""
    try:
        conn, cur = get_database_connection()
        pg = _is_postgres(cur)
        sql = f"SELECT ts, ema7, ema25, ema_slope, ret_1, ret_5, ret_15 FROM features WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT {int(limit)}"
        cur.execute(_q(sql, pg), (symbol, timeframe))
        rows = cur.fetchall()
        cols = ["ts","ema7","ema25","ema_slope","ret_1","ret_5","ret_15"]
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
        pg = _is_postgres(cur)
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
        pg = _is_postgres(cur)
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
        pg = _is_postgres(cur)
        sql = "SELECT ts, regime, confidence, model_version FROM regime_states WHERE symbol = ? AND timeframe = ? ORDER BY ts DESC LIMIT 1"
        cur.execute(_q(sql, pg), (symbol, timeframe))
        row = cur.fetchone()
        if not row:
            return {}
        cols = ["ts","regime","confidence","model_version"]
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
        pg = _is_postgres(cur)
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
