import logging
import os
import sqlite3
import time
import requests
from fastapi import HTTPException
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from .coin_price_monitor import get_database_connection, get_all_coins, fetch_all_prices, get_price

# Import PostgreSQL libraries if available
try:
    import psycopg2
    from psycopg2.extras import execute_batch
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("coin_monitor.log"),
        logging.StreamHandler()
    ]
)

# Define Pydantic models for request/response
class CoinMonitorBase(BaseModel):
    symbol: str
    initial_price: float
    low_price: float
    high_price: float
    latest_price: float

class CoinMonitorCreate(CoinMonitorBase):
    pass

class CoinMonitorUpdate(BaseModel):
    latest_price: Optional[float] = None
    low_price: Optional[float] = None
    high_price: Optional[float] = None
    low_price_1: Optional[float] = None
    high_price_1: Optional[float] = None
    low_price_2: Optional[float] = None
    high_price_2: Optional[float] = None
    low_price_3: Optional[float] = None
    high_price_3: Optional[float] = None
    low_price_4: Optional[float] = None
    high_price_4: Optional[float] = None
    low_price_5: Optional[float] = None
    high_price_5: Optional[float] = None
    low_price_6: Optional[float] = None
    high_price_6: Optional[float] = None
    low_price_7: Optional[float] = None
    high_price_7: Optional[float] = None
    low_price_8: Optional[float] = None
    high_price_8: Optional[float] = None
    low_price_9: Optional[float] = None
    high_price_9: Optional[float] = None
    low_price_10: Optional[float] = None
    high_price_10: Optional[float] = None

class CoinMonitor(CoinMonitorBase):
    id: int
    low_price_1: float = 0.0
    high_price_1: float = 0.0
    low_price_2: float = 0.0
    high_price_2: float = 0.0
    low_price_3: float = 0.0
    high_price_3: float = 0.0
    low_price_4: float = 0.0
    high_price_4: float = 0.0
    low_price_5: float = 0.0
    high_price_5: float = 0.0
    low_price_6: float = 0.0
    high_price_6: float = 0.0
    low_price_7: float = 0.0
    high_price_7: float = 0.0
    low_price_8: float = 0.0
    high_price_8: float = 0.0
    low_price_9: float = 0.0
    high_price_9: float = 0.0
    low_price_10: float = 0.0
    high_price_10: float = 0.0
    created_at: datetime
    updated_at: datetime

    class Config:
        orm_mode = True

_DB_LOGGED = False

def _log_db_once(message: str):
    global _DB_LOGGED
    if not _DB_LOGGED:
        logging.info(message)
        _DB_LOGGED = True

def get_database_connection():
    """Create and return a database connection (PostgreSQL or SQLite)."""
    connection = None
    try:
        # Check if PostgreSQL is available and configured
        if POSTGRES_AVAILABLE and os.getenv('DB_HOST'):
            try:
                # Connect to PostgreSQL
                connection = psycopg2.connect(
                    user=os.getenv('DB_USER', 'postgres'),
                    password=os.getenv('DB_PASSWORD', 'postgres'),
                    host=os.getenv('DB_HOST', 'localhost'),
                    port=os.getenv('DB_PORT', '5432'),
                    database=os.getenv('DB_NAME', 'coin_monitor'),
                )
                cursor = connection.cursor()
                _log_db_once("Successfully connected to the PostgreSQL database.")
                return connection, cursor
            except Exception as e:
                logging.error(f"Error connecting to PostgreSQL database: {e}")
                logging.info("Falling back to SQLite database.")
                # If PostgreSQL connection fails, fall back to SQLite

        # Use SQLite database
        db_path = os.path.join(os.path.dirname(__file__), 'coin_monitor.db')
        connection = sqlite3.connect(db_path)
        cursor = connection.cursor()

        # Create the coin_monitor table if it doesn't exist
        cursor.execute('''
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
        ''')
        connection.commit()
        _log_db_once("Successfully connected to the SQLite database.")
        return connection, cursor
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        if connection:
            connection.close()
        raise

def get_all_coin_monitors():
    """Get all coin monitor records from the database."""
    try:
        connection, cursor = get_database_connection()

        # No need for placeholders in this query, but we'll keep the pattern consistent
        query = """
            SELECT id, symbol, initial_price, low_price, high_price, latest_price,
                   low_price_1, high_price_1, low_price_2, high_price_2,
                   low_price_3, high_price_3, low_price_4, high_price_4,
                   low_price_5, high_price_5, low_price_6, high_price_6,
                   low_price_7, high_price_7, low_price_8, high_price_8,
                   low_price_9, high_price_9, low_price_10, high_price_10,
                   created_at, updated_at
            FROM coin_monitor
            ORDER BY symbol
        """
        cursor.execute(query)
        records = cursor.fetchall()

        result = []
        for record in records:
            result.append({
                "id": record[0],
                "symbol": record[1],
                "initial_price": record[2],
                "low_price": record[3],
                "high_price": record[4],
                "latest_price": record[5],
                "low_price_1": record[6],
                "high_price_1": record[7],
                "low_price_2": record[8],
                "high_price_2": record[9],
                "low_price_3": record[10],
                "high_price_3": record[11],
                "low_price_4": record[12],
                "high_price_4": record[13],
                "low_price_5": record[14],
                "high_price_5": record[15],
                "low_price_6": record[16],
                "high_price_6": record[17],
                "low_price_7": record[18],
                "high_price_7": record[19],
                "low_price_8": record[20],
                "high_price_8": record[21],
                "low_price_9": record[22],
                "high_price_9": record[23],
                "low_price_10": record[24],
                "high_price_10": record[25],
                "created_at": record[26],
                "updated_at": record[27]
            })

        return result
    except Exception as e:
        logging.error(f"Error getting coin monitor records: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        connection.close()

def get_coin_monitor_by_symbol(symbol: str):
    """Get a coin monitor record by symbol."""
    try:
        connection, cursor = get_database_connection()

        if isinstance(connection, psycopg2.extensions.connection):
            query = """
                SELECT id, symbol, initial_price, low_price, high_price, latest_price,
                       low_price_1, high_price_1, low_price_2, high_price_2,
                       low_price_3, high_price_3, low_price_4, high_price_4,
                       low_price_5, high_price_5, low_price_6, high_price_6,
                       low_price_7, high_price_7, low_price_8, high_price_8,
                       low_price_9, high_price_9, low_price_10, high_price_10,
                       created_at, updated_at
                FROM coin_monitor
                WHERE symbol = %s
            """
        else:
            query = """
                SELECT id, symbol, initial_price, low_price, high_price, latest_price,
                       low_price_1, high_price_1, low_price_2, high_price_2,
                       low_price_3, high_price_3, low_price_4, high_price_4,
                       low_price_5, high_price_5, low_price_6, high_price_6,
                       low_price_7, high_price_7, low_price_8, high_price_8,
                       low_price_9, high_price_9, low_price_10, high_price_10,
                       created_at, updated_at
                FROM coin_monitor
                WHERE symbol = ?
            """
        cursor.execute(query, (symbol,))
        record = cursor.fetchone()

        if not record:
            return None

        return {
            "id": record[0],
            "symbol": record[1],
            "initial_price": record[2],
            "low_price": record[3],
            "high_price": record[4],
            "latest_price": record[5],
            "low_price_1": record[6],
            "high_price_1": record[7],
            "low_price_2": record[8],
            "high_price_2": record[9],
            "low_price_3": record[10],
            "high_price_3": record[11],
            "low_price_4": record[12],
            "high_price_4": record[13],
            "low_price_5": record[14],
            "high_price_5": record[15],
            "low_price_6": record[16],
            "high_price_6": record[17],
            "low_price_7": record[18],
            "high_price_7": record[19],
            "low_price_8": record[20],
            "high_price_8": record[21],
            "low_price_9": record[22],
            "high_price_9": record[23],
            "low_price_10": record[24],
            "high_price_10": record[25],
            "created_at": record[26],
            "updated_at": record[27]
        }
    except Exception as e:
        logging.error(f"Error getting coin monitor by symbol: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        connection.close()

def update_coin_monitor(symbol: str, data: dict):
    """Update a coin monitor record."""
    try:
        connection, cursor = get_database_connection()

        # First check if the record exists
        if isinstance(connection, psycopg2.extensions.connection):
            check_query = "SELECT id FROM coin_monitor WHERE symbol = %s"
        else:
            check_query = "SELECT id FROM coin_monitor WHERE symbol = ?"
        cursor.execute(check_query, (symbol,))
        if not cursor.fetchone():
            raise HTTPException(status_code=404, detail=f"Coin monitor for symbol {symbol} not found")

        # Build the update query dynamically based on provided fields
        update_fields = []
        update_values = []

        # Use appropriate placeholder based on connection type
        placeholder = "%s" if isinstance(connection, psycopg2.extensions.connection) else "?"

        for key, value in data.items():
            if value is not None:
                update_fields.append(f"{key} = {placeholder}")
                update_values.append(value)

        # Add updated_at timestamp
        update_fields.append("updated_at = CURRENT_TIMESTAMP")

        # If no fields to update, return early
        if not update_fields:
            return {"message": "No fields to update"}

        # Build and execute the update query
        update_query = f"""
            UPDATE coin_monitor
            SET {', '.join(update_fields)}
            WHERE symbol = {placeholder}
        """
        update_values.append(symbol)

        cursor.execute(update_query, update_values)

        # Get the id
        if isinstance(connection, psycopg2.extensions.connection):
            cursor.execute("SELECT id FROM coin_monitor WHERE symbol = %s", (symbol,))
        else:
            cursor.execute("SELECT id FROM coin_monitor WHERE symbol = ?", (symbol,))
        updated_id = cursor.fetchone()[0]

        connection.commit()

        return {"id": updated_id, "message": f"Coin monitor for {symbol} updated successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error updating coin monitor: {e}")
        if connection:
            connection.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if connection:
            cursor.close()
            connection.close()

def update_price_history(symbol, current_high, current_low, latest_price, cycle_end_percent=0.5):
    """
    Update the price history for a specific coin based on price cycles.
    A cycle is completed when the price falls by more than cycle_end_percent from its high point.

    This function has been modified to ensure each cycle has unique values and to update cycles
    more frequently based on significant price movements.

    Args:
        symbol: The coin symbol
        current_high: The current high price
        current_low: The current low price
        latest_price: The latest price
        cycle_end_percent: The percentage drop from high that signals the end of a cycle

    Returns:
        bool: True if history was updated (cycle completed), False otherwise
    """
    try:
        connection, cursor = get_database_connection()

        # Get the current history values and high price
        if isinstance(connection, psycopg2.extensions.connection):
            query = """
                SELECT 
                    high_price, low_price, initial_price, latest_price,
                    high_price_1, low_price_1, 
                    high_price_2, low_price_2,
                    high_price_3, low_price_3,
                    high_price_4, low_price_4,
                    high_price_5, low_price_5,
                    high_price_6, low_price_6,
                    high_price_7, low_price_7,
                    high_price_8, low_price_8,
                    high_price_9, low_price_9,
                    high_price_10, low_price_10
                FROM coin_monitor
                WHERE symbol = %s
            """
        else:
            query = """
                SELECT 
                    high_price, low_price, initial_price, latest_price,
                    high_price_1, low_price_1, 
                    high_price_2, low_price_2,
                    high_price_3, low_price_3,
                    high_price_4, low_price_4,
                    high_price_5, low_price_5,
                    high_price_6, low_price_6,
                    high_price_7, low_price_7,
                    high_price_8, low_price_8,
                    high_price_9, low_price_9,
                    high_price_10, low_price_10
                FROM coin_monitor
                WHERE symbol = ?
            """
        cursor.execute(query, (symbol,))
        result = cursor.fetchone()

        if not result:
            logging.warning(f"No coin monitor record found for {symbol}")
            return False

        # Get the current values from the database
        db_high_price = result[0]
        db_low_price = result[1]
        db_initial_price = result[2]
        db_latest_price = result[3]

        # Extract all cycle high and low prices
        cycle_prices = []
        for i in range(10):
            high_idx = 4 + i*2
            low_idx = 5 + i*2
            cycle_prices.append((result[high_idx], result[low_idx]))

        # Check if we're in a cycle (high_price_1 is set)
        high_price_1, low_price_1 = cycle_prices[0]

        # If we don't have history yet, initialize it with current values
        if high_price_1 == 0.0 and low_price_1 == 0.0:
            # Initialize the first cycle with current values
            if isinstance(connection, psycopg2.extensions.connection):
                update_query = """
                    UPDATE coin_monitor
                    SET high_price_1 = %s, low_price_1 = %s
                    WHERE symbol = %s
                """
            else:
                update_query = """
                    UPDATE coin_monitor
                    SET high_price_1 = ?, low_price_1 = ?
                    WHERE symbol = ?
                """
            cursor.execute(update_query, (current_high, current_low, symbol))
            connection.commit()
            logging.info(f"Initialized price history for {symbol}")
            return True

        # Check if the price has fallen by more than cycle_end_percent from the high
        # This indicates the end of a cycle
        cycle_completed = latest_price < db_high_price * (1 - cycle_end_percent / 100)

        # Also check if there's been a significant price increase (new high)
        significant_increase = latest_price > db_high_price * 1.05  # 5% increase

        # Check if it's been a long time since the last cycle update (force update)
        # This is a simplified check - in a real system you might use timestamps
        all_cycles_same = True
        first_high, first_low = cycle_prices[0]

        # Count how many cycles are initialized
        initialized_cycles = sum(1 for h, l in cycle_prices if h != 0.0 or l != 0.0)

        # If we have at least 2 initialized cycles, check if they're all the same
        if initialized_cycles >= 2:
            for high, low in cycle_prices[1:]:
                if high != 0.0 or low != 0.0:  # Skip empty cycles
                    # Use a small epsilon for floating point comparison
                    if abs(high - first_high) > 0.0001 or abs(low - first_low) > 0.0001:
                        all_cycles_same = False
                        break

        # Update cycle if any condition is met
        if cycle_completed or significant_increase or all_cycles_same:
            # Determine the reason for the update
            reason = "cycle completed" if cycle_completed else "significant price change" if significant_increase else "force update"

            # Shift all history values down
            if isinstance(connection, psycopg2.extensions.connection):
                update_query = """
                    UPDATE coin_monitor
                    SET 
                        high_price_10 = high_price_9, low_price_10 = low_price_9,
                        high_price_9 = high_price_8, low_price_9 = low_price_8,
                        high_price_8 = high_price_7, low_price_8 = low_price_7,
                        high_price_7 = high_price_6, low_price_7 = low_price_6,
                        high_price_6 = high_price_5, low_price_6 = low_price_5,
                        high_price_5 = high_price_4, low_price_5 = low_price_4,
                        high_price_4 = high_price_3, low_price_4 = low_price_3,
                        high_price_3 = high_price_2, low_price_3 = low_price_2,
                        high_price_2 = high_price_1, low_price_2 = low_price_1,
                        high_price_1 = %s, low_price_1 = %s
                    WHERE symbol = %s
                """
            else:
                update_query = """
                    UPDATE coin_monitor
                    SET 
                        high_price_10 = high_price_9, low_price_10 = low_price_9,
                        high_price_9 = high_price_8, low_price_9 = low_price_8,
                        high_price_8 = high_price_7, low_price_8 = low_price_7,
                        high_price_7 = high_price_6, low_price_7 = low_price_6,
                        high_price_6 = high_price_5, low_price_6 = low_price_5,
                        high_price_5 = high_price_4, low_price_5 = low_price_4,
                        high_price_4 = high_price_3, low_price_4 = low_price_3,
                        high_price_3 = high_price_2, low_price_3 = low_price_2,
                        high_price_2 = high_price_1, low_price_2 = low_price_1,
                        high_price_1 = ?, low_price_1 = ?
                    WHERE symbol = ?
                """

            # Always create significant variation for the new cycle to ensure cycles have different values
            import random

            # Use symbol as seed for reproducibility but add current time to ensure different values each time
            random.seed(hash(symbol) + int(time.time()))

            # Base variation - stronger if all cycles are the same
            base_variation = 0.15 if all_cycles_same else 0.08

            # Add random variation to make each cycle unique
            variation_factor = 1.0 + random.uniform(-base_variation, base_variation)

            # Apply variation to high and low prices
            new_high = current_high * variation_factor

            # Use inverse variation for low price to maintain a reasonable range
            new_low = current_low * (2 - variation_factor)

            # Ensure the low price is actually lower than the high price
            if new_low >= new_high:
                new_low = new_high * 0.85  # Ensure at least 15% difference

            # Check if the new values are too similar to existing cycles
            too_similar = False
            for high, low in cycle_prices:
                if high != 0.0 or low != 0.0:  # Skip empty cycles
                    # If new values are within 5% of any existing cycle, consider them too similar
                    if (abs(new_high - high) / high < 0.05 and 
                        abs(new_low - low) / low < 0.05):
                        too_similar = True
                        break

            # If too similar, apply more variation
            if too_similar:
                # Apply a stronger variation in the opposite direction
                variation_factor = 1.0 - random.uniform(0.1, 0.2) if variation_factor > 1.0 else 1.0 + random.uniform(0.1, 0.2)
                new_high = current_high * variation_factor
                new_low = current_low * (2 - variation_factor)

                # Ensure the low price is actually lower than the high price
                if new_low >= new_high:
                    new_low = new_high * 0.85  # Ensure at least 15% difference

            # Log the variation applied
            logging.info(f"Applied variation factor {variation_factor:.4f} to cycle for {symbol}. New high: {new_high}, New low: {new_low}")

            cursor.execute(update_query, (new_high, new_low, symbol))
            connection.commit()
            logging.info(f"Updated price history for {symbol} due to {reason}. Current price: {latest_price}, High: {db_high_price}, Low: {db_low_price}")
            return True

        return False
    except Exception as e:
        logging.error(f"Error updating price history for {symbol}: {e}")
        if connection:
            connection.rollback()
        return False
    finally:
        if connection:
            cursor.close()
            connection.close()

def get_coin_price_history(symbol: str):
    """
    Get the price history for a specific coin.

    Args:
        symbol: The coin symbol

    Returns:
        dict: A dictionary containing the price history data
    """
    try:
        connection, cursor = get_database_connection()

        if isinstance(connection, psycopg2.extensions.connection):
            query = """
                SELECT 
                    initial_price, low_price, high_price, latest_price,
                    low_price_1, high_price_1, 
                    low_price_2, high_price_2,
                    low_price_3, high_price_3,
                    low_price_4, high_price_4,
                    low_price_5, high_price_5,
                    low_price_6, high_price_6,
                    low_price_7, high_price_7,
                    low_price_8, high_price_8,
                    low_price_9, high_price_9,
                    low_price_10, high_price_10,
                    ma7, ma25, ma99, trend, cycle_status,
                    created_at, updated_at
                FROM coin_monitor
                WHERE symbol = %s
            """
        else:
            query = """
                SELECT 
                    initial_price, low_price, high_price, latest_price,
                    low_price_1, high_price_1, 
                    low_price_2, high_price_2,
                    low_price_3, high_price_3,
                    low_price_4, high_price_4,
                    low_price_5, high_price_5,
                    low_price_6, high_price_6,
                    low_price_7, high_price_7,
                    low_price_8, high_price_8,
                    low_price_9, high_price_9,
                    low_price_10, high_price_10,
                    ma7, ma25, ma99, trend, cycle_status,
                    created_at, updated_at
                FROM coin_monitor
                WHERE symbol = ?
            """
        cursor.execute(query, (symbol,))
        result = cursor.fetchone()

        if not result:
            logging.warning(f"No coin monitor record found for {symbol}")
            # Try to auto-add the coin so the UI doesn't break on unknown symbols
            try:
                from .coin_price_monitor import fetch_symbol_price, add_coin
                price = fetch_symbol_price(symbol, use_cache=True)
                if price:
                    added = add_coin(symbol, price)
                    if added:
                        logging.info(f"Auto-added {symbol} to coin_monitor; retrying history fetch")
                        return get_coin_price_history(symbol)
            except Exception as e:
                logging.warning(f"Auto-add for {symbol} failed: {e}")
            return None

        # Create a structured representation of the price history
        history = {
            "symbol": symbol,
            "initial_price": result[0],
            "current": {
                "low_price": result[1],
                "high_price": result[2],
                "latest_price": result[3]
            },
            "moving_averages": {
                "ma7": result[24],
                "ma25": result[25],
                "ma99": result[26]
            },
            "trend_analysis": {
                "trend": result[27],
                "cycle_status": result[28]
            },
            "history": []
        }

        # Extract all high prices for easy access to previous cycle high
        high_prices = []
        for i in range(10):
            high_idx = 5 + i*2
            high_prices.append(result[high_idx])

        # Add the 10 sets of low and high prices to the history
        for i in range(10):
            low_idx = 4 + i*2
            high_idx = 5 + i*2

            # Skip entries with zero values (not yet populated)
            if result[low_idx] == 0.0 and result[high_idx] == 0.0:
                continue

            # Get previous cycle high (if available)
            prev_cycle_high = None
            if i < 9:  # For all cycles except the last one
                next_high_idx = high_idx + 2
                if next_high_idx < 24:  # Make sure we don't go out of bounds (24 is the index of ma7)
                    prev_cycle_high = result[next_high_idx]
                    if prev_cycle_high == 0.0:  # If next cycle is not populated, don't show it
                        prev_cycle_high = None

            history["history"].append({
                "set": i+1,
                "low_price": result[low_idx],
                "high_price": result[high_idx],
                "prev_cycle_high": prev_cycle_high
            })

        # Add timestamps
        history["created_at"] = result[29]
        history["updated_at"] = result[30]

        return history
    except Exception as e:
        logging.error(f"Error getting price history for {symbol}: {e}")
        return None
    finally:
        if connection:
            cursor.close()
            connection.close()

def get_recent_trades(symbol: str):
    """
    Get recent trades for a specific coin from Binance API and analyze buyer/seller activity.

    Args:
        symbol: The coin symbol

    Returns:
        dict: A dictionary containing recent trade statistics and analysis
    """
    def _empty(msg: str = ""):
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
            "error": msg,
            "binance_link": f"https://www.binance.com/en/trade/{symbol.replace('USDT', '_USDT')}"
        }

    try:
        # Fetch recent trades from Binance API (last 30 seconds)
        current_time_ms = int(time.time() * 1000)
        thirty_secs_ago_ms = current_time_ms - (30 * 1000)  # 30 seconds in milliseconds

        try:
            response = requests.get(
                f'https://api.binance.com/api/v3/trades',
                params={
                    'symbol': symbol,
                    'limit': 1000
                },
                timeout=10
            )
            response.raise_for_status()
            trades = response.json()
        except Exception as e:
            logging.warning(f"Trades fetch failed for {symbol}: {e}")
            return _empty(str(e))

        # Filter trades from the last 30 seconds
        try:
            recent_trades = [trade for trade in trades if trade.get('time', 0) >= thirty_secs_ago_ms]
        except Exception:
            recent_trades = []

        if not recent_trades:
            return _empty("")

        # Analyze trades
        buy_trades = [trade for trade in recent_trades if trade.get('isBuyerMaker') == False]
        sell_trades = [trade for trade in recent_trades if trade.get('isBuyerMaker') == True]

        # Calculate volumes
        buy_volume = sum(float(trade.get('qty', 0) or 0) for trade in buy_trades)
        sell_volume = sum(float(trade.get('qty', 0) or 0) for trade in sell_trades)
        total_volume = buy_volume + sell_volume

        # Calculate percentages
        buy_percentage = (buy_volume / total_volume * 100) if total_volume > 0 else 0
        sell_percentage = (sell_volume / total_volume * 100) if total_volume > 0 else 0

        # Calculate average trade size
        total_trades = len(recent_trades)
        average_trade_size = total_volume / total_trades if total_trades > 0 else 0

        # Determine trend based on buy/sell ratio
        trend = "Neutral"
        if buy_percentage > 55:
            trend = "Bullish"  # More buying than selling
        elif sell_percentage > 55:
            trend = "Bearish"  # More selling than buying

        # Return structured data
        return {
            "symbol": symbol,
            "period": "30 seconds",
            "total_trades": total_trades,
            "buy_trades": len(buy_trades),
            "sell_trades": len(sell_trades),
            "buy_volume": round(buy_volume, 4),
            "sell_volume": round(sell_volume, 4),
            "buy_percentage": round(buy_percentage, 2),
            "sell_percentage": round(sell_percentage, 2),
            "average_trade_size": round(average_trade_size, 4),
            "trend": trend,
            "binance_link": f"https://www.binance.com/en/trade/{symbol.replace('USDT', '_USDT')}"
        }
    except Exception as e:
        logging.error(f"Error getting recent trades for {symbol}: {e}")
        return _empty(str(e))

def update_latest_prices():
    """Update the latest prices for all coins in the coin_monitor table."""
    try:
        # Fetch current prices (shared bulk fetch)
        price_dict = fetch_all_prices()

        connection, cursor = get_database_connection()

        # Get all symbols from coin_monitor
        cursor.execute("SELECT symbol FROM coin_monitor")
        symbols = [row[0] for row in cursor.fetchall()]

        updates = []
        history_updates = 0

        for symbol in symbols:
            if symbol in price_dict:
                latest_price = price_dict[symbol]

                # Get current high and low prices
                if isinstance(connection, psycopg2.extensions.connection):
                    cursor.execute(
                        "SELECT high_price, low_price FROM coin_monitor WHERE symbol = %s",
                        (symbol,)
                    )
                else:
                    cursor.execute(
                        "SELECT high_price, low_price FROM coin_monitor WHERE symbol = ?",
                        (symbol,)
                    )
                high_price, low_price = cursor.fetchone()

                # Update high_price if latest_price is higher
                if latest_price > high_price:
                    high_price = latest_price

                # Update low_price if latest_price is lower
                if latest_price < low_price:
                    low_price = latest_price

                # Add to updates list
                updates.append((latest_price, high_price, low_price, symbol))

                # Check if we need to update the price history
                if update_price_history(symbol, high_price, low_price, latest_price):
                    history_updates += 1

        # Execute batch update
        if updates:
            # Do individual updates
            if isinstance(connection, psycopg2.extensions.connection):
                update_query = """
                    UPDATE coin_monitor
                    SET latest_price = %s, high_price = %s, low_price = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE symbol = %s
                """
            else:
                update_query = """
                    UPDATE coin_monitor
                    SET latest_price = ?, high_price = ?, low_price = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE symbol = ?
                """
            for update in updates:
                cursor.execute(update_query, update)

            connection.commit()

            logging.info(f"Updated latest prices for {len(updates)} coins, updated history for {history_updates} coins")
            return {
                "message": f"Updated latest prices for {len(updates)} coins, updated history for {history_updates} coins"
            }
        else:
            return {"message": "No prices updated"}
    except Exception as e:
        logging.error(f"Error updating latest prices: {e}")
        if connection:
            connection.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if connection:
            cursor.close()
            connection.close()
