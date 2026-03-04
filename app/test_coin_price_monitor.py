import time
import logging
import psycopg2
import os
from coin_price_monitor import start_price_monitor, get_database_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def print_coin_monitor_data():
    """Print the data from the coin_monitor table."""
    try:
        connection, cursor = get_database_connection()
        query = """
            SELECT symbol, initial_price, low_price, high_price, latest_price,
                   low_price_1, high_price_1
            FROM coin_monitor
            LIMIT 10
        """
        cursor.execute(query)
        records = cursor.fetchall()
        
        print("\n=== Coin Monitor Data ===")
        print("Symbol | Initial Price | Low Price | High Price | Latest Price | Low Price 1 | High Price 1")
        print("-" * 90)
        
        for record in records:
            symbol, initial_price, low_price, high_price, latest_price, low_price_1, high_price_1 = record
            print(f"{symbol} | {initial_price:.6f} | {low_price:.6f} | {high_price:.6f} | {latest_price:.6f} | {low_price_1:.6f} | {high_price_1:.6f}")
        
        print("=" * 90)
        return True
    except Exception as e:
        logging.error(f"Error printing coin monitor data: {e}")
        return False
    finally:
        if connection:
            cursor.close()
            connection.close()

def main():
    """Test the coin price monitor."""
    logging.info("Starting coin price monitor test")
    
    # Start the price monitor in a separate thread
    monitor_thread = start_price_monitor()
    
    # Wait for a few seconds to let it initialize and update prices
    logging.info("Waiting for 10 seconds to let the monitor initialize and update prices...")
    time.sleep(10)
    
    # Print the data from the coin_monitor table
    print_coin_monitor_data()
    
    # Wait a bit more to see updates
    logging.info("Waiting for 10 more seconds to see updates...")
    time.sleep(10)
    
    # Print the data again to see the updates
    print_coin_monitor_data()
    
    logging.info("Test completed. The price monitor thread will continue running in the background.")
    logging.info("Press Ctrl+C to exit.")
    
    # Keep the main thread alive to let the monitor thread run
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Test terminated by user.")

if __name__ == "__main__":
    main()