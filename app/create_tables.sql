-- Create the coin_monitor table
CREATE TABLE IF NOT EXISTS coin_monitor (
    id              SERIAL PRIMARY KEY,
    symbol          TEXT NOT NULL,
    initial_price   FLOAT NOT NULL,
    low_price       FLOAT NOT NULL,
    high_price      FLOAT NOT NULL,
    latest_price    FLOAT NOT NULL,
    low_price_1     FLOAT DEFAULT 0.0,
    high_price_1    FLOAT DEFAULT 0.0,
    low_price_2     FLOAT DEFAULT 0.0,
    high_price_2    FLOAT DEFAULT 0.0,
    low_price_3     FLOAT DEFAULT 0.0,
    high_price_3    FLOAT DEFAULT 0.0,
    low_price_4     FLOAT DEFAULT 0.0,
    high_price_4    FLOAT DEFAULT 0.0,
    low_price_5     FLOAT DEFAULT 0.0,
    high_price_5    FLOAT DEFAULT 0.0,
    low_price_6     FLOAT DEFAULT 0.0,
    high_price_6    FLOAT DEFAULT 0.0,
    low_price_7     FLOAT DEFAULT 0.0,
    high_price_7    FLOAT DEFAULT 0.0,
    low_price_8     FLOAT DEFAULT 0.0,
    high_price_8    FLOAT DEFAULT 0.0,
    low_price_9     FLOAT DEFAULT 0.0,
    high_price_9    FLOAT DEFAULT 0.0,
    low_price_10    FLOAT DEFAULT 0.0,
    high_price_10   FLOAT DEFAULT 0.0,
    ma7             FLOAT DEFAULT 0.0,
    ma25            FLOAT DEFAULT 0.0,
    ma99            FLOAT DEFAULT 0.0,
    trend           TEXT DEFAULT 'Neutral',
    cycle_status    TEXT DEFAULT 'Consolidation',
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create a unique index on the symbol column
CREATE UNIQUE INDEX IF NOT EXISTS coin_monitor_symbol_idx ON coin_monitor (symbol);

-- Create a table to store historical price data for moving average calculations
CREATE TABLE IF NOT EXISTS price_history (
    id              SERIAL PRIMARY KEY,
    symbol          TEXT NOT NULL,
    price           FLOAT NOT NULL,
    timestamp       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create an index on the symbol and timestamp columns for faster queries
CREATE INDEX IF NOT EXISTS price_history_symbol_timestamp_idx ON price_history (symbol, timestamp);

-- No sample data - all coins will be initialized with current prices from Binance API
