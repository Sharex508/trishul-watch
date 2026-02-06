# Trishul Watch

A standalone application for monitoring cryptocurrency prices in real-time, updating the database every 20 seconds with the latest prices from Binance API. Includes a React.js frontend for visualizing the data.

## Features

- Automatically retrieves all USDT trading pairs from Binance API
- Updates prices every 20 seconds in real-time
- Categorizes coins as rising or falling based on price changes
- Shows percentage gain/loss for each coin
- Tracks price history by storing up to 10 sets of low and high prices in cycles
- A cycle is completed when the price falls by more than 0.5% from its high point
- When a cycle completes, the history is updated and a new cycle begins
- Provides API endpoints to retrieve the price history data
- Runs as a background thread in the FastAPI application
- Includes a React.js frontend for visualizing coin prices and history
- Supports both SQLite and PostgreSQL databases
- Can be run locally or using Docker

## Installation

### Option 1: Local Installation

1. Clone the repository:
```bash
git clone https://github.com/Sharex508/trishul-watch.git
cd trishul-watch
```

2. Create a virtual environment and install dependencies for the backend:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

3. Install dependencies for the frontend:
```bash
cd frontend
npm install
cd ..
```

Note: The application uses SQLite as the database, which doesn't require any additional setup. The database file will be created automatically in the app directory.

### Option 2: Using Docker

1. Clone the repository:
```bash
git clone https://github.com/Sharex508/trishul-watch.git
cd trishul-watch
```

2. Run with Docker Compose:
```bash
docker-compose up
```

This will start both the API server and the React frontend. The API will be available at http://localhost:8000 and the frontend will be available at http://localhost:3000.

Note: The database will be reset when Docker is brought down with `docker-compose down`. This means all coin data and price history will be cleared and reinitialized when the containers are started again.

### Option 3: Using the Run Script

The project includes a run.py script that can help with setup and running:

1. Initialize the database:
```bash
python run.py --init-db
```

2. Run the test script:
```bash
python run.py --test
```

3. Run the API server:
```bash
python run.py --api
```

## Usage

### Running the API Server

Start the FastAPI application:
```bash
cd app
uvicorn main:app --reload
```

The API will be available at http://localhost:8000

### Running the Frontend

Start the React development server:
```bash
cd frontend
npm start
```

The frontend will be available at http://localhost:3000

### Running the Test Script

To test the price monitoring functionality:
```bash
cd app
python test_coin_price_monitor.py
```

This will start the price monitor and print the data from the coin_monitor table after 10 seconds and again after 10 more seconds.

### API Endpoints

The following API endpoints are available:

- `GET /api/coin-monitors`: Get all coin monitor records
- `GET /api/coin-monitors/{symbol}`: Get a specific coin's monitoring data by symbol
- `GET /api/coin-monitors/{symbol}/history`: Get the price history for a specific coin
- `PUT /api/coin-monitors/{symbol}`: Update a coin's monitoring data
- `POST /api/coin-monitors/update-prices`: Manually trigger a price update for all coins
- `POST /api/coin-monitors/add`: Add a new coin to monitor
- `POST /api/coin-monitors/force-update-history`: Force update all coins' price history with varied values
- `POST /api/coin-monitors/update-initial-prices`: Update the initial prices for all coins to match the current prices

### Example API Requests

#### Get all coin monitors
```bash
curl -X GET "http://localhost:8000/api/coin-monitors"
```

#### Get a specific coin's monitoring data
```bash
curl -X GET "http://localhost:8000/api/coin-monitors/BTCUSDT"
```

#### Get a coin's price history
```bash
curl -X GET "http://localhost:8000/api/coin-monitors/BTCUSDT/history"
```

#### Update a coin's monitoring data
```bash
curl -X PUT "http://localhost:8000/api/coin-monitors/BTCUSDT" \
  -H "Content-Type: application/json" \
  -d '{"high_price": 55000.0, "low_price": 45000.0}'
```

#### Manually update all prices
```bash
curl -X POST "http://localhost:8000/api/coin-monitors/update-prices"
```

#### Add a new coin to monitor
```bash
curl -X POST "http://localhost:8000/api/coin-monitors/add" \
  -H "Content-Type: application/json" \
  -d '{"symbol": "DOGEUSDT"}'
```

#### Update initial prices to match current prices
```bash
curl -X POST "http://localhost:8000/api/coin-monitors/update-initial-prices"
```

## Price History Format

The `/api/coin-monitors/{symbol}/history` endpoint returns a structured representation of the price history:

```json
{
  "symbol": "BTCUSDT",
  "initial_price": 50000.0,
  "current": {
    "low_price": 48000.0,
    "high_price": 52000.0,
    "latest_price": 51000.0
  },
  "history": [
    {
      "set": 1,
      "low_price": 47000.0,
      "high_price": 53000.0
    },
    {
      "set": 2,
      "low_price": 45000.0,
      "high_price": 55000.0
    },
    ...
  ],
  "created_at": "2023-06-01T12:00:00",
  "updated_at": "2023-06-02T12:00:00"
}
```

The history array contains up to 10 sets of low and high prices, with the most recent changes first. Only sets with non-zero values are included in the response.

## How It Works

1. **Initialization**:
   - When the application starts, the `initialize_coin_monitor()` function is called
   - It initializes the coin_monitor table with default coins or specified coins
   - After initialization, the `update_initial_prices()` function is called
   - This function updates the initial_price, low_price, high_price, and latest_price to match the current price from the Binance API
   - This ensures that after a Docker restart, the initial prices match the current prices

2. **Price Updates**:
   - Every 20 seconds, the `update_coin_prices()` function is called
   - It fetches the latest prices from Binance API
   - For each coin in the coin_monitor table:
     - It retrieves the current high_price and low_price
     - If the latest price is higher than the current high_price, it updates high_price
     - If the latest price is lower than the current low_price, it updates low_price
     - It updates the latest_price with the current price
     - It calls `update_price_history()` to check if the price history needs to be updated

3. **Price History Tracking**:
   - The `update_price_history()` function is called for each coin during price updates
   - It tracks price cycles for each coin:
     - A cycle begins when a coin's price starts being monitored
     - During a cycle, the high and low prices are continuously updated
     - A cycle is completed when the price falls by more than 0.5% from its high point
     - When a cycle completes, the current high and low prices are stored in history
   - When a cycle completes, it shifts all history values down
   - Each cycle is initialized with slightly different values to ensure unique price history
   - The application ensures that all cycles have varied values, even during normal updates
   - This creates a history of price cycles over time, capturing the volatility of each coin

## License

This project is licensed under the MIT License - see the LICENSE file for details.
