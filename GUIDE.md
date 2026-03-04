# Trishul Watch - User Guide

This guide will help you run the Trishul Watch application and view the UI in your browser. You can choose between SQLite (simpler) or PostgreSQL (more powerful) for the database.

## Prerequisites

- Python 3.7 or higher
- Node.js and npm (for the frontend)
- Git (to clone the repository)
- Docker and Docker Compose (optional, for containerized setup)
- PostgreSQL (optional, for advanced database setup)

## Option 1: Quick Start with SQLite (Recommended for most users)

### Step 1: Run the Backend API Server

1. Open a terminal window
2. Navigate to the project directory:
   ```bash
   cd /Users/harsha/Downloads/coin_price_monitor_project
   ```
3. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   # On macOS/Linux
   source venv/bin/activate

   # On Windows
   venv\Scripts\activate
   ```
4. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
5. Start the API server:
   ```bash
   python run.py --api
   ```

   You should see output indicating that the server is running, typically on http://0.0.0.0:8000

### Step 2: Run the Frontend Development Server

1. Open a new terminal window (keep the backend server running)
2. Navigate to the project directory:
   ```bash
   cd /Users/harsha/Downloads/coin_price_monitor_project
   ```
3. Navigate to the frontend directory:
   ```bash
   cd frontend
   ```
4. Install dependencies (this step is REQUIRED):
   ```bash
   npm install
   ```
   This command will create a node_modules directory and install all required dependencies, including react-scripts.

5. Start the React development server:
   ```bash
   npm start
   ```

   This will automatically open your browser to http://localhost:3000

## Option 2: Using PostgreSQL Database

### Step 1: Set Up PostgreSQL

1. Install PostgreSQL if you haven't already:
   ```bash
   # On Ubuntu/Debian
   sudo apt-get install postgresql postgresql-contrib

   # On macOS with Homebrew
   brew install postgresql
   ```

2. Start the PostgreSQL service:
   ```bash
   # On Ubuntu/Debian
   sudo service postgresql start

   # On macOS with Homebrew
   brew services start postgresql
   ```

3. Create a database:
   ```bash
   createdb coin_monitor
   ```

4. Initialize the database schema:
   ```bash
   psql -d coin_monitor -f app/create_tables.sql
   ```

### Step 2: Run the Backend with PostgreSQL

1. Set environment variables to use PostgreSQL:
   ```bash
   # On macOS/Linux
   export DB_HOST=localhost
   export DB_USER=postgres
   export DB_PASSWORD=postgres
   export DB_NAME=coin_monitor
   export DB_PORT=5432

   # On Windows
   set DB_HOST=localhost
   set DB_USER=postgres
   set DB_PASSWORD=postgres
   set DB_NAME=coin_monitor
   set DB_PORT=5432
   ```

2. Start the API server:
   ```bash
   python run.py --api
   ```

3. Follow the same frontend setup steps as in Option 1.

## Option 3: Using Docker (Easiest for full stack setup)

1. Make sure Docker and Docker Compose are installed:
   ```bash
   docker --version
   docker-compose --version
   ```

2. Ensure Docker is running:
   - **On macOS**: Open Docker Desktop from the Applications folder or Launchpad
   - **On Windows**: Make sure Docker Desktop is running (check the whale icon in the system tray)
   - **On Linux**: Verify the Docker service is active with `sudo systemctl status docker`

   You can verify Docker is running properly with:
   ```bash
   docker info
   ```

3. Navigate to the project directory:
   ```bash
   cd /Users/harsha/Downloads/coin_price_monitor_project
   ```

4. Build and start the containers:
   ```bash
   docker-compose up --build
   ```

   This will start:
   - PostgreSQL database
   - Backend API server
   - Frontend development server

4. Access the application:
   - Frontend: http://localhost:3000
   - API: http://localhost:8000

5. To stop the containers:
   ```bash
   docker-compose down
   ```

## Using the Application

Once both servers are running, you can:

1. View the list of monitored cryptocurrencies on the left side of the screen
2. Use the tabs to filter between All, Rising, and Falling coins
3. Click on any cryptocurrency to view its detailed information on the right side
4. See real-time price updates (the data refreshes automatically every 5 seconds)
5. View historical price data for each cryptocurrency

## Troubleshooting

### If the backend server fails to start:

- Make sure port 8000 is not already in use
- Check the logs for any error messages
- Ensure you have all the required Python dependencies installed:
  ```bash
  pip install -r requirements.txt
  ```

### If the frontend server fails to start:

- Make sure port 3000 is not already in use
- Check that Node.js and npm are properly installed:
  ```bash
  node --version
  npm --version
  ```
- If you see "react-scripts: command not found" error, it means the dependencies are not installed. Run:
  ```bash
  npm install
  ```
  This is a crucial step and must be completed before running npm start.

### If using PostgreSQL:

- Make sure PostgreSQL is running:
  ```bash
  # On Ubuntu/Debian
  sudo service postgresql status

  # On macOS with Homebrew
  brew services list
  ```
- Check that the database exists:
  ```bash
  psql -l
  ```
- Verify your connection settings match the environment variables

### If using Docker:

- Make sure no other services are using ports 3000, 5433, or 8000
- Check container logs:
  ```bash
  docker-compose logs
  ```
- Ensure Docker has enough resources allocated (memory, CPU)

## Additional Information

- The application automatically fetches all USDT trading pairs from Binance
- Data is refreshed every 2 seconds from the Binance API
- The UI updates every 5 seconds to show the latest data
- Price history tracks cycles of price movements, with a cycle completing when price drops 0.5% from its high
