#!/usr/bin/env python3
"""
Script to initialize the database and run the application.
"""
import os
import sys
import argparse
import subprocess
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)

def create_database():
    """Create the PostgreSQL database if it doesn't exist."""
    try:
        # Check if psql is installed
        try:
            # Check if database exists
            db_name = os.getenv('DB_NAME', 'coin_monitor')
            result = subprocess.run(
                ['psql', '-lqt'],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )

            if db_name in result.stdout:
                logging.info(f"Database '{db_name}' already exists.")
            else:
                # Create database
                subprocess.run(['createdb', db_name], check=True)
                logging.info(f"Created database '{db_name}'.")

            return True
        except FileNotFoundError:
            logging.error("PostgreSQL is not installed or 'psql' command is not in PATH.")
            logging.error("Please install PostgreSQL or add it to your PATH.")
            logging.error("You can still run the API server, but database functionality will not work.")
            return True  # Return True to allow the application to continue
    except subprocess.CalledProcessError as e:
        logging.error(f"Error creating database: {e}")
        return False

def initialize_tables():
    """Initialize the database tables."""
    try:
        db_name = os.getenv('DB_NAME', 'coin_monitor')
        sql_file = os.path.join('app', 'create_tables.sql')

        if not os.path.exists(sql_file):
            logging.error(f"SQL file not found: {sql_file}")
            return False

        try:
            subprocess.run(['psql', '-d', db_name, '-f', sql_file], check=True)
            logging.info("Initialized database tables.")
            return True
        except FileNotFoundError:
            logging.error("PostgreSQL is not installed or 'psql' command is not in PATH.")
            logging.error("Please install PostgreSQL or add it to your PATH.")
            logging.error("You can still run the API server, but database functionality will not work.")
            return True  # Return True to allow the application to continue
    except subprocess.CalledProcessError as e:
        logging.error(f"Error initializing tables: {e}")
        return False

def run_api():
    """Run the FastAPI application."""
    try:
        project_root = os.path.dirname(os.path.abspath(__file__))
        host = os.getenv('API_HOST', '0.0.0.0')
        port = int(os.getenv('API_PORT', '8001'))  # Changed default port to 8001
        debug = os.getenv('DEBUG', 'True').lower() in ('true', '1', 't')

        # Try up to 10 different ports if the specified one is already in use
        max_port_attempts = 10
        for port_attempt in range(max_port_attempts):
            try:
                current_port = port + port_attempt
                cmd = [sys.executable, '-m', 'uvicorn', 'app.main:app', '--host', host, '--port', str(current_port)]
                if debug:
                    cmd.append('--reload')

                logging.info(f"Starting API server at http://{host}:{current_port}")
                subprocess.run(cmd, check=True, cwd=project_root)
                return True
            except subprocess.CalledProcessError as e:
                if port_attempt < max_port_attempts - 1:
                    logging.warning(f"Port {current_port} is already in use. Trying port {current_port + 1}...")
                else:
                    logging.error(f"Failed to find an available port after {max_port_attempts} attempts.")
                    raise e
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running API server: {e}")
        return False

def run_test():
    """Run the test script."""
    try:
        os.chdir('app')
        logging.info("Running test script...")
        subprocess.run(['python', 'test_coin_price_monitor.py'], check=True)
        return True
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running test script: {e}")
        return False

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Run the Trishul Watch application.')
    parser.add_argument('--init-db', action='store_true', help='Initialize the database')
    parser.add_argument('--test', action='store_true', help='Run the test script')
    parser.add_argument('--api', action='store_true', help='Run the API server')

    args = parser.parse_args()

    # Load environment variables from .env file if available
    try:
        from dotenv import load_dotenv
        load_dotenv()
        logging.info("Loaded environment variables from .env file.")
    except ImportError:
        logging.warning("python-dotenv not installed. Using default environment variables.")

    # If no arguments provided, show help
    if not (args.init_db or args.test or args.api):
        parser.print_help()
        return

    # Initialize database if requested
    if args.init_db:
        if not create_database():
            return
        if not initialize_tables():
            return

    # Run test if requested
    if args.test:
        if not run_test():
            return

    # Run API if requested
    if args.api:
        if not run_api():
            return

if __name__ == '__main__':
    main()
