# Docker Build Guide for Trishul Watch

This guide provides detailed instructions on how to build and run the Trishul Watch application using Docker.

## Prerequisites

Before you begin, make sure you have the following installed on your system:

- Docker: [Get Docker](https://docs.docker.com/get-docker/)
- Docker Compose: [Get Docker Compose](https://docs.docker.com/compose/install/)

### Ensuring Docker is Running

#### On macOS
Docker Desktop must be running before you can use Docker commands:

1. Open Docker Desktop from your Applications folder or the Launchpad
2. Wait for Docker Desktop to start (the whale icon in the menu bar will stop animating)
3. Verify Docker is running with:
   ```bash
   docker info
   ```

If you see the error `Cannot connect to the Docker daemon`, it means Docker Desktop is not running.

#### On Windows
Ensure Docker Desktop is running (check for the whale icon in the system tray).

#### On Linux
Make sure the Docker service is running:
```bash
sudo systemctl status docker
```

If it's not running, start it with:
```bash
sudo systemctl start docker
```

You can verify your installations by running:
```bash
docker --version
docker-compose --version
```

## Project Structure

The Trishul Watch application consists of three main services:

1. **PostgreSQL Database**: Stores all cryptocurrency data and price history
2. **Backend API Server**: FastAPI application that fetches data from Binance and provides API endpoints
3. **Frontend Development Server**: React application that displays the cryptocurrency data

Each service is defined in the `docker-compose.yml` file and has its own Dockerfile.

## Building and Running with Docker Compose

### Step 1: Clone the Repository (if you haven't already)

```bash
git clone https://github.com/Sharex508/trishul-watch.git
cd trishul-watch
```

### Step 2: Build and Start the Containers

From the project root directory, run:

```bash
docker-compose up --build
```

This command does the following:

- Builds the Docker images for each service using their respective Dockerfiles
- Creates and starts containers for each service
- Sets up the network between containers
- Configures environment variables
- Maps ports from the containers to your host machine

The `--build` flag ensures that Docker rebuilds the images, which is important when you make changes to the code.

### Step 3: Access the Application

Once all containers are running, you can access:

- Frontend: http://localhost:3000
- API: http://localhost:8000
- API Documentati![Screenshot 2025-09-08 at 9.09.35 PM.png](../../Desktop/Screenshot%202025-09-08%20at%209.09.35%E2%80%AFPM.png)on: http://localhost:8000/docs

### Step 4: Stop the Containers

To stop the running containers, press `Ctrl+C` in the terminal where docker-compose is running.

To stop and remove the containers, networks, and volumes, run:

```bash
docker-compose down
```

## Understanding the Docker Setup

### PostgreSQL Service

The PostgreSQL service uses the official PostgreSQL 13 image and:
- Sets up a database named `coin_monitor`
- Initializes the database schema using the `create_tables.sql` script
- Exposes port 5433 for database connections
- Includes a health check to ensure the database is ready before starting dependent services

### Backend API Service

The backend service:
- Uses Python 3.9 as the base image
- Installs all dependencies from `requirements.txt`
- Connects to the PostgreSQL database
- Runs the FastAPI application on port 8000
- Includes a health check to ensure the API is ready before starting dependent services

### Frontend Service

The frontend service:
- Uses Node.js 16 as the base image
- Installs all dependencies from `package.json`
- Mounts the frontend code as a volume for live reloading during development
- Runs the React development server on port 3000

## Running in Production

For a production environment, you might want to make the following changes:

1. For the frontend, build a production-optimized bundle:
   - Modify the frontend Dockerfile to run `npm run build` instead of `npm start`
   - Serve the static files using Nginx or a similar web server

2. For the backend, consider:
   - Adding proper authentication
   - Configuring HTTPS
   - Setting up proper logging
   - Implementing database backups

## Troubleshooting

### Common Issues

1. **Docker daemon not running**: If you see an error like `Cannot connect to the Docker daemon at unix:///Users/username/.docker/run/docker.sock. Is the docker daemon running?`, it means Docker Desktop is not running:

   - **On macOS**: Open Docker Desktop from the Applications folder or click the Docker icon in the menu bar and select "Start Docker Desktop"
   - **On Windows**: Open Docker Desktop from the Start menu or system tray
   - **On Linux**: Start the Docker service with `sudo systemctl start docker`

2. **Port conflicts**: Make sure ports 3000, 5433, and 8000 are not already in use by other applications.

3. **Container fails to start**: Check the logs for error messages:
   ```bash
   docker-compose logs
   ```

   To see logs for a specific service:
   ```bash
   docker-compose logs postgres
   docker-compose logs api
   docker-compose logs frontend
   ```

4. **Database connection issues**: Ensure the PostgreSQL container is healthy:
   ```bash
   docker-compose ps
   ```

   The health status should show as "healthy" for all services.

5. **Changes not reflecting**: If you've made changes to the code and they're not reflecting:
   - For the frontend, changes should automatically reload due to the volume mount
   - For the backend, you may need to rebuild the container:
     ```bash
     docker-compose up --build api
     ```

### Resetting the Database

If you need to reset the database to its initial state:

```bash
docker-compose down
docker volume prune  # This will remove all unused volumes
docker-compose up --build
```

## Additional Docker Commands

- **View running containers**:
  ```bash
  docker-compose ps
  ```

- **Execute commands in a running container**:
  ```bash
  docker-compose exec api bash
  docker-compose exec postgres psql -U postgres -d coin_monitor
  docker-compose exec frontend npm install <package-name>
  ```

- **View container logs in real-time**:
  ```bash
  docker-compose logs -f
  ```

- **Rebuild a specific service**:
  ```bash
  docker-compose up --build <service-name>
  ```

## Conclusion

Using Docker with Docker Compose makes it easy to run the entire Trishul Watch application stack with minimal setup. The containerized approach ensures consistency across different environments and simplifies the development workflow.
