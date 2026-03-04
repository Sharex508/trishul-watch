# Use multi-stage build for smaller final image
# Stage 1: Build dependencies
FROM python:3.11-slim AS builder

# Add network configuration to help with IPv6 connectivity issues
ENV DOCKER_OPTS="--ipv6=false"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /app

# Copy only requirements file first to leverage Docker cache
COPY requirements.txt .

# Install build dependencies and Python packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc python3-dev && \
    pip install --no-cache-dir --upgrade pip && \
    pip wheel --no-cache-dir --wheel-dir /app/wheels -r requirements.txt && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Stage 2: Final image
FROM python:3.11-slim

# Add network configuration to help with IPv6 connectivity issues
ENV DOCKER_OPTS="--ipv6=false"
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set the working directory in the container
WORKDIR /app

# Install PostgreSQL client and curl for healthcheck
RUN apt-get update && \
    apt-get install -y --no-install-recommends postgresql-client curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy wheels from builder stage
COPY --from=builder /app/wheels /wheels
# Install Python packages from wheels
RUN pip install --no-cache-dir --no-index --find-links=/wheels/ /wheels/* && \
    rm -rf /wheels

# Copy only necessary files
COPY app/ /app/app/
COPY run.py /app/

# Make port 8000 available to the world outside this container
EXPOSE 8000

# Define environment variable
ENV DB_HOST=postgres
ENV DB_USER=postgres
ENV DB_PASSWORD=postgres
ENV DB_NAME=coin_monitor
ENV DB_PORT=5432
ENV API_HOST=0.0.0.0
ENV API_PORT=8000
ENV DEBUG=False

# Run app when the container launches
CMD ["python", "-m", "app.main"]
