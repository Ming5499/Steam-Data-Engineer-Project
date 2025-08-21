FROM python:3.10-slim

# Set working dir
WORKDIR /app

# Avoid Python writing .pyc files + flush stdout
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

# Install system deps
RUN apt-get update && apt-get install -y \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    prefect==3.4.9 \
    requests \
    pandas \
    mysql-connector-python \
    kafka-python \
    numpy \
    python-dotenv \
    aiohttp  \
    pymongo \
    dbt-core \
    dbt-mysql
    

# Copy project files
COPY flows/ ./flows/
COPY tasks/ ./tasks/
COPY utils/ ./utils/
COPY config/ ./config/ 
COPY prefect.yaml ./

# Default command (overridden by docker-compose)
CMD ["prefect", "worker", "start", "--pool", "default-agent-pool", "--type", "process"]