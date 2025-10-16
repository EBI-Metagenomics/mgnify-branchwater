# syntax=docker/dockerfile:1
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install system deps if needed in the future
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app/metadata

# Copy requirements first for better caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY attrcounts_4.5percent.csv ./
COPY prepare_bq.py prepare_sra.py load_duckdb.py run.py ./

# Data volume for inputs/outputs and credentials
#VOLUME ["/data/bw_db"]

# Default command shows help; user selects subcommand
#ENTRYPOINT ["python", "/app/metadata/run.py"]
ENTRYPOINT ["sleep", "infinity"]
#CMD ["--help"]
