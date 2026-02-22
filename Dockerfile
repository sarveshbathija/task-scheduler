FROM python:3.11-slim

WORKDIR /app

# Install system dependencies:
#   curl          - needed by clear_tv_watchlist.py for TradingView API calls
#   postgresql-client - pg_dump for TeslaMate backups
#   default-mysql-client - mysqldump for stocks backups
RUN apt-get update && \
    apt-get install -y curl postgresql-client default-mysql-client && \
    rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scheduler, backup script, and default task config
COPY scheduler.py .
COPY backup.py .
COPY tasks.json .
RUN chmod +x scheduler.py backup.py

# Create log directory
RUN mkdir -p /var/log

# Run scheduler
CMD ["python3", "scheduler.py"]
