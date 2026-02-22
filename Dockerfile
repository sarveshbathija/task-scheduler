FROM python:3.11-slim

WORKDIR /app

# Install curl (needed by clear_tv_watchlist.py for TradingView API calls)
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy scheduler and default task config
COPY scheduler.py .
COPY tasks.json .
RUN chmod +x scheduler.py

# Create log directory
RUN mkdir -p /var/log

# Run scheduler
CMD ["python3", "scheduler.py"]
