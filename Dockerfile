FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/backend \
    TZ=Europe/Paris

WORKDIR /app

# Install system dependencies
# - libpq-dev for psycopg2
# - curl and libcurl4 for potential scraping needs
# - tzdata for timezone management
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    curl \
    libcurl4 \
    tzdata \
    && ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && \
    python -m spacy download fr_core_news_md

# Copy application code
COPY backend/ ./backend/
COPY frontend/ ./frontend/
# Ensure data directory exists for local logs/sqlite if needed
RUN mkdir -p /app/data

EXPOSE 3080

WORKDIR /app/backend

# The scheduler starts via main.py startup event
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "3080", "--proxy-headers", "--forwarded-allow-ips", "*"]
