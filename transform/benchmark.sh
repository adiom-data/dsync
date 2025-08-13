#!/bin/bash
set -e

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="benchmark_${TIMESTAMP}.log"

# Function to format duration in HH:MM:SS (works on both macOS and Ubuntu)
format_duration() {
    local duration=$1
    local hours=$((duration / 3600))
    local minutes=$(((duration % 3600) / 60))
    local seconds=$((duration % 60))
    printf "%02d:%02d:%02d" $hours $minutes $seconds
}

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    echo "📁 Loading configuration from .env file..."
    export $(grep -v '^#' .env | xargs)
else
    echo "📁 No .env file found, using environment variables or defaults..."
fi

# Database Configuration with fallbacks
DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-"5433"}
DB_NAME=${DB_NAME:-"tpch"}
DB_USER=${DB_USER:-"postgres"}
DB_PASSWORD=${DB_PASSWORD:-"123456"}
DB_SSL_MODE=${DB_SSL_MODE:-"disable"}

# MongoDB Configuration  
MONGO_HOST=${MONGO_HOST:-"localhost"}
MONGO_PORT=${MONGO_PORT:-"27018"}
MONGO_USER=${MONGO_USER}
MONGO_PASSWORD=${MONGO_PASSWORD}

export SRC="postgresql://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_NAME}?sslmode=${DB_SSL_MODE}"

# Build MongoDB connection string with authentication
if [ -n "$MONGO_USER" ] && [ -n "$MONGO_PASSWORD" ]; then
    export DST="mongodb://${MONGO_USER}:${MONGO_PASSWORD}@${MONGO_HOST}:${MONGO_PORT}"
else
    export DST="mongodb://${MONGO_HOST}:${MONGO_PORT}"
fi


echo "MongoDB connection string: $DST"
echo "Postgres connection string: $SRC"

echo "🚀 Starting benchmark - Results will be logged to: $LOG_FILE"
echo "Benchmark started at: $(date)" | tee "$LOG_FILE"
echo "=========================================" | tee -a "$LOG_FILE"

# Calculate average execution time
TOTAL_TIME=0
for i in {1..10}; do
    START_TIME=$(date +%s)
    bash test_run.sh
    END_TIME=$(date +%s)
    RUN_TIME=$((END_TIME - START_TIME))
    TOTAL_TIME=$((TOTAL_TIME + RUN_TIME))
    echo "Run $i took $RUN_TIME seconds ($(format_duration $RUN_TIME))" | tee -a "$LOG_FILE"
    echo "----------------------------------------" >> "$LOG_FILE"
    mongosh "$DST/public" --eval "db.dropDatabase()"

done
AVERAGE_TIME=$((TOTAL_TIME / 10))
MIN_TIME_FORMATTED=$(format_duration $AVERAGE_TIME)

echo "" | tee -a "$LOG_FILE"
echo "📊 BENCHMARK SUMMARY:" | tee -a "$LOG_FILE"
echo "=========================================" | tee -a "$LOG_FILE"
echo "Total runs: 10" | tee -a "$LOG_FILE"
echo "Total time: $TOTAL_TIME seconds ($(date -u -r $TOTAL_TIME '+%H:%M:%S'))" | tee -a "$LOG_FILE"
echo "Average time: $AVERAGE_TIME seconds ($MIN_TIME_FORMATTED)" | tee -a "$LOG_FILE"
echo "Benchmark completed at: $(date)" | tee -a "$LOG_FILE"
echo "Log file: $LOG_FILE" | tee -a "$LOG_FILE"
