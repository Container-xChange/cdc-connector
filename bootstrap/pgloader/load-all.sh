#!/bin/bash
set -euo pipefail

# Get script and project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Load environment variables
# In production: variables should already be exported via SSH/CI
# In local: load from env/.env if not already set
if [ -z "${TRADING_HOST:-}" ]; then
    if [ -f "$PROJECT_ROOT/env/.env" ]; then
        echo "Loading local environment variables..."
        set -a
        source "$PROJECT_ROOT/env/.env"
        set +a
    else
        echo "Error: Required environment variables not set and env/.env not found"
        echo "Either export variables or create env/.env from env/local.env.example"
        exit 1
    fi
fi

# Override PG_HOST for Docker network (pgloader runs inside container)
export PG_HOST=postgres

# Change to pgloader directory for processing .load files
cd "$SCRIPT_DIR"

# Function to substitute environment variables in .load files
substitute_vars() {
    local template=$1
    local output="${template%.load}.processed.load"

    envsubst < "$template" > "$output"
    echo "$output"
}

echo "Starting pgloader snapshot dumps..."
echo "=========================================="

# Load from MariaDB Instance 1
#echo ""
#echo "[$(date '+%Y-%m-%d %H:%M:%S')] Loading from MariaDB Instance 1..."
#LOAD_FILE=$(substitute_vars main_app.load)
#docker compose -f "$PROJECT_ROOT/docker-compose.yml" run --rm -T pgloader pgloader --verbose "$LOAD_FILE"
#EXIT_CODE=$?
#rm "$LOAD_FILE"
#if [ $EXIT_CODE -ne 0 ]; then
#    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Instance 1 failed with exit code $EXIT_CODE"
#    exit $EXIT_CODE
#fi
#echo "[$(date '+%Y-%m-%d %H:%M:%S')] Instance 1 completed successfully"
#
## Load from MariaDB Instance 2
#echo ""
#echo "[$(date '+%Y-%m-%d %H:%M:%S')] Loading from MariaDB Instance 2..."
#LOAD_FILE=$(substitute_vars trading.load)
#docker compose -f "$PROJECT_ROOT/docker-compose.yml" run --rm -T pgloader pgloader --verbose "$LOAD_FILE"
#EXIT_CODE=$?
#rm "$LOAD_FILE"
#if [ $EXIT_CODE -ne 0 ]; then
#    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Instance 2 failed with exit code $EXIT_CODE"
#    exit $EXIT_CODE
#fi
#echo "[$(date '+%Y-%m-%d %H:%M:%S')] Instance 2 completed successfully"

# Load from MariaDB Instance 3 (Finance)
echo ""
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Loading from MariaDB Instance 3 (Finance)..."
LOAD_FILE=$(substitute_vars finance.load)
docker compose -f "$PROJECT_ROOT/docker-compose.yml" run --rm -T pgloader pgloader --verbose "$LOAD_FILE"
EXIT_CODE=$?
rm "$LOAD_FILE"
if [ $EXIT_CODE -ne 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ERROR: Instance 3 failed with exit code $EXIT_CODE"
    exit $EXIT_CODE
fi
echo "[$(date '+%Y-%m-%d %H:%M:%S')] Instance 3 completed successfully"

echo ""
echo "=========================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S')] All snapshot dumps completed successfully!"
