#!/bin/bash
set -euo pipefail
set -e

echo "TRADING_USER=${TRADING_USER}"
echo "TRADING_HOST=${TRADING_HOST}"
echo "TRADING_DB=${TRADING_DB}"

# Required environment variables for each database
REQUIRED_VARS=(
    "TRADING_HOST" "TRADING_PORT" "TRADING_USER" "TRADING_PASS" "TRADING_DB"
    "FINANCE_HOST" "FINANCE_PORT" "FINANCE_USER" "FINANCE_PASS" "FINANCE_DB"
    "LIVE_HOST" "LIVE_PORT" "LIVE_USER" "LIVE_PASS" "LIVE_DB"
    "PG_HOST" "PG_PORT" "PG_USER" "PG_PASS" "PG_DB"
)

# Validate all required environment variables are set
MISSING_VARS=()
for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var:-}" ]; then
        MISSING_VARS+=("$var")
    fi
done

if [ ${#MISSING_VARS[@]} -gt 0 ]; then
    echo "❌ Error: Required environment variables are not set:" >&2
    printf '   - %s\n' "${MISSING_VARS[@]}" >&2
    echo "" >&2
    echo "For local development: Ensure .env file exists in project root" >&2
    echo "For production: Ensure secrets are configured in deployment platform" >&2
    exit 1
fi

# Detect if running in Docker container
if [ -f /.dockerenv ]; then
    cd /app
else
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    cd "$SCRIPT_DIR"
fi

# Enable SSL for PostgreSQL connections but don't verify certificates
# This is needed for AWS RDS/self-hosted Postgres with self-signed certs
export PGSSLMODE=prefer

echo "Starting pgloader snapshot dumps..."
echo "=========================================="

# Function to URL-encode passwords (handles special characters like =, !, @, etc.)
urlencode() {
    local string="${1}"
    local strlen=${#string}
    local encoded=""
    local pos c o

    for (( pos=0 ; pos<strlen ; pos++ )); do
        c=${string:$pos:1}
        case "$c" in
            [-_.~a-zA-Z0-9] ) o="${c}" ;;
            * ) printf -v o '%%%02x' "'$c"
        esac
        encoded+="${o}"
    done
    echo "${encoded}"
}

# Function to build pgloader connection string and run migration
run_migration() {
    local instance_name=$1
    local source_prefix=$2
    local load_template=$3

    echo ""
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Loading from $instance_name..."

    # Build connection strings
    local source_host="${source_prefix}_HOST"
    local source_port="${source_prefix}_PORT"
    local source_user="${source_prefix}_USER"
    local source_pass="${source_prefix}_PASS"
    local source_db="${source_prefix}_DB"

    # Don't URL-encode passwords - pgloader doesn't support it in connection URIs
    # Instead, use the raw passwords
    local encoded_source_pass="${!source_pass}"
    local encoded_pg_pass="${PG_PASS}"

    local SOURCE_URI="mysql://${!source_user}:${encoded_source_pass}@${!source_host}:${!source_port}/${!source_db}"
    local TARGET_URI="postgresql://${PG_USER}:${encoded_pg_pass}@${PG_HOST}:${PG_PORT}/${PG_DB}"

    # Create temporary processed file with substituted values
    local PROCESSED_FILE="${load_template%.load}.processed.load"

    # Use sed for safe, targeted substitution of known variables only
    # URL-encode passwords for use in connection strings
    sed -e "s|\${${source_prefix}_USER}|${!source_user}|g" \
        -e "s|\${${source_prefix}_PASS}|${encoded_source_pass}|g" \
        -e "s|\${${source_prefix}_HOST}|${!source_host}|g" \
        -e "s|\${${source_prefix}_PORT}|${!source_port}|g" \
        -e "s|\${${source_prefix}_DB}|${!source_db}|g" \
        -e "s|\${PG_USER}|${PG_USER}|g" \
        -e "s|\${PG_PASS}|${encoded_pg_pass}|g" \
        -e "s|\${PG_HOST}|${PG_HOST}|g" \
        -e "s|\${PG_PORT}|${PG_PORT}|g" \
        -e "s|\${PG_DB}|${PG_DB}|g" \
        "$load_template" > "$PROCESSED_FILE"

    echo "[DEBUG] Connection string check:"
    head -3 "$PROCESSED_FILE"

    # Run pgloader with the processed file
    # --no-ssl-cert-verification: Allow self-signed certs
    if pgloader --no-ssl-cert-verification --verbose "$PROCESSED_FILE"; then
        rm "$PROCESSED_FILE"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ $instance_name completed successfully"
        return 0
    else
        local exit_code=$?
        rm "$PROCESSED_FILE"
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ❌ ERROR: $instance_name failed with exit code $exit_code" >&2
        return $exit_code
    fi
}

# Run migrations for each database
run_migration "MariaDB Trading" "TRADING" "trading.load"
run_migration "MariaDB Finance" "FINANCE" "finance.load"
run_migration "MariaDB Live" "LIVE" "live.load"

echo ""
echo "=========================================="
echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ All snapshot dumps completed successfully!"
echo ""
echo "Note: Indexes were not created during load for performance."
echo "      Run 'make add-indexes' to add indexes and foreign keys."
