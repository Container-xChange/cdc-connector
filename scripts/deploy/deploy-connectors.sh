#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONNECTORS_DIR="$PROJECT_ROOT/connectors"
DEBEZIUM_URL="${DEBEZIUM_URL:-http://localhost:8083}"

echo "🔌 Deploying Debezium Connectors..."
echo "======================================"

# Load environment variables
if [ -f "$PROJECT_ROOT/env/.env" ]; then
    set -a
    source "$PROJECT_ROOT/env/.env"
    set +a
fi

# Function to wait for Debezium Connect to be ready
wait_for_debezium() {
    echo "⏳ Waiting for Debezium Connect to be ready..."
    for i in {1..60}; do
        if curl -sf "$DEBEZIUM_URL" > /dev/null 2>&1; then
            echo "✅ Debezium Connect is ready"
            return 0
        fi
        echo -n "."
        sleep 2
    done
    echo "❌ Debezium Connect did not become ready in time"
    exit 1
}

# Function to substitute environment variables in connector JSON
substitute_vars() {
    local template=$1
    envsubst < "$template"
}

# Function to deploy a connector
deploy_connector() {
    local connector_file=$1
    local connector_name=$(basename "$connector_file" .json)

    echo ""
    echo "📤 Deploying connector: $connector_name"

    # Check if connector already exists
    if curl -sf "$DEBEZIUM_URL/connectors/$connector_name" > /dev/null 2>&1; then
        echo "   ⚠️  Connector $connector_name already exists, deleting..."
        curl -X DELETE "$DEBEZIUM_URL/connectors/$connector_name" 2>/dev/null || true
        sleep 2
    fi

    # Substitute environment variables and deploy
    local connector_config=$(substitute_vars "$connector_file")

    response=$(echo "$connector_config" | curl -X POST "$DEBEZIUM_URL/connectors" \
        -H "Content-Type: application/json" \
        -d @- 2>/dev/null)

    if echo "$response" | jq -e '.name' > /dev/null 2>&1; then
        echo "   ✅ Successfully deployed $connector_name"
    else
        echo "   ❌ Failed to deploy $connector_name"
        echo "$response" | jq '.' || echo "$response"
        return 1
    fi
}

# Wait for Debezium to be ready
wait_for_debezium

# Deploy source connectors (MariaDB)
echo ""
echo "📥 Deploying Source Connectors (MariaDB -> Kafka)..."
for connector in "$CONNECTORS_DIR"/sources/mariadb/*.json; do
    if [ -f "$connector" ]; then
        deploy_connector "$connector"
    fi
done

# Wait a bit for source connectors to initialize
sleep 5

# Deploy sink connectors (Kafka -> Postgres)
echo ""
echo "📤 Deploying Sink Connectors (Kafka -> Postgres)..."
for connector in "$CONNECTORS_DIR"/sinks/postgres/*.json; do
    if [ -f "$connector" ]; then
        deploy_connector "$connector"
    fi
done

# Show connector status
echo ""
echo "======================================"
echo "📊 Connector Status:"
echo "======================================"
sleep 3

for connector in $(curl -s "$DEBEZIUM_URL/connectors" | jq -r '.[]'); do
    status=$(curl -s "$DEBEZIUM_URL/connectors/$connector/status" | jq -r '.connector.state')
    task_states=$(curl -s "$DEBEZIUM_URL/connectors/$connector/status" | jq -r '.tasks[].state' | sort | uniq -c)
    echo "📌 $connector: $status"
    if [ -n "$task_states" ]; then
        echo "$task_states" | sed 's/^/   /'
    fi
done

echo ""
echo "✅ All connectors deployed!"
