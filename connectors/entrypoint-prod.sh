#!/bin/bash
set -e

echo "🔐 Starting OpenVPN connection..."
# Start OpenVPN in the background with daemon mode
openvpn --config /kafka/profile-286.ovpn --auth-user-pass /pass-prod.txt --daemon

# Wait for VPN connection to establish
echo "⏳ Waiting for VPN connection..."
for i in {1..30}; do
    if ip addr show tun0 &>/dev/null; then
        echo "✅ VPN connected successfully"
        ip addr show tun0 | grep inet
        break
    fi
    if [ $i -eq 30 ]; then
        echo "❌ VPN connection timeout"
        exit 1
    fi
    sleep 1
done

# Verify connectivity to MariaDB hosts
echo "🔍 Testing connectivity to MariaDB hosts..."
nc -zv xc-trading.covl02ovmomq.eu-central-1.rds.amazonaws.com 3306 2>&1 | head -1 || echo "⚠️  Trading DB unreachable"
nc -zv xc-finance.covl02ovmomq.eu-central-1.rds.amazonaws.com 3306 2>&1 | head -1 || echo "⚠️  Finance DB unreachable"
nc -zv 172.31.23.19 3306 2>&1 | head -1 || echo "⚠️  Live DB unreachable"

echo "🚀 Starting Debezium Kafka Connect..."

# Configure JVM options
export JDK_JAVA_OPTIONS="-XshowSettings:vm -XX:MaxRAMPercentage=75.0"

# Ensure REST API binds to 0.0.0.0 (critical for Fly.io)
export CONNECT_REST_HOST_NAME=0.0.0.0
export CONNECT_REST_PORT=8083
export CONNECT_REST_ADVERTISED_HOST_NAME=${CONNECT_REST_ADVERTISED_HOST_NAME:-cdc-connector.fly.dev}
export CONNECT_REST_ADVERTISED_PORT=8083

echo "📡 REST API will listen on: ${CONNECT_REST_HOST_NAME}:${CONNECT_REST_PORT}"
echo "📡 Advertised as: ${CONNECT_REST_ADVERTISED_HOST_NAME}:${CONNECT_REST_ADVERTISED_PORT}"

# Start Debezium Connect (original entrypoint)
exec /docker-entrypoint.sh start

