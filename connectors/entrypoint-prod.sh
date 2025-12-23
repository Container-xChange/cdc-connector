#!/bin/bash
set -e

echo "🔐 Starting OpenVPN connection..."
openvpn --config /kafka/profile-286.ovpn --auth-user-pass /pass-prod.txt --daemon

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

echo "🔍 Testing connectivity to MariaDB hosts..."
nc -zv xc-trading.covl02ovmomq.eu-central-1.rds.amazonaws.com 3306 2>&1 | head -1 || echo "⚠️  Trading DB unreachable"
nc -zv xc-finance.covl02ovmomq.eu-central-1.rds.amazonaws.com 3306 2>&1 | head -1 || echo "⚠️  Finance DB unreachable"
nc -zv 172.31.23.19 3306 2>&1 | head -1 || echo "⚠️  Live DB unreachable"

echo "🚀 Starting Debezium Kafka Connect..."

# Start connector registration in background (run after Debezium is up)
(
    echo "⏳ Waiting for Debezium Connect to be ready before deploying connectors..."
    for i in {1..120}; do
        if curl -sf http://localhost:8083/ > /dev/null 2>&1; then
            echo "✅ Debezium Connect is ready"
            cd /kafka
            if [ -f "scripts/deploy/deploy-connectors.sh" ]; then
                echo "🔌 Deploying connectors..."
                bash scripts/deploy/deploy-connectors.sh
                echo "✅ Connectors deployed successfully"
            fi
            break
        fi
        sleep 2
    done
) &

# Run Debezium Connect directly (this blocks and keeps container alive)
exec /docker-entrypoint.sh start
