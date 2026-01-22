#!/bin/bash
set -e

echo "= Starting OpenVPN connection..."
openvpn --config /vpn/profile-286.ovpn --auth-user-pass /vpn/pass-prod.txt --daemon

echo "� Waiting for VPN connection..."
for i in {1..30}; do
    if ip addr show tun0 &>/dev/null; then
        echo " VPN connected successfully"
        ip addr show tun0 | grep inet
        break
    fi
    if [ $i -eq 30 ]; then
        echo "L VPN connection timeout"
        exit 1
    fi
    sleep 1
done

echo "⏰ Setting up cron job for CDC validation..."

# Dump environment variables for cron to use
env > /app/.env_for_cron

# Install crontab from file
crontab /app/tests/crontab

# Start cron in foreground
echo "🚀 Starting cron daemon..."
cron -f
