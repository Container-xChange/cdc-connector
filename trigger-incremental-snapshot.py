#!/usr/bin/env python3
"""
Trigger incremental snapshot via Kafka signaling
Uses confluent-kafka Python library for better reliability
"""
import os
import sys
import json
import time
from pathlib import Path
from confluent_kafka import Producer

def load_env():
    """Load environment variables from .env file"""
    env_path = '/Users/yenrenkhor/IdeaProjects/cdc-pipeline/cdc-pipeline/.env'

    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                value = value.strip('"').strip("'")
                os.environ[key] = value

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
        sys.exit(1)
    else:
        print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}]')

def trigger_snapshot(table, filter_condition=None):
    """Send incremental snapshot signal to Kafka"""

    load_env()

    # Get environment variables
    bootstrap_servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    security_protocol = os.getenv('KAFKA_SECURITY_PROTOCOL', 'SASL_SSL')
    sasl_mechanism = os.getenv('KAFKA_SASL_MECHANISM', 'SCRAM-SHA-512')
    sasl_username = os.getenv('CONFLUENT_API_KEY')
    sasl_password = os.getenv('CONFLUENT_API_SECRET')

    if not all([bootstrap_servers, sasl_username, sasl_password]):
        print("❌ Error: Required environment variables not set")
        print("   KAFKA_BOOTSTRAP_SERVERS, CONFLUENT_API_KEY, CONFLUENT_API_SECRET")
        sys.exit(1)

    signal_topic = os.getenv('SIGNAL_TOPIC', 'debezium-signals-li')
    snapshot_id = 'xchange_finance_li'

    print("=" * 60)
    print("Triggering Incremental Snapshot")
    print("=" * 60)
    print(f"Signal Topic: {signal_topic}")
    print(f"Table: {table}")
    print(f"Snapshot ID: {snapshot_id}")
    if filter_condition:
        print(f"Filter: {filter_condition}")
    else:
        print("Filter: None (full table)")
    print("=" * 60)
    print()

    # Build signal payload
    signal_data = {
        "id": snapshot_id,
        "type": "execute-snapshot",
        "data": {
            "data-collections": [table]
        }
    }

    if filter_condition:
        signal_data["data"]["additional-conditions"] = [{
            "data-collection": table,
            "filter": filter_condition
        }]

    # Configure Kafka producer with SASL
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': security_protocol,
        'sasl.mechanism': sasl_mechanism,
        'sasl.username': sasl_username,
        'sasl.password': sasl_password,
    }

    producer = Producer(conf)

    # Send signal message (key:value format)
    message_key = snapshot_id
    message_value = json.dumps(signal_data)

    print("Sending signal to Kafka...")
    producer.produce(
        signal_topic,
        key=message_key.encode('utf-8'),
        value=message_value.encode('utf-8'),
        callback=delivery_report
    )

    # Wait for delivery
    producer.flush()

    print()
    print("=" * 60)
    print("Monitor progress with:")
    print("  make connector-status C=mariadb-finance-line-item-connector")
    print()
    print("Check Debezium logs:")
    print("  make logs-debezium")
    print("=" * 60)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Trigger incremental snapshot via Kafka signaling',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full table snapshot
  %(prog)s

  # Snapshot with date filter
  %(prog)s --filter "approved_date >= '2024-01-01'"

  # Snapshot recent records only
  %(prog)s --filter "approved_date >= '2025-01-01'"

  # Custom table
  %(prog)s --table xchange_finance.T_INVOICE
        """
    )

    parser.add_argument(
        '--table',
        default='xchange_finance.T_LINE_ITEM',
        help='Table to snapshot (default: xchange_finance.T_LINE_ITEM)'
    )

    parser.add_argument(
        '--filter',
        help='SQL filter condition (e.g., "approved_date >= \'2024-01-01\'")'
    )

    args = parser.parse_args()

    trigger_snapshot(args.table, args.filter)