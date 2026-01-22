# CDC Pipeline - Debezium Change Data Capture

## Overview

This project implements a CDC (Change Data Capture) pipeline using Debezium connectors to replicate data from MariaDB instances to PostgreSQL in real-time.

**Architecture:**
- **Debezium Connectors**: Hosted on Fly.io (https://cdc-connector.fly.dev)
- **Source**: MariaDB databases (8 separate instances)
- **Target**: PostgreSQL database (mp_cdc schema)
- **Message Broker**: Confluent Kafka
- **Initial Data Migration**: Python script (migrate_v3.py)

## Quick Start

### 1. Environment Setup

Update `.env` file with your database credentials:

```bash
# MariaDB Sources
TRADING_HOST=...
TRADING_USER=...
TRADING_PASS=...
# ... (repeat for finance, live, chat, performance, concontrol, claim, payment)

# PostgreSQL Target
SINK_DB_URL=jdbc:postgresql://...
SINK_DB_USER=...
SINK_DB_PASSWORD=...

# Kafka
KAFKA_BOOTSTRAP_SERVERS=...
CONFLUENT_API_KEY=...
CONFLUENT_API_SECRET=...

# Debezium
DEBEZIUM_URL=https://cdc-connector.fly.dev
```

### 2. Normal Workflow (New Database Setup)

**Step 1: Register Source Connector**
```bash
make register-<database>-source
```

**Step 2: Migrate Initial Data**
```bash
python3 migrate_v3.py --database <database> --tables all
```

**Step 3: Register Sink Connector**
```bash
make register-<database>-sink
```

This establishes the CDC pipeline. The sink reads from the earliest offset, ensuring no data is missed between migration and CDC activation.

**Available databases**: `trading`, `finance`, `live`, `chat`, `performance`, `concontrol`, `claim`, `payment`

### 3. Adding New Tables to Existing Database

When you add a table to the allowlist in `.env`:

**Step 1: Update Source Config**

Edit `connectors/sources/mariadb/<database>.json`:
```json
{
  "config": {
    "snapshot.mode": "recovery",
    ...
  }
}
```

**Step 2: Migrate the New Table**
```bash
python3 migrate_v3.py --database <database> --tables T_NEW_TABLE
```

**Step 3: Restart Source and Sink**
```bash
make restart-<database>-source
make restart-<database>-sink
```

The source will snapshot the new table while continuing to stream changes from existing tables.

**Step 4: Revert to schema_only**

After the new table is synced, revert:
```json
{
  "config": {
    "snapshot.mode": "schema_only",
    ...
  }
}
```

Then restart again:
```bash
make restart-<database>-source
```

### 4. Removing Tables from Allowlist

When removing tables from the allowlist:

**Step 1: Update Environment Variable**

Remove table from `.env`:
```bash
TRADING_TABLE_ALLOWLIST=xchange_trading.T_DEAL,xchange_trading.T_CARRIER
# Removed: T_OLD_TABLE
```

**Step 2: Update Source Connector**

Edit `connectors/sources/mariadb/<database>.json`:
```json
{
  "config": {
    "table.include.list": "${TRADING_TABLE_ALLOWLIST}",
    "schema.history.internal.kafka.topic": "xchange_trading-v2.schema-history"
  }
}
```

Change the schema history topic name (e.g., add `-v2`, `-v3`) to force Debezium to read from a new binlog offset.

**Step 3: Update Sink Connector**

Edit `connectors/sinks/postgres/<database>.json`:
```json
{
  "config": {
    "topics.regex": "xchange_trading_v2\\.xchange_trading\\..*"
  }
}
```

Update the topic regex to match the new topic prefix.

**Step 4: Update Source Topic Prefix**

Edit `connectors/sources/mariadb/<database>.json`:
```json
{
  "config": {
    "topic.prefix": "xchange_trading_v2"
  }
}
```

**Step 5: Restart Connectors**
```bash
make restart-<database>-source
make restart-<database>-sink
```

**Example**: See `connectors/sources/mariadb/concontrol.json` and `connectors/sinks/postgres/concontrol.json`

## Available Commands

### Registration
```bash
make register-<database>-source    # Register source connector
make register-<database>-sink      # Register sink connector
```

### Restart (Preserves Connection)
```bash
make restart-<database>-source     # Update source config without losing binlog position
make restart-<database>-sink       # Update sink config without losing offset
```

### Unregister
```bash
make unregister-<database>-source  # Delete source connector
make unregister-<database>-sink    # Delete sink connector
```

### Monitoring
```bash
make connectors                    # List all connectors and their status
make connector-status C=<name>     # Get detailed status and error messages
```

**Example:**
```bash
make connector-status C=mariadb-trading-connector
make connector-status C=postgres-sink-trading
```

## Migration Script (migrate_v3.py)

**Features:**
- Parallel table migration
- Automatic type conversions (bit(1) → boolean, etc.)
- Year-based chunking for large tables
- UNLOGGED tables during load, then converts to LOGGED
- Deferred index creation

**Usage:**

```bash
# Migrate all tables
python3 migrate_v3.py --database trading --tables all

# Migrate specific tables
python3 migrate_v3.py --database finance --tables T_INVOICE,T_ACCOUNT

# Migrate with custom settings
python3 migrate_v3.py --database trading \
  --tables T_DEAL \
  --max-workers 8 \
  --batch-size 50000
```

**Parameters:**
- `--database`: Database name (required)
- `--tables`: Comma-separated table list or "all" (required)
- `--max-workers`: Parallel workers (default: 2)
- `--chunk-workers`: Workers for large table chunks (default: 4)
- `--batch-size`: Rows per batch (default: 100000)
- `--threshold`: Large table threshold (default: 1000000)

## Connector Configuration

### Source Connector Settings

Key configurations in `connectors/sources/mariadb/<database>.json`:

```json
{
  "snapshot.mode": "schema_only",          // Normal mode
  "snapshot.mode": "recovery",             // Use when adding new tables
  "table.include.list": "${TABLE_ALLOWLIST}",
  "topic.prefix": "xchange_trading",
  "schema.history.internal.kafka.topic": "xchange_trading.schema-history"
}
```

### Sink Connector Settings

Key configurations in `connectors/sinks/postgres/<database>.json`:

```json
{
  "topics.regex": "xchange_trading\\.xchange_trading\\..*",
  "insert.mode": "upsert",
  "primary.key.mode": "record_key",
  "delete.enabled": "true",
  "schema.evolution": "basic"
}
```

## Troubleshooting

### Check Connector Health
```bash
make connectors
```

### View Error Messages
```bash
make connector-status C=mariadb-trading-connector
make connector-status C=postgres-sink-trading
```

### Common Issues

**1. Type Mismatch (bit(1) fields)**

Add Cast transform to sink connector:
```json
{
  "transforms": "route,castBits",
  "transforms.castBits.type": "org.apache.kafka.connect.transforms.Cast$Value",
  "transforms.castBits.spec": "active:boolean,deleted:boolean"
}
```

**2. Source Connector Not Capturing Changes**

- Verify `snapshot.mode: "schema_only"` after initial setup
- Check binlog position hasn't expired
- Ensure tables are in allowlist

**3. Sink Connector Failing**

- Check topic regex matches source topic prefix
- Verify primary keys exist in target tables
- Check schema compatibility

## Data Flow

```
MariaDB (Source)
    ↓
Debezium Source Connector (captures binlog changes)
    ↓
Kafka Topics (xchange_<db>.<db>.<table>)
    ↓
Debezium Sink Connector (consumes from earliest offset)
    ↓
PostgreSQL (Target: mp_cdc schema)
```

## Databases

| Database | MariaDB Schema | Table Count | Topic Prefix |
|----------|----------------|-------------|--------------|
| Trading | xchange_trading | 14 | xchange_trading |
| Finance | xchange_finance | 8 | xchange_finance |
| Live | xchangelive | 15 | xchangelive |
| Chat | xchange_chat | TBD | xchange_chat |
| Performance | xchange_performance | TBD | xchange_performance |
| Concontrol | xchange_concontrol | TBD | xchange_concontrol_v4 |
| Claim | xchange_claim | TBD | xchange_claim |
| Payment | xchange_payment | TBD | xchange_payment |

## Notes

- **Debezium hosted on Fly.io**: No local Debezium Connect required
- **Source connectors**: Use `snapshot.mode: "schema_only"` for normal operation
- **Sink connectors**: Read from `earliest` offset to prevent data loss
- **Schema history**: Changing the topic name forces new binlog offset
- **PUT endpoint**: `restart-*` commands preserve connector state/offsets
