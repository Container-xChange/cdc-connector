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

Create `.env` file based on `.env.template` with your database credentials:

```bash
# MariaDB Sources
TRADING_HOST=...
TRADING_USER=...
TRADING_PASS=...
TRADING_TABLE_ALLOWLIST=xchange_trading.T_TABLE1,xchange_trading.T_TABLE2
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

See `.env.template` for all required environment variables.

### 2. Normal Workflow (New Database Setup)

**Manual Workflow (without CI/CD):**
#### Adding Tables
When adding a new table manually, a requirement checklist must be followed:
- Create new connector JSON files
- Update migrate_v3.py database config if needed new database migration
- Update `.env` with database connection secrets and table allowlist
- Create required make commands in Makefile (register, restart, unregister source and sink)

```bash

make register-<database>-source
python3 migrate_v3.py --database <database> --tables all
make register-<database>-sink
```
all is simple, as the script will skip tables that are already migrated. But you can also specify a table (more below)
**Available databases**: `trading`, `finance`, `live`, `chat`, `performance`, `concontrol`, `claim`, `payment`, add more if needed

### 3. Updating Existing Database (Adding/Removing Tables)

#### Adding New Tables

When you add a table to the allowlist:

**Step 1: Update Table Include List**

Edit `connectors/sources/mariadb/<database>.json`:
```json
{
  "config": {
    "table.include.list": "schema.T_OLD_TABLE,schema.T_NEW_TABLE",
    "snapshot.mode": "recovery"
  }
}
```

Also update `.env` for migration script:
```bash
<DATABASE>_TABLE_ALLOWLIST=schema.T_OLD_TABLE,schema.T_NEW_TABLE
```

**Step 2: Restart Connector (To be automated via github action)**
```bash

make restart-<database>-source
```

**Step 3: Revert snapshot.mode (Manual)**

After Debezium completes snapshot (check connector status), update source config:
```json
{
  "config": {
    "snapshot.mode": "schema_only"
  }
}
```

Commit, push, and GitHub Actions will restart the connector.

**Same should apply to removing tables** 

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
  "snapshot.mode": "recovery",             // Use when adding/removing new tables
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

## Creating New Connectors

When adding a new database to the CDC pipeline, you need to create several files and configurations:

### 1. Environment Variables (`.env`)

Add database configuration for the new database:

```bash
# MariaDB Instance - <DATABASE_NAME>
<DATABASE>_HOST=...
<DATABASE>_PORT=3306
<DATABASE>_DB=...
<DATABASE>_USER=...
<DATABASE>_PASS=...
<DATABASE>_SERVER_ID=...
<DATABASE>_TABLE_ALLOWLIST=<schema>.<table1>,<schema>.<table2>
```

**Note**: Use uppercase for the database prefix (e.g., `TRADING_`, `FINANCE_`)

### 2. Source Connector (`connectors/sources/mariadb/<database>.json`)

Create source connector configuration:

```json
{
  "name": "mariadb-<database>-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "${<DATABASE>_HOST}",
    "database.port": "${<DATABASE>_PORT}",
    "database.user": "${<DATABASE>_USER}",
    "database.password": "${<DATABASE>_PASS}",
    "database.server.id": "${<DATABASE>_SERVER_ID}",
    "topic.prefix": "xchange_<database>",
    "schema.history.internal.kafka.topic": "xchange_<database>.schema-history",
    "schema.history.internal.kafka.bootstrap.servers": "${KAFKA_BOOTSTRAP_SERVERS}",
    "table.include.list": "<schema>.<table1>,<schema>.<table2>",
    "snapshot.mode": "schema_only",
    "snapshot.locking.mode": "none",
    "include.schema.changes": "false",
    "converters": "boolean",
    "boolean.type": "io.debezium.connector.mysql.converters.TinyIntOneToBooleanConverter"
  }
}
```

**Important**:
- Use **lowercase** database name in `name`, `topic.prefix`, and file name
- `table.include.list` should be **hardcoded** in the JSON (not `${...}` variable reference)
- Keep a copy of the table list in `.env` as `<DATABASE>_TABLE_ALLOWLIST` for Python migration script
- Set `snapshot.mode: "schema_only"` for initial setup

### 3. Sink Connector (`connectors/sinks/postgres/<database>.json`)

Create sink connector configuration:

```json
{
  "name": "postgres-sink-<database>",
  "config": {
    "connector.class": "io.debezium.connector.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "connection.url": "${SINK_DB_URL}&currentSchema=mp_cdc",
    "connection.username": "${SINK_DB_USER}",
    "connection.password": "${SINK_DB_PASSWORD}",
    "topics.regex": "xchange_<database>\\.<schema>\\..*",
    "insert.mode": "upsert",
    "primary.key.mode": "record_key",
    "schema.evolution": "basic",
    "delete.enabled": "true",
    "consumer.override.auto.offset.reset": "earliest",
    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "xchange_<database>\\.<schema>\\.(.*)",
    "transforms.route.replacement": "<schema>_$1"
  }
}
```

**Important**:
- `topics.regex` must match source connector's `topic.prefix` format
- Add `castBits` transform if you have `bit(1)` fields (see "Type Mismatch" in Troubleshooting)

### 4. Migration Script Configuration (if needed)

If you need to add the new database to the migration script, update the `DB_CONFIGS` dictionary in `migrate_v3.py`:

```python
DB_CONFIGS = {
    # ... existing configs ...
    'NEW_TABLE': {
        'mysql': {
            'host': 'NEW_TABLE_HOST',
            'port': 'NEW_TABLE_PORT',
            'user': 'NEW_TABLE_USER',
            'password': 'NEW_TABLE_PASS',
            'database': 'NEW_TABLE_DB'
        },
        'schema': 'mp_cdc',  # target schema in PostgreSQL
        'prefix': 'trading_' # custom prefix if needed
    },
}
```

## Notes

- **Debezium hosted on Fly.io**: No local Debezium Connect required
- **Source connectors**: Use `snapshot.mode: "schema_only"` for normal operation
- **Sink connectors**: Read from `earliest` offset to prevent data loss
- **Schema history**: Changing the topic name forces new binlog offset
- **PUT endpoint**: `restart-*` commands preserve connector state/offsets
- **Table include list**: Hardcoded in source connector JSON, duplicated in `.env` for migration script
