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

**Step 2: Create PR and Trigger GitHub Actions**

When you commit and push:
- GitHub Actions will detect `snapshot.mode: "recovery"`
- Connector will be restarted via `make restart-<database>-source`
- Migration script will NOT run (only for new databases)
- Debezium will snapshot the new table while streaming existing tables

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

**Manual Workflow (without CI/CD):**
```bash
# 1. Update configs (.env and source JSON)
# 2. Migrate new table
python3 migrate_v3.py --database <database> --tables T_NEW_TABLE

# 3. Restart connectors
make restart-<database>-source
make restart-<database>-sink

# 4. After snapshot completes, revert snapshot.mode and restart
make restart-<database>-source
```

#### Removing Tables

When you remove a table from the allowlist:

**Note**: Removing tables does NOT require changing snapshot mode, but you must version the topic prefix to force a new binlog offset.

**Step 1: Update Source Connector**

Edit `connectors/sources/mariadb/<database>.json`:
```json
{
  "config": {
    "table.include.list": "schema.T_KEEP_THIS",
    "topic.prefix": "xchange_<database>_v2",
    "schema.history.internal.kafka.topic": "xchange_<database>-v2.schema-history"
  }
}
```

**Step 2: Update Sink Connector**

Edit `connectors/sinks/postgres/<database>.json`:
```json
{
  "config": {
    "topics.regex": "xchange_<database>_v2\\.<schema>\\..*"
  }
}
```

**Step 3: Update `.env`**
```bash
<DATABASE>_TABLE_ALLOWLIST=schema.T_KEEP_THIS
```

**Step 4: Create PR and Trigger GitHub Actions**

GitHub Actions will:
- Detect config changes
- Restart both source and sink connectors via `make restart-*`
- Verify connectors are running

This creates new Kafka topics with the new prefix. Old topics are not automatically deleted.

**Manual Workflow:**
```bash
make restart-<database>-source
make restart-<database>-sink
```

**Example**: See `connectors/sources/mariadb/concontrol.json` (uses `xchange_concontrol_v4`)

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

### 4. Makefile Commands

Add these targets to `Makefile`:

```makefile
register-<database>-source:
	@echo "Registering <database> source connector..."
	@$(MAKE) -s load-env
	@envsubst < connectors/sources/mariadb/<database>.json | curl -X POST \
		-H "Content-Type: application/json" \
		-d @- \
		${DEBEZIUM_URL}/connectors
	@echo "\n✓ <Database> source connector registered"

register-<database>-sink:
	@echo "Registering <database> sink connector..."
	@$(MAKE) -s load-env
	@envsubst < connectors/sinks/postgres/<database>.json | curl -X POST \
		-H "Content-Type: application/json" \
		-d @- \
		${DEBEZIUM_URL}/connectors
	@echo "\n✓ <Database> sink connector registered"

restart-<database>-source:
	@echo "Restarting <database> source connector..."
	@$(MAKE) -s load-env
	@envsubst < connectors/sources/mariadb/<database>.json | curl -X PUT \
		-H "Content-Type: application/json" \
		-d @- \
		${DEBEZIUM_URL}/connectors/mariadb-<database>-connector/config
	@echo "\n✓ <Database> source connector restarted"

restart-<database>-sink:
	@echo "Restarting <database> sink connector..."
	@$(MAKE) -s load-env
	@envsubst < connectors/sinks/postgres/<database>.json | curl -X PUT \
		-H "Content-Type: application/json" \
		-d @- \
		${DEBEZIUM_URL}/connectors/postgres-sink-<database>/config
	@echo "\n✓ <Database> sink connector restarted"

unregister-<database>-source:
	@echo "Unregistering <database> source connector..."
	@$(MAKE) -s load-env
	@curl -X DELETE ${DEBEZIUM_URL}/connectors/mariadb-<database>-connector
	@echo "\n✓ <Database> source connector unregistered"

unregister-<database>-sink:
	@echo "Unregistering <database> sink connector..."
	@$(MAKE) -s load-env
	@curl -X DELETE ${DEBEZIUM_URL}/connectors/postgres-sink-<database>
	@echo "\n✓ <Database> sink connector unregistered"
```

**Important**: Replace `<database>` with lowercase name, `<Database>` with capitalized, `<DATABASE>` with uppercase

### 5. Migration Script Configuration (if needed)

If the database uses a non-standard schema name or requires special type mappings, update `migrate_v3.py`:

```python
DB_CONFIGS = {
    # ... existing configs ...
    '<database>': {
        'schema': 'custom_schema_name',  # If different from database name
        'type_overrides': {
            'T_SPECIAL_TABLE': {
                'special_column': 'TEXT'
            }
        }
    }
}
```

### 6. Initial Setup Workflow

After creating all files:

```bash
# 1. Register source connector
make register-<database>-source

# 2. Migrate initial data
python3 migrate_v3.py --database <database> --tables all

# 3. Register sink connector
make register-<database>-sink

# 4. Verify connectors
make connector-status C=mariadb-<database>-connector
make connector-status C=postgres-sink-<database>
```

### 7. GitHub Actions Workflows

#### Manual Workflow 1: Setup New Database

To set up CDC for a new database using GitHub Actions:

1. **Navigate to Actions tab** in GitHub
2. **Select**: "Setup New Database" workflow
3. **Click**: "Run workflow"
4. **Select**:
   - Database: Choose from dropdown (trading, finance, live, etc.)
   - Dry run: Enable to validate without executing

**What it does:**
```
✓ Validates connector JSON files exist
✓ Validates JSON syntax and required fields
✓ Registers source connector (make register-<database>-source)
✓ Runs migration script (python3 migrate_v3.py --database <database> --tables all)
✓ Registers sink connector (make register-<database>-sink)
✓ Verifies all connectors are RUNNING
✓ Fails if any step fails
```

**Prerequisites:**
- Source connector file: `connectors/sources/mariadb/<database>.json`
- Sink connector file: `connectors/sinks/postgres/<database>.json`
- Environment secrets configured in GitHub (see Required GitHub Secrets below)
- `snapshot.mode: "schema_only"` in source connector

**Example output:**
```
✅ Source connector file: connectors/sources/mariadb/trading.json
✅ Sink connector file: connectors/sinks/postgres/trading.json
✅ JSON syntax valid
✅ Required fields present
📡 Registering source connector: mariadb-trading-connector
✅ Source connector registered successfully
⏳ Waiting for source connector to be ready...
✅ Source connector is RUNNING
🔄 Starting migration for database: trading
✅ Migration completed successfully
📡 Registering sink connector: postgres-sink-trading
✅ Sink connector registered successfully
⏳ Waiting for sink connector to be ready...
✅ Sink connector is RUNNING
🏥 Running final health check...
✅ All connectors are healthy
```

#### Manual Workflow 2: Update Database Configuration

To update an existing database configuration (add/remove tables or config changes):

1. **Navigate to Actions tab** in GitHub
2. **Select**: "Update Database Configuration" workflow
3. **Click**: "Run workflow"
4. **Select**:
   - Database: Choose from dropdown
   - Update Type:
     - `add_tables` - Adding new tables to allowlist (requires `snapshot.mode: "recovery"`)
     - `remove_tables` - Removing tables (requires versioned topic prefix)
     - `config_change` - General configuration updates
   - Dry run: Enable to validate without executing

**What it does:**
```
✓ Validates connector JSON files and syntax
✓ Extracts and validates snapshot.mode and topic.prefix
✓ Warns if configuration doesn't match update type
✓ Restarts source connector (make restart-<database>-source)
✓ Waits for source connector to stabilize
✓ Restarts sink connector (make restart-<database>-sink)
✓ Waits for sink connector to stabilize
✓ Verifies all connectors (make connectors)
✓ Fails if any connector is FAILED
✓ Displays post-update instructions
```

**Update Type: add_tables**

When adding new tables to the allowlist:

**Prerequisites:**
- Update `table.include.list` in `connectors/sources/mariadb/<database>.json`
- Set `snapshot.mode: "recovery"` in source connector
- Update `<DATABASE>_TABLE_ALLOWLIST` in `.env` (for future migrations)
- Commit and push changes

**Workflow:**
1. Run workflow with `update_type: add_tables`
2. Workflow restarts connectors
3. Debezium snapshots new tables while streaming existing ones
4. Monitor snapshot progress: `make connector-status C=mariadb-<database>-connector`
5. After snapshot completes:
   - Change `snapshot.mode` back to `"schema_only"`
   - Run workflow again with `update_type: config_change`

**Example output:**
```
📋 Update Details
================
Database: trading
Update Type: add_tables
Snapshot Mode: recovery
- ✅ Correct snapshot mode for adding tables

🔄 Restarting source connector: mariadb-trading-connector
✅ Source connector restarted successfully
⏳ Waiting for source connector to stabilize...
Note: Adding tables may take longer due to snapshotting
🔄 Restarting sink connector: postgres-sink-trading
✅ Sink connector restarted successfully
🏥 Checking all connectors health...
✅ All connectors are healthy

⚠️  IMPORTANT: Snapshot mode is currently set to 'recovery'

Next steps:
1. Monitor snapshot progress:
   make connector-status C=mariadb-trading-connector

2. After snapshot completes:
   a. Edit connectors/sources/mariadb/trading.json
   b. Change snapshot.mode back to 'schema_only'
   c. Commit and push changes
   d. Run this workflow again with update_type: config_change
```

**Update Type: remove_tables**

When removing tables from the allowlist:

**Prerequisites:**
- Update `table.include.list` in `connectors/sources/mariadb/<database>.json`
- Version the `topic.prefix` (e.g., `xchange_trading_v2`)
- Version the `schema.history.internal.kafka.topic` (e.g., `xchange_trading-v2.schema-history`)
- Update `topics.regex` in `connectors/sinks/postgres/<database>.json` to match new prefix
- Update `<DATABASE>_TABLE_ALLOWLIST` in `.env`
- Commit and push changes

**Workflow:**
1. Run workflow with `update_type: remove_tables`
2. Workflow validates topic prefix is versioned
3. Workflow restarts both connectors
4. New Kafka topics created with new prefix
5. Old topics remain but are no longer consumed

**Example output:**
```
📋 Update Details
================
Database: concontrol
Update Type: remove_tables
Topic prefix: xchange_concontrol_v4
- ✅ Topic prefix is versioned (correct for removing tables)

🔄 Restarting source connector: mariadb-concontrol-connector
✅ Source connector restarted successfully
🔄 Restarting sink connector: postgres-sink-concontrol
✅ Sink connector restarted successfully
✅ All connectors are healthy

✅ Tables removed and connectors updated
Note: Old Kafka topics still exist but are no longer consumed
```

**Update Type: config_change**

For any other configuration changes:

**Prerequisites:**
- Update connector JSON files as needed
- Commit and push changes

**Workflow:**
1. Run workflow with `update_type: config_change`
2. Workflow restarts connectors
3. Configuration changes applied

#### Automatic: CI/CD Integration (Coming Soon)

#### Workflow Triggers

**On Pull Request:**
- Validates connector JSON files
- Checks for required environment variables in secrets

**On Push to Main (after PR merge):**
- Detects changed connector files
- Executes appropriate workflow based on changes

#### Workflow Scenarios

**Scenario 1: New Database (New Connector Files Created)**

When both source and sink connector files are created:

```yaml
jobs:
  setup-new-database:
    steps:
      - Detect new connector files
      - Register source connector (make register-<database>-source)
      - Run migration (python3 migrate_v3.py --database <database> --tables all)
      - Register sink connector (make register-<database>-sink)
      - Verify connectors (make connector-status)
      - Fail job if any connector is not RUNNING
```

**Scenario 2: Adding Tables (snapshot.mode = "recovery")**

When source connector is modified and `snapshot.mode: "recovery"` is detected:

```yaml
jobs:
  add-tables:
    steps:
      - Detect snapshot.mode: "recovery" in source config
      - Restart source connector (make restart-<database>-source)
      - Wait for Debezium to complete snapshot
      - Restart sink connector (make restart-<database>-sink)
      - Verify connectors (make connector-status)
      - Post comment on PR: "Please revert snapshot.mode to 'schema_only' after snapshot completes"
```

**Scenario 3: Removing Tables (Topic Prefix Changed)**

When source connector `topic.prefix` version is incremented:

```yaml
jobs:
  remove-tables:
    steps:
      - Detect topic.prefix version change
      - Verify matching sink topics.regex update
      - Restart source connector (make restart-<database>-source)
      - Restart sink connector (make restart-<database>-sink)
      - Verify connectors (make connector-status)
```

**Scenario 4: Configuration Updates (Other Changes)**

For any other connector config changes:

```yaml
jobs:
  update-config:
    steps:
      - Detect modified connector files
      - Restart affected connectors (make restart-*)
      - Verify connectors (make connector-status)
```

#### Final Health Check

All workflows end with:

```yaml
jobs:
  health-check:
    steps:
      - Run: make connectors
      - Parse output for FAILED state
      - Fail job if any connector is FAILED
      - Post summary to PR/commit
```

#### Required GitHub Secrets

Store these as repository secrets (Settings → Secrets and variables → Actions → New repository secret):

**MariaDB Databases:**
```
TRADING_HOST
TRADING_USER
TRADING_PASS
TRADING_SERVER_ID
TRADING_TABLE_ALLOWLIST

FINANCE_HOST
FINANCE_USER
FINANCE_PASS
FINANCE_SERVER_ID
FINANCE_TABLE_ALLOWLIST

LIVE_HOST
LIVE_USER
LIVE_PASS
LIVE_SERVER_ID
LIVE_TABLE_ALLOWLIST

CHAT_HOST
CHAT_USER
CHAT_PASS
CHAT_SERVER_ID
CHAT_TABLE_ALLOWLIST

PERFORMANCE_HOST
PERFORMANCE_USER
PERFORMANCE_PASS
PERFORMANCE_SERVER_ID
PERFORMANCE_TABLE_ALLOWLIST

CONCONTROL_HOST
CONCONTROL_USER
CONCONTROL_PASS
CONCONTROL_SERVER_ID
CONCONTROL_TABLE_ALLOWLIST

CLAIM_HOST
CLAIM_USER
CLAIM_PASS
CLAIM_SERVER_ID
CLAIM_TABLE_ALLOWLIST

PAYMENT_HOST
PAYMENT_USER
PAYMENT_PASS
PAYMENT_SERVER_ID
PAYMENT_TABLE_ALLOWLIST
```

**PostgreSQL Target:**
```
PG_HOST
PG_USER
PG_PASS

SINK_DB_HOST
SINK_DB_USER
SINK_DB_PASSWORD
SINK_DB_URL
```

**Kafka:**
```
KAFKA_BOOTSTRAP_SERVERS
CONFLUENT_API_KEY
CONFLUENT_API_SECRET
```

**Debezium:**
```
DEBEZIUM_URL
```

**Total:** 42 secrets required

#### Example GitHub Actions Output

```
✓ Detected new connector files: trading.json (source), trading.json (sink)
✓ Registered source connector: mariadb-trading-connector
✓ Running migration: trading (14 tables)
  ├─ T_DEAL: 1.2M rows migrated
  ├─ T_DEAL_HISTORY: 800K rows migrated
  └─ ...
✓ Registered sink connector: postgres-sink-trading
✓ Health check passed: All connectors RUNNING

Connector Status:
- mariadb-trading-connector: RUNNING
- postgres-sink-trading: RUNNING
```

## Notes

- **Debezium hosted on Fly.io**: No local Debezium Connect required
- **Source connectors**: Use `snapshot.mode: "schema_only"` for normal operation
- **Sink connectors**: Read from `earliest` offset to prevent data loss
- **Schema history**: Changing the topic name forces new binlog offset
- **PUT endpoint**: `restart-*` commands preserve connector state/offsets
- **Table include list**: Hardcoded in source connector JSON, duplicated in `.env` for migration script
