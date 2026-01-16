# CDC Pipeline - Database Migration

## Overview

This project provides two methods for migrating data from MariaDB instances to PostgreSQL:

1. **pgloader** (legacy) - Bulk migration using pgloader configuration files
2. **migrate.py** (recommended) - Python-based migration with parallel processing and incremental support

## Migration Tools

### 1. Python Migration Script (Recommended)

`migrate.py` is a custom Python tool with advanced features:

**Features:**
- ✅ Parallel table migration with configurable workers
- ✅ Automatic year-based chunking for large tables (>1M rows)
- ✅ Non-destructive mode (preserves existing schemas/tables)
- ✅ Handles all type conversions (bit(1) → boolean, etc.)
- ✅ Selective table migration
- ✅ UNLOGGED tables during load, then converts to LOGGED
- ✅ Deferred index creation for optimal performance

**Usage:**

```bash
# Migrate specific tables from trading database with 5 parallel workers
python3 migrate.py --database trading \
  --tables T_ABSTRACT_OFFER,T_DEAL,T_CARRIER \
  --max-workers 5

# Migrate single table from finance database (default 2 workers)
python3 migrate.py --database finance \
  --tables T_INVOICE

# Migrate with custom batch size and large table threshold
python3 migrate.py --database trading \
  --tables T_DEAL \
  --batch-size 50000 \
  --threshold 500000 \
  --max-workers 4 \
  --chunk-workers 6

# Migrate multiple tables from live database
python3 migrate.py --database live \
  --tables T_CARRIER,T_LOCATION
```

**Command-Line Parameters:**

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `--database` | Yes | - | Database to migrate: `trading`, `finance`, or `live` |
| `--tables` | Yes | - | Comma-separated list of tables to migrate |
| `--max-workers` | No | 2 | Number of parallel workers for table migration |
| `--chunk-workers` | No | 4 | Number of parallel workers for year-chunk processing in large tables |
| `--batch-size` | No | 100000 | Rows per batch |
| `--threshold` | No | 1000000 | Row threshold for triggering large table chunking |

**Environment Variables (Alternative Configuration):**

```bash
export MIGRATION_BATCH_SIZE=20000
export MIGRATION_WORKERS=8
export CHUNK_WORKERS=6
export LARGE_TABLE_THRESHOLD=500000
```

**How It Works:**

1. **Schema Creation** - Creates schema if not exists (non-destructive)
2. **Table Creation** - Creates UNLOGGED tables without indexes for fast loading
3. **Data Migration** - Uses PostgreSQL COPY protocol for bulk inserts
   - Small tables: Sequential migration
   - Large tables (>threshold): Parallel year-based chunking
4. **Index Creation** - Converts to LOGGED and adds all indexes/constraints

**Large Table Optimization:**

Tables exceeding the threshold (default 1M rows) are automatically split into year-based chunks and processed in parallel:
- Finds suitable date/timestamp column (prioritizes `created_date`, `created_at`)
- Splits data by year
- Processes each year chunk in parallel (up to `--chunk-workers` threads)
- Other tables continue migrating concurrently (up to `--max-workers` total)

### 2. pgloader (Legacy)

Located in `bootstrap/pgloader/` directory.

**Files:**
- `load-all.sh` - Main script that loads all databases sequentially
- `main_app.load` - Configuration for main application database
- `trading.load` - Configuration for trading database
- `finance.load` - Configuration for finance database

**Known Issues (Resolved):**

The finance database has 19 tables with MariaDB-specific sequence defaults in the form:
```sql
DEFAULT nextval(`xchange_finance`.`seq_table_name`)
```

This was resolved by adding these CAST rules to `finance.load`:
```
CAST type int to int drop default drop typemod,
     type bigint to bigint drop default drop typemod
```

**Usage:**

```bash
# Load all databases via Makefile
make migrate

# Or manually with load-all.sh
cd bootstrap/pgloader
./load-all.sh

# Or individual databases
source .env
cd bootstrap/pgloader

# Trading
LOAD_FILE=$(envsubst < trading.load)
docker compose run --rm -T pgloader pgloader --verbose "$LOAD_FILE"

# Finance
LOAD_FILE=$(envsubst < finance.load)
docker compose run --rm -T pgloader pgloader --verbose "$LOAD_FILE"
```

## Environment Variables

Required in `.env`:

**MariaDB Sources:**
- `TRADING_HOST`, `TRADING_PORT`, `TRADING_USER`, `TRADING_PASS`, `TRADING_DB` - Trading database
- `FINANCE_HOST`, `FINANCE_PORT`, `FINANCE_USER`, `FINANCE_PASS`, `FINANCE_DB` - Finance database
- `LIVE_HOST`, `LIVE_PORT`, `LIVE_USER`, `LIVE_PASS`, `LIVE_DB` - Live database

**PostgreSQL Target:**
- `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASS`, `PG_DB` - PostgreSQL target database

## Database Schemas

**Source Databases:**
1. **Trading** (`xchange_trading`) - 14 tables
   - T_CARRIER, T_DEAL, T_DEAL_*, T_LOCATION, T_USER, V_ABSTRACT_OFFER*

2. **Finance** (`xchange_finance`) - 8 tables
   - T_ACCOUNT, T_INSURANCE_UNIT, T_INVOICE, T_LEASING_*, T_MEMBERSHIP_*

3. **Live** (`xchangelive`) - 15 tables
   - pipedrive_id_lookup, T_CARRIER, T_COMPANY_*, T_DEPOT_MAPPER, T_LOCATION, T_REQUEST*

**Target Database:**
- PostgreSQL (`cdc_pipeline`)
- Schemas: `xchange_trading`, `xchange_finance`, `xchangelive`

## Migration Strategy

**For CDC Pipeline (Recommended Approach):**

1. Use `migrate.py` for initial data load (non-destructive, incremental)
2. Use Debezium connectors for ongoing CDC (Change Data Capture)

**Important:**
- `migrate.py` does NOT drop existing schemas or tables
- You must manually drop tables if you want a clean slate
- Re-running migration on existing tables will skip table creation and attempt data insertion (may fail on duplicate keys)

## Performance Tuning

**For Small Databases (<1M rows per table):**
```bash
python3 migrate.py --database finance \
  --tables T_INVOICE,T_ACCOUNT \
  --max-workers 4
```

**For Large Databases:**
```bash
python3 migrate.py --database trading \
  --tables T_DEAL,T_ABSTRACT_OFFER \
  --max-workers 8 \
  --chunk-workers 6 \
  --batch-size 50000 \
  --threshold 500000
```

**Tips:**
- Increase `--max-workers` if you have many small-to-medium tables
- Increase `--chunk-workers` for tables with >1M rows
- Decrease `--batch-size` if running into memory issues
- Lower `--threshold` to enable chunking on smaller tables
