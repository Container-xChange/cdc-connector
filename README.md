# pgloader CDC Pipeline

## Overview

This directory contains pgloader configuration files and scripts for migrating data from MariaDB instances to PostgreSQL.

## Files

- `load-all.sh` - Main script that loads all databases sequentially
- `main_app.load` - Configuration for main application database
- `trading.load` - Configuration for trading database
- `finance.load` - Configuration for finance database

## Known Issues (Resolved)

### Finance Database - MariaDB Sequence Defaults

The finance database has 19 tables with MariaDB-specific sequence defaults in the form:
```sql
DEFAULT nextval(`xchange_finance`.`seq_table_name`)
```

This was causing pgloader to fail, but has been resolved by adding these CAST rules to `finance.load`:
```
CAST type int to int drop default drop typemod,
     type bigint to bigint drop default drop typemod
```

This strips all default values from integer columns during schema creation, preventing the invalid `nextval()` syntax from reaching PostgreSQL

## Usage

### Load all databases:
```bash
./load-all.sh
```

### Load individual databases:
```bash
source .env

# Main app
LOAD_FILE=$(envsubst < main_app.load)
docker compose run --rm -T pgloader pgloader --verbose "$LOAD_FILE"

# Trading
LOAD_FILE=$(envsubst < trading.load)
docker compose run --rm -T pgloader pgloader --verbose "$LOAD_FILE"

# Finance
LOAD_FILE=$(envsubst < finance.load)
docker compose run --rm -T pgloader pgloader --verbose "$LOAD_FILE"
```

## Environment Variables

Required in `.env`:
- `MARIA1_HOST`, `MARIA1_PORT`, `MARIA1_USER`, `MARIA1_PASS`, `MARIA1_DB` - Main app database
- `MARIA2_HOST`, `MARIA2_PORT`, `MARIA2_USER`, `MARIA2_PASS`, `MARIA2_DB` - Finance database
- `MARIA3_HOST`, `MARIA3_PORT`, `MARIA3_USER`, `MARIA3_PASS`, `MARIA3_DB` - Trading database (currently using MARIA1)
- `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASS`, `PG_DB` - PostgreSQL target database
