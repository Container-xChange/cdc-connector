# CDC Pipeline Project - CRITICAL INSTRUCTIONS

## 🚨 CDC PIPELINE ARCHITECTURE - READ THIS FIRST

**DATA FLOW:**
1. **pgloader** has ALREADY loaded initial snapshots from MariaDB → Postgres (user runs `make migrate`)
2. **Debezium source connectors** capture ONLY changes after setup (snapshot.mode: "schema_only")
3. **Debezium sink connectors** write changes from Kafka → Postgres (upsert mode)

**CRITICAL RULES:**
- **NEVER change snapshot.mode from "schema_only"** - Initial data is loaded via pgloader
- **NEVER suggest running `make migrate`** - Only user can run this
- Tables already exist in Postgres with primary keys - CDC syncs changes only
- Source connectors read binlog AFTER connector registration time
- To test CDC: user must make NEW updates in MariaDB AFTER connectors are registered

---

## 🚨 BEFORE DOING ANYTHING - MANDATORY CHECKS

**ALWAYS CHECK EXISTING FILES FIRST:**
1. Run `ls -la` in relevant directories
2. Check `Makefile` for existing targets (269 lines of commands)
3. Check `.env` for environment variables
4. Check `bootstrap/pgloader/` for migration scripts
5. Check `connectors/` for connector configs
6. Check `scripts/deploy/` for deployment scripts

**NEVER CREATE NEW FILES WITHOUT CHECKING IF THEY ALREADY EXIST**

---

## Existing Project Structure (DO NOT DUPLICATE)

```
├── Makefile                              # 269 lines - ALL CDC operations
├── .env                                  # Environment variables (3 MariaDB sources)
├── docker-compose.yml                    # Postgres service definition
├── bootstrap/
│   ├── Dockerfile                        # pgloader image
│   ├── pgloader/
│   │   ├── load-all.sh                   # Master migration script (EXISTS!)
│   │   ├── finance.load                  # Finance migration config
│   │   ├── finance.processed.load        # Processed version
│   │   ├── trading.load                  # Trading migration config
│   │   └── main_app.load                 # Main app migration config
│   └── sql/
│       └── add-primary-keys.sql          # Primary key creation script
├── connectors/
│   ├── Dockerfile                        # Debezium image
│   ├── sources/mariadb/
│   │   ├── trading.json                  # Trading source connector
│   │   ├── finance.json                  # Finance source connector
│   │   └── live.json                     # Live source connector
│   └── sinks/postgres/
│       ├── sink.json                     # Generic sink
│       ├── trading.json                  # Trading sink
│       ├── finance.json                  # Finance sink
│       └── live.json                     # Live sink
└── scripts/
    └── deploy/
        └── deploy-connectors.sh          # Connector deployment script (EXISTS!)
```

---

## Available Makefile Commands (USE THESE - DON'T RECREATE)

**Setup:**
- `make build-debezium` - Build Debezium image
- `make build-pgloader` - Build pgloader image
- `make start` - Start CDC stack (Postgres + Debezium)
- `make stop` - Stop CDC stack
- `make restart` - Restart CDC stack
- `make clean` - Stop and remove all containers/volumes

**Migration:**
- `make migrate` - Run pgloader migration (uses existing Dockerfile + load-all.sh)
- `make add-pks` - Add primary keys to tables

**Connectors:**
- `make register-trading` - Register trading source
- `make register-finance` - Register finance source
- `make register-live` - Register live source
- `make register-sink` - Register Postgres sink
- `make register-all` - Register all connectors (uses scripts/deploy/deploy-connectors.sh)
- `make unregister-all` - Delete all connectors
- `make connectors` - List all connectors and status

**Monitoring:**
- `make logs-debezium` - Tail Debezium logs
- `make logs-postgres` - Tail Postgres logs
- `make check-health` - Check service health
- `make connector-status C=<name>` - Get connector status

**Full Deployment:**
- `make deploy` - Build + start + migrate + add PKs + register connectors

---

## Data Sources (FROM .env)

### 3 MariaDB Instances:
1. **Trading** (`xchange_trading`) - 14 tables
   - Host: xc-trading.covl02ovmomq.eu-central-1.rds.amazonaws.com
   - Tables: T_CARRIER, T_DEAL, T_DEAL_*, T_LOCATION, T_USER, V_ABSTRACT_OFFER*

2. **Finance** (`xchange_finance`) - 8 tables
   - Host: xc-finance.covl02ovmomq.eu-central-1.rds.amazonaws.com
   - Tables: T_ACCOUNT, T_INSURANCE_UNIT, T_INVOICE, T_LEASING_*, T_MEMBERSHIP_*

3. **Live** (`xchangelive`) - 15 tables
   - Host: 172.31.23.19
   - Tables: pipedrive_id_lookup, T_CARRIER, T_COMPANY_*, T_DEPOT_MAPPER, T_LOCATION, T_REQUEST*

### Target:
- **Postgres** (`cdc_pipeline`)
  - Local: localhost:5432
  - Container: postgres:5432

### Kafka:
- AWS MSK cluster (3 brokers)
- Schema Registry: http://schema-registry-prod.eks

---

## CRITICAL RULES - READ CAREFULLY

### 🚫 DO NOT CREATE:
1. **New migration scripts** - Use `bootstrap/pgloader/load-all.sh`
2. **New deployment scripts** - Use `scripts/deploy/deploy-connectors.sh`
3. **New Makefile targets** - Check existing 40+ targets first
4. **New Dockerfiles** - `connectors/Dockerfile` and `bootstrap/Dockerfile` exist
5. **New connector configs** - 7 JSON files already configured
6. **Duplicate .env files** - Single `.env` at root with all configs

### ✅ DO:
1. **ALWAYS run `ls -la` before creating any file**
2. **Check Makefile** before suggesting new commands
3. **Read existing scripts** before writing new ones
4. **Use `make <target>`** instead of raw docker/curl commands
5. **Edit existing files** instead of creating new ones
6. **Ask user** if unclear whether something exists

### 📝 Common Tasks - USE EXISTING TOOLS:

**Migration?** → `make migrate` (uses load-all.sh)
**Deploy connectors?** → `make register-all` (uses deploy-connectors.sh)
**Check status?** → `make connectors` or `make check-health`
**View logs?** → `make logs-debezium` or `make logs-postgres`
**Full setup?** → `make deploy`

---

## Performance Requirements

- **Before creating files**: Check if they exist (ls, find, glob)
- **Before suggesting commands**: Check Makefile
- **Before writing scripts**: Check bootstrap/ and scripts/
- **Minimize tool calls**: Batch reads/checks when possible
- **No duplicates**: Never create what already exists

---

## 🎯 Current Status & Progress

### ✅ Successfully Tested:
- **T_ABSTRACT_OFFER CDC** - Working end-to-end
  - Trading source connector capturing changes
  - Kafka topics receiving events
  - Postgres sink writing updates
  - Verified with real database updates

### 🔄 Next Steps:
1. **Remove hardcoded values from connector configs**
   - Test with environment variables/parameterized configs
   - Ensure dynamic configuration still works
2. **Scale to remaining tables**
   - Apply working pattern to other Trading tables
   - Deploy Finance and Live connectors
3. **Production readiness**
   - Monitoring and alerting setup
   - Error handling validation

---

## Quick Reference

**Start everything:**
```bash
make deploy
```

**Check what's running:**
```bash
make check-health
make connectors
```

**Restart services:**
```bash
make restart
```

**Clean slate:**
```bash
make clean
```

---

# important-instruction-reminders
Do what has been asked; nothing more, nothing less.
NEVER create files unless they're absolutely necessary for achieving your goal.
ALWAYS prefer editing an existing file to creating a new one.
NEVER proactively create documentation files (*.md) or README files. Only create documentation files if explicitly requested by the User.
**MANDATORY: Check existing files/scripts/commands BEFORE creating anything new.**
