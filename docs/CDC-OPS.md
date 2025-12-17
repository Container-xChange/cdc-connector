# CDC Operations Guide

## Architecture

```
MariaDB (Fly.io) → Debezium → Redpanda → JDBC Sink → Postgres (Fly.io)
                       ↓
                 Schema History Topic
```

**Key decisions:**
- `snapshot.mode = schema_only` - No data snapshots, CDC only
- Tables are pre-created by pgloader
- Deduplication happens downstream using PK + event timestamp
- Explicit table allowlist via `TABLE_ALLOWLIST` env var

---

## Schema Change Handling

### What Debezium Does
- **Captures schema changes** via `include.schema.changes = true`
- **Stores schema history** in Kafka topic `marketplace.schema-history`
- **Does NOT automatically alter Postgres tables**

### ADD COLUMN (safe)
1. Add column in MariaDB
2. Debezium captures DDL event, continues streaming
3. Manually add column to Postgres:
   ```sql
   ALTER TABLE users ADD COLUMN new_field VARCHAR(255);
   ```
4. CDC resumes with new column in events

### DROP COLUMN (requires coordination)
1. Remove column from `TABLE_ALLOWLIST` if needed
2. Manually drop column in Postgres
3. Drop column in MariaDB
4. Monitor Debezium logs for errors

### RENAME COLUMN (dangerous)
Debezium sees this as DROP + ADD. Requires:
1. Pause connector: `curl -X PUT http://localhost:8083/connectors/mariadb-source-connector/pause`
2. Rename in both databases manually
3. Resume connector

### ANTI-PATTERN: Letting `auto.evolve = true`
❌ **Never enable `auto.evolve`** in sink connector. It can:
- Create duplicate columns with wrong types
- Break PK constraints
- Cause data loss on type mismatches

---

## Table Management

### Adding a Table
1. Ensure table exists in Postgres (run pgloader if needed)
2. Add to `TABLE_ALLOWLIST` in `connectors/.env`:
   ```bash
   TABLE_ALLOWLIST=marketplace.users,marketplace.orders,marketplace.new_table
   ```
3. Update connector config:
   ```bash
   make unregister-source
   make register-source
   ```
4. Verify topic creation:
   ```bash
   make topics | grep new_table
   ```

### Removing a Table
1. Remove from `TABLE_ALLOWLIST`
2. Update connector: `make unregister-source && make register-source`
3. Optionally delete topic:
   ```bash
   docker compose exec redpanda rpk topic delete marketplace.marketplace.table_name
   ```

### ANTI-PATTERN: Using Wildcards
❌ Don't use `table.include.list = "marketplace.*"`. Risks:
- System tables get CDC'd
- Temp tables flood Kafka
- Unpredictable resource usage

---

## Connector Lifecycle

### Initial Setup
```bash
# 1. Configure databases
cp connectors/.env.example connectors/.env
vim connectors/.env

# 2. Start infrastructure
make start

# 3. Register connectors
make register-source
make register-sink

# 4. Verify
make connectors
make topics
```

### Monitoring
```bash
# Service health
make check-health

# Connector status
make connector-status C=mariadb-source-connector

# View recent events
make consume-topic T=marketplace.marketplace.users

# UI monitoring
make console  # → http://localhost:8080
```

### Stopping CDC Without Data Loss
```bash
# Graceful shutdown
make stop  # Commits offsets before stopping
```

### Restarting After Failure
```bash
# Resume from last committed offset
make start
make register-all
```

Debezium uses offset storage in Kafka topic `debezium_offsets` - no data loss on restart.

---

## Common Operations

### Backfill Missing Data
CDC is incremental only. For backfills:
```bash
# Re-run pgloader for specific table
docker compose --profile migration run pgloader pgloader /pgloader/load-table.load

# Or manual INSERT INTO SELECT
psql -d cdc_pipeline -c "INSERT INTO users SELECT * FROM dblink(...) ON CONFLICT DO NOTHING"
```

### Reset CDC Position (nuclear option)
```bash
make unregister-source
docker compose exec redpanda rpk topic delete debezium_offsets
make register-source  # Starts from latest binlog position
```

### Dealing With Lag
Check lag via:
```bash
docker compose exec redpanda rpk group describe debezium-cluster
```

Remediation:
1. Increase `tasks.max` in connector config (parallel processing)
2. Increase `batch.size` in sink connector
3. Scale Debezium horizontally (run multiple Connect workers)

---

## Anti-Patterns

### ❌ Don't Rely on `updated_at` Cursors
CDC captures changes via binlog, not timestamps. An `UPDATE` without changing `updated_at` is still captured.

### ❌ Don't Enable Snapshots in Production
`snapshot.mode = initial` will lock tables and duplicate data already loaded by pgloader.

### ❌ Don't Modify Offset Topics Manually
Let Debezium manage `debezium_offsets`. Manual edits cause duplicate/missing events.

### ❌ Don't Run CDC Without Monitoring
Always monitor:
- Connector state (should be `RUNNING`)
- Task lag (should be < 1000 messages)
- Binlog position (should advance)

### ❌ Don't Ignore `FAILED` State
If connector enters `FAILED`:
```bash
make connector-status C=mariadb-source-connector
# Read error in logs
make logs-debezium

# Common fixes:
# - Check DB credentials
# - Verify binlog is enabled
# - Check table exists and has PK
```

### ❌ Don't Delete Schema History Topic
Topic `marketplace.schema-history` is critical. Deleting it forces Debezium to re-read entire binlog to reconstruct schema.

---

## Fly.io Production Setup

### 1. Deploy Redpanda
```bash
# Single-node Redpanda on Fly.io
fly apps create cdc-redpanda
fly volumes create redpanda_data --size 10 --region sjc
fly deploy -c fly-redpanda.toml
```

### 2. Deploy Debezium Connect
```bash
fly apps create cdc-debezium
fly secrets set \
  DB_HOST=<mariadb-host> \
  DB_USER=<cdc-user> \
  DB_PASSWORD=<pass> \
  SINK_DB_URL=jdbc:postgresql://<postgres-host>:5432/analytics \
  SINK_DB_USER=postgres \
  SINK_DB_PASSWORD=<pass> \
  TABLE_ALLOWLIST=marketplace.users,marketplace.orders

fly deploy -c fly-debezium.toml
```

### 3. Register Connectors
```bash
# Port-forward to Debezium
fly proxy 8083:8083 -a cdc-debezium

# Register (use localhost:8083)
make register-all
```

### 4. Monitor via fly logs
```bash
fly logs -a cdc-debezium
```

---

## Debugging

### No Events Flowing
```bash
# Check source connector
make connector-status C=mariadb-source-connector

# Check if topics exist
make topics

# Check if events are in Kafka
make consume-topic T=marketplace.marketplace.users

# Check sink connector
make connector-status C=postgres-sink-connector
```

### Sink Connector Failing
```bash
# Check Postgres connection
docker compose exec postgres psql -U postgres -d cdc_pipeline -c "\dt"

# Check if tables exist
# Check if PK columns match

# Verify topic format matches regex
make topics  # Should match: marketplace.marketplace.*
```

### Events Not Reaching Postgres
```bash
# Check transform rules in postgres-sink.json
# Ensure topic regex matches: marketplace\.(marketplace)\..*
# Ensure table name routing is correct

# Tail Postgres logs
docker compose logs -f postgres
```

---

## Performance Tuning

### Source Connector
- `snapshot.fetch.size`: Rows per fetch (default 10240)
- `max.queue.size`: Internal buffer (default 8192)
- `poll.interval.ms`: How often to poll binlog (default 1000)

### Sink Connector
- `batch.size`: Records per INSERT (default 3000)
- `tasks.max`: Parallel sink tasks (use # of tables)

### Redpanda
- Increase memory: `--memory=2G` in docker-compose
- Tune retention: `rpk topic alter-config <topic> --set retention.ms=86400000`

---

## Troubleshooting Checklist

- [ ] MariaDB has binlog enabled (`log_bin=ON`)
- [ ] MariaDB user has `REPLICATION SLAVE`, `REPLICATION CLIENT` privileges
- [ ] All tables have PRIMARY KEY
- [ ] Postgres tables exist with matching schema
- [ ] `TABLE_ALLOWLIST` matches actual table names
- [ ] Debezium connector state = `RUNNING`
- [ ] Kafka topics exist for all tables
- [ ] Sink connector state = `RUNNING`
- [ ] No lag in consumer group `debezium-cluster`

---

## Appendix: Required MariaDB Privileges

```sql
CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'cdc_pass';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
```

Verify binlog:
```sql
SHOW VARIABLES LIKE 'log_bin';          -- Should be ON
SHOW VARIABLES LIKE 'binlog_format';    -- Should be ROW
SHOW VARIABLES LIKE 'binlog_row_image'; -- Should be FULL
```
