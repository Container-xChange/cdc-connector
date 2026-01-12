#!/usr/bin/env python3
"""
MariaDB to Postgres migration tool
Handles all type conversions including bit(1) -> boolean
Supports parallel table migration with auto-discovery
"""
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from io import StringIO
import mysql.connector
import psycopg2
from psycopg2 import sql

# Load .env file
def load_env():
    """Load environment variables from .env file"""
    env_path = Path(__file__).parent.parent / '.env'
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    # Remove quotes if present
                    value = value.strip('"').strip("'")
                    os.environ[key] = value
        print(f"✅ Loaded environment from {env_path}")
    else:
        print(f"⚠️  No .env file found at {env_path}")

# Load environment variables on module import
load_env()

# Performance settings (adjust for stress testing)
BATCH_SIZE = int(os.environ.get('MIGRATION_BATCH_SIZE', '10000'))  # Rows per batch (reduced for memory)
MAX_WORKERS = int(os.environ.get('MIGRATION_WORKERS', '2'))  # Parallel tables (reduced for memory)
PREFETCH_SIZE = int(os.environ.get('MIGRATION_PREFETCH', '10000'))  # MySQL fetch size

# Database configurations
DATABASES = {
    'trading': {
        'mysql': {
            'host': 'TRADING_HOST',
            'port': 'TRADING_PORT',
            'user': 'TRADING_USER',
            'password': 'TRADING_PASS',
            'database': 'TRADING_DB'
        },
        'schema': 'xchange_trading',
        'table_pattern': 'T_%',  # Auto-discover tables matching pattern
        'include_tables': None  # Or specify list to override auto-discovery
    },
    'finance': {
        'mysql': {
            'host': 'FINANCE_HOST',
            'port': 'FINANCE_PORT',
            'user': 'FINANCE_USER',
            'password': 'FINANCE_PASS',
            'database': 'FINANCE_DB'
        },
        'schema': 'xchange_finance',
        'table_pattern': 'T_%',  # Auto-discover all tables with T_ prefix
        'include_tables': None  # Use pattern instead of explicit list
    },
    'live': {
        'mysql': {
            'host': 'LIVE_HOST',
            'port': 'LIVE_PORT',
            'user': 'LIVE_USER',
            'password': 'LIVE_PASS',
            'database': 'LIVE_DB'
        },
        'schema': 'xchangelive',
        'table_pattern': None,  # Not using pattern
        'include_tables': ['T_CARRIER']  # Only migrate T_CARRIER
    }
}

POSTGRES = {
    'host': 'PG_HOST',
    'port': 'PG_PORT',
    'user': 'PG_USER',
    'password': 'PG_PASS',
    'database': 'PG_DB'
}

# Type mapping from MySQL to Postgres
TYPE_MAP = {
    'tinyint(1)': 'boolean',
    'bit(1)': 'boolean',
    'tinyint': 'smallint',
    'smallint': 'smallint',
    'mediumint': 'integer',
    'int': 'integer',
    'bigint': 'bigint',
    'decimal': 'numeric',
    'float': 'real',
    'double': 'double precision',
    'date': 'date',
    'datetime': 'timestamp',
    'timestamp': 'timestamptz',
    'time': 'time',
    'char': 'varchar',
    'varchar': 'varchar',
    'text': 'text',
    'mediumtext': 'text',
    'longtext': 'text',
    'binary': 'bytea',
    'varbinary': 'bytea',
    'blob': 'bytea',
    'mediumblob': 'bytea',
    'longblob': 'bytea',
    'enum': 'varchar'
}

def get_env_value(key):
    """Get environment variable value"""
    val = os.environ.get(key)
    if not val:
        raise ValueError(f"Environment variable {key} not set")
    return val

def get_mysql_conn(db_config):
    """Create MySQL connection"""
    return mysql.connector.connect(
        host=get_env_value(db_config['host']),
        port=int(get_env_value(db_config['port'])),
        user=get_env_value(db_config['user']),
        password=get_env_value(db_config['password']),
        database=get_env_value(db_config['database']),
        connect_timeout=300,  # 5 minutes to connect
        use_pure=True,  # Use pure Python implementation (more stable for large results)
        connection_timeout=3600  # 1 hour for long queries
    )

def get_pg_conn():
    """Create Postgres connection"""
    return psycopg2.connect(
        host=get_env_value(POSTGRES['host']),
        port=int(get_env_value(POSTGRES['port'])),
        user=get_env_value(POSTGRES['user']),
        password=get_env_value(POSTGRES['password']),
        database=get_env_value(POSTGRES['database'])
    )

def convert_mysql_type(column_type, column_name):
    """Convert MySQL column type to Postgres type"""
    # Extract base type
    base_type = column_type.lower().split('(')[0]

    # Special cases
    if 'tinyint(1)' in column_type.lower():
        return 'boolean'
    if 'bit(1)' in column_type.lower():
        return 'boolean'
    if base_type == 'int' and 'unsigned' in column_type.lower():
        return 'bigint'

    # Check for type with precision
    if '(' in column_type and base_type in ['decimal', 'numeric', 'varchar', 'char']:
        precision = column_type[column_type.index('('):column_type.index(')')+1]
        pg_base = TYPE_MAP.get(base_type, 'text')
        if base_type in ['decimal', 'numeric']:
            return f"{pg_base}{precision}"
        elif base_type in ['varchar']:
            return f"{pg_base}{precision}"
        return pg_base

    return TYPE_MAP.get(base_type, 'text')

def convert_value(value, mysql_type, pg_type):
    """Convert MySQL value to Postgres-compatible value"""
    if value is None:
        return None

    # Handle bit(1) -> boolean
    if 'bit' in mysql_type.lower() and pg_type == 'boolean':
        if isinstance(value, bytes):
            return value != b'\x00'
        if isinstance(value, int):
            return value != 0
        return bool(value)

    # Handle tinyint(1) -> boolean
    if 'tinyint(1)' in mysql_type.lower() and pg_type == 'boolean':
        return bool(value)

    # Handle zero dates
    if pg_type in ['date', 'timestamp', 'timestamptz']:
        if str(value).startswith('0000-00-00'):
            return None

    # Remove NUL characters from strings (Postgres doesn't allow them)
    if isinstance(value, str) and '\x00' in value:
        return value.replace('\x00', '')

    # Remove NUL characters from bytes
    if isinstance(value, bytes) and b'\x00' in value:
        return value.replace(b'\x00', b'')

    return value

def create_schema(pg_conn, schema_name):
    """Create schema if not exists"""
    with pg_conn.cursor() as cur:
        cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema_name)))
        cur.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
        cur.execute(sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(
            sql.Identifier(schema_name),
            sql.Identifier(get_env_value(POSTGRES['user']))
        ))
    pg_conn.commit()

def get_table_schema(mysql_conn, table_name):
    """Get table schema from MySQL"""
    with mysql_conn.cursor() as cur:
        cur.execute(f"DESCRIBE {table_name}")
        columns = []
        for row in cur.fetchall():
            col_name, col_type, nullable, key, default, extra = row
            columns.append({
                'name': col_name,
                'type': col_type,
                'nullable': nullable == 'YES',
                'is_primary': key == 'PRI'
            })
        return columns

def create_table(pg_conn, schema_name, table_name, columns):
    """Create table in Postgres"""
    table_lower = table_name.lower()

    col_definitions = []
    primary_keys = []

    for col in columns:
        col_name_lower = col['name'].lower()
        pg_type = convert_mysql_type(col['type'], col['name'])

        col_def = f'"{col_name_lower}" {pg_type}'
        if not col['nullable']:
            col_def += ' NOT NULL'

        col_definitions.append(col_def)

        if col['is_primary']:
            primary_keys.append(f'"{col_name_lower}"')

    if primary_keys:
        col_definitions.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema_name}.{table_lower} (
        {', '.join(col_definitions)}
    )
    """

    with pg_conn.cursor() as cur:
        try:
            cur.execute(create_sql)
            pg_conn.commit()
        except Exception as e:
            pg_conn.rollback()
            raise Exception(f"Failed to create table {schema_name}.{table_lower}: {e}")

def discover_tables(mysql_config, pattern=None):
    """Auto-discover tables from MySQL database"""
    mysql_conn = get_mysql_conn(mysql_config)
    try:
        with mysql_conn.cursor() as cur:
            if pattern:
                cur.execute(f"SHOW TABLES LIKE '{pattern}'")
            else:
                cur.execute("SHOW TABLES")
            tables = [row[0] for row in cur.fetchall()]
        return tables
    finally:
        mysql_conn.close()

def migrate_table_data(mysql_config, pg_schema, table_name):
    """Migrate data for a single table"""
    mysql_conn = get_mysql_conn(mysql_config)
    pg_conn = get_pg_conn()

    try:
        table_lower = table_name.lower()
        start_time = datetime.now()

        # Get column info
        with mysql_conn.cursor() as cur:
            cur.execute(f"DESCRIBE {table_name}")
            columns = []
            for row in cur.fetchall():
                columns.append({
                    'name': row[0],
                    'type': row[1],
                    'pg_type': convert_mysql_type(row[1], row[0])
                })

        # Get total count
        with mysql_conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name}")
            total = cur.fetchone()[0]

        if total == 0:
            print(f"    {table_name}: 0 rows (empty table)")
            return True

        # Prepare column names for COPY
        col_names = [col["name"].lower() for col in columns]

        processed = 0
        last_progress = 0
        pg_cur = pg_conn.cursor()

        try:
            # Use chunked reading with LIMIT/OFFSET to avoid connection timeout
            chunk_size = BATCH_SIZE
            offset = 0

            while offset < total:
                # Fetch chunk from MySQL
                mysql_cur = mysql_conn.cursor()
                mysql_cur.execute(f"SELECT * FROM {table_name} LIMIT {chunk_size} OFFSET {offset}")

                # Build CSV-like buffer for COPY
                csv_buffer = StringIO()
                batch_count = 0

                for row in mysql_cur:
                    # Convert values
                    converted_values = []
                    for i, val in enumerate(row):
                        try:
                            converted_val = convert_value(val, columns[i]['type'], columns[i]['pg_type'])
                            if converted_val is None:
                                converted_values.append('\\N')  # Postgres NULL marker
                            elif isinstance(converted_val, bool):
                                converted_values.append('t' if converted_val else 'f')
                            elif isinstance(converted_val, (bytes, bytearray)):
                                # Handle binary data - convert to hex format
                                converted_values.append('\\\\x' + converted_val.hex())
                            elif isinstance(converted_val, str):
                                # Escape special characters for COPY
                                escaped = converted_val.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                                converted_values.append(escaped)
                            else:
                                converted_values.append(str(converted_val))
                        except Exception as e:
                            # Log problematic value and use None
                            print(f"      Warning: Error converting column {columns[i]['name']}: {e}")
                            converted_values.append('\\N')

                    csv_buffer.write('\t'.join(converted_values) + '\n')
                    batch_count += 1

                mysql_cur.close()

                # COPY batch to Postgres
                if batch_count > 0:
                    try:
                        csv_buffer.seek(0)
                        # Use SQL composition to properly handle schema.table
                        copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH (FORMAT TEXT, NULL '\\N')").format(
                            sql.Identifier(pg_schema),
                            sql.Identifier(table_lower),
                            sql.SQL(', ').join(map(sql.Identifier, col_names))
                        )
                        pg_cur.copy_expert(copy_sql.as_string(pg_conn), csv_buffer)
                        pg_conn.commit()
                        processed += batch_count

                        # Show progress every 10%
                        progress = 100 * processed // total
                        if progress >= last_progress + 10:
                            elapsed = (datetime.now() - start_time).total_seconds()
                            rate = processed / elapsed if elapsed > 0 else 0
                            print(f"    {table_name}: {processed}/{total} ({progress}%) - {rate:.0f} rows/sec")
                            last_progress = progress

                    except Exception as e:
                        print(f"      Error copying batch at offset {offset}: {e}")
                        print(f"      This batch will be skipped (COPY doesn't support row-level recovery)")
                        pg_conn.rollback()
                    finally:
                        # Clear buffer to free memory
                        csv_buffer.close()

                offset += chunk_size

        finally:
            pg_cur.close()

        elapsed = (datetime.now() - start_time).total_seconds()
        rate = processed / elapsed if elapsed > 0 else 0
        print(f"    {table_name}: ✅ {processed} rows in {elapsed:.1f}s ({rate:.0f} rows/sec)")
        return True

    except Exception as e:
        print(f"    {table_name}: ❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        mysql_conn.close()
        pg_conn.close()

def migrate_table(mysql_config, pg_schema, table_name):
    """Migrate a single table (schema + data)"""
    try:
        # Get schema and create table
        mysql_conn = get_mysql_conn(mysql_config)
        pg_conn = get_pg_conn()

        try:
            columns = get_table_schema(mysql_conn, table_name)
            create_table(pg_conn, pg_schema, table_name, columns)
            print(f"    {table_name}: Table created, starting data migration...")
        finally:
            mysql_conn.close()
            pg_conn.close()

        # Migrate data (uses fresh connections)
        return migrate_table_data(mysql_config, pg_schema, table_name)

    except Exception as e:
        print(f"    {table_name}: ❌ ERROR during setup: {e}")
        import traceback
        traceback.print_exc()
        return False

def migrate_database(db_name, config):
    """Migrate entire database"""
    print(f"\n{'='*60}")
    print(f"Migrating {db_name.upper()}")
    print(f"{'='*60}")

    # Discover or use configured tables
    if config.get('include_tables'):
        tables = config['include_tables']
        print(f"  Using configured table list: {len(tables)} tables")
    elif config.get('table_pattern'):
        tables = discover_tables(config['mysql'], config['table_pattern'])
        print(f"  Auto-discovered tables matching '{config['table_pattern']}': {len(tables)} tables")
    else:
        tables = discover_tables(config['mysql'])
        print(f"  Auto-discovered all tables: {len(tables)} tables")

    if not tables:
        print(f"  ⚠️  No tables found to migrate")
        return True

    # Create schema
    pg_conn = get_pg_conn()
    create_schema(pg_conn, config['schema'])
    pg_conn.close()
    print(f"  ✅ Schema {config['schema']} created")

    # PHASE 1: Create ALL tables first (sequential to avoid conflicts)
    print(f"\n  Creating {len(tables)} table schemas...")
    mysql_conn = get_mysql_conn(config['mysql'])
    pg_conn = get_pg_conn()
    try:
        for table in tables:
            columns = get_table_schema(mysql_conn, table)
            create_table(pg_conn, config['schema'], table, columns)
            table_lower = table.lower()
            print(f"    ✅ {table} -> {config['schema']}.{table_lower}")
        # CRITICAL: Commit all table creations before parallel data migration
        pg_conn.commit()
    finally:
        mysql_conn.close()
        pg_conn.close()

    # PHASE 2: Migrate data in parallel
    print(f"\n  Migrating data for {len(tables)} tables (workers={MAX_WORKERS}, batch_size={BATCH_SIZE})...")
    print(f"  Tables: {', '.join(tables[:5])}{', ...' if len(tables) > 5 else ''}")

    start_time = datetime.now()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(migrate_table_data, config['mysql'], config['schema'], table): table
            for table in tables
        }

        success_count = 0
        for future in as_completed(futures):
            table = futures[future]
            if future.result():
                success_count += 1

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"\n  ✅ {db_name.upper()} completed: {success_count}/{len(tables)} tables in {elapsed:.1f}s")
    return success_count == len(tables)

def main():
    print("="*60)
    print("MariaDB to Postgres Migration Tool")
    print("="*60)
    print(f"Performance settings:")
    print(f"  BATCH_SIZE: {BATCH_SIZE}")
    print(f"  MAX_WORKERS: {MAX_WORKERS}")
    print(f"  PREFETCH_SIZE: {PREFETCH_SIZE}")
    print()
    print(f"Tip: Adjust with environment variables:")
    print(f"  export MIGRATION_BATCH_SIZE=20000")
    print(f"  export MIGRATION_WORKERS=8")

    overall_start = datetime.now()

    # Migrate all databases
    for db_name, config in DATABASES.items():
        # Skip trading - already migrated (except t_abstract_offer_historization - too large)
        if db_name == 'trading' or db_name == 'finance':
            print(f"\nℹ️  Skipping {db_name} - already migrated")
            continue

        if not migrate_database(db_name, config):
            print(f"\n❌ Migration failed for {db_name}")
            sys.exit(1)

    overall_elapsed = (datetime.now() - overall_start).total_seconds()

    print("\n" + "="*60)
    print(f"✅ All databases migrated successfully in {overall_elapsed:.1f}s!")
    print("="*60)

if __name__ == '__main__':
    main()
