#!/usr/bin/env python3
"""
MariaDB to Postgres migration tool
Handles all type conversions including bit(1) -> boolean
Supports parallel table migration with auto-discovery
"""
import os
import sys
import argparse
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
    # Try multiple paths: same directory as script, or current working directory
    script_dir_env = Path(__file__).parent / '.env'
    cwd_env = Path.cwd() / '.env'

    env_path = None
    if script_dir_env.exists():
        env_path = script_dir_env
    elif cwd_env.exists():
        env_path = cwd_env

    if env_path:
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
        print(f"⚠️  No .env file found at {script_dir_env} or {cwd_env}")

# Load environment variables on module import
load_env()

# Performance settings (adjust for stress testing)
BATCH_SIZE = int(os.environ.get('MIGRATION_BATCH_SIZE', '100000'))  # Rows per batch (reduced for memory)
MAX_WORKERS = int(os.environ.get('MIGRATION_WORKERS', '2'))  # Parallel tables (reduced for memory)
PREFETCH_SIZE = int(os.environ.get('MIGRATION_PREFETCH', '10000'))  # MySQL fetch size
LARGE_TABLE_THRESHOLD = int(os.environ.get('LARGE_TABLE_THRESHOLD', '1000000'))  # 1M rows
CHUNK_WORKERS = int(os.environ.get('CHUNK_WORKERS', '4'))  # Parallel year chunks for large tables

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
        'table_pattern': None,  # Disabled - using explicit list
        'include_tables': []  # Only migrate this table
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
        # 'table_pattern': 'T_%',  # Auto-discover all tables with T_ prefix
        'include_tables': ['T_INVOICE_LINE_ITEM']  # Use pattern instead of explicit list
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
        'include_tables': None
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
    """Create schema if not exists (does NOT drop existing schema)"""
    with pg_conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
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

def get_table_indexes(mysql_conn, table_name):
    """Extract all indexes from MySQL table"""
    with mysql_conn.cursor() as cur:
        cur.execute(f"SHOW INDEX FROM {table_name}")
        indexes = {}
        for row in cur.fetchall():
            table, non_unique, key_name, seq_in_index, column_name, collation, cardinality, sub_part, packed, null, index_type, comment, index_comment, ignored = row

            # Skip PRIMARY - we'll handle it separately
            if key_name == 'PRIMARY':
                continue

            if key_name not in indexes:
                indexes[key_name] = {
                    'name': key_name,
                    'unique': non_unique == 0,
                    'columns': [],
                    'type': index_type
                }
            indexes[key_name]['columns'].append(column_name)

        return list(indexes.values())

def get_table_foreign_keys(mysql_conn, database_name, table_name):
    """Extract foreign key constraints from MySQL table"""
    with mysql_conn.cursor() as cur:
        query = """
            SELECT
                CONSTRAINT_NAME,
                COLUMN_NAME,
                REFERENCED_TABLE_NAME,
                REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
        """
        cur.execute(query, (database_name, table_name))

        fks = {}
        for row in cur.fetchall():
            constraint_name, column_name, ref_table, ref_column = row

            if constraint_name not in fks:
                fks[constraint_name] = {
                    'name': constraint_name,
                    'columns': [],
                    'ref_table': ref_table,
                    'ref_columns': []
                }
            fks[constraint_name]['columns'].append(column_name)
            fks[constraint_name]['ref_columns'].append(ref_column)

        return list(fks.values())

def create_table(pg_conn, schema_name, table_name, columns, skip_constraints=False, unlogged=False):
    """Create table in Postgres without indexes/constraints for fast loading (does NOT drop existing table)"""
    table_lower = table_name.lower()

    # Check if table already exists
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name = %s
            )
        """, (schema_name, table_lower))
        table_exists = cur.fetchone()[0]

        if table_exists:
            print(f"      ⚠️  Table {schema_name}.{table_lower} already exists - skipping creation")
            # Return primary keys from existing table
            primary_keys = [col['name'] for col in columns if col['is_primary']]
            return primary_keys

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

    # Only add PRIMARY KEY if not skipping constraints
    if primary_keys and not skip_constraints:
        col_definitions.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

    # Add UNLOGGED option for faster bulk loading
    unlogged_clause = "UNLOGGED" if unlogged else ""

    create_sql = f"""
    CREATE {unlogged_clause} TABLE {schema_name}.{table_lower} (
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

    return primary_keys  # Return PK columns for later constraint creation

def create_indexes_and_constraints(pg_conn, schema_name, table_name, primary_keys, indexes, foreign_keys):
    """Create all indexes and constraints after data load for optimal performance"""
    table_lower = table_name.lower()
    start_time = datetime.now()

    with pg_conn.cursor() as cur:
        # 1. Convert table to LOGGED (enable WAL)
        try:
            cur.execute(sql.SQL("ALTER TABLE {}.{} SET LOGGED").format(
                sql.Identifier(schema_name),
                sql.Identifier(table_lower)
            ))
            print(f"      ✅ Enabled WAL logging")
        except Exception as e:
            print(f"      ⚠️  Could not enable logging (table may already be logged): {e}")

        # 2. Add PRIMARY KEY
        if primary_keys:
            try:
                pk_columns = [f'"{pk.lower()}"' for pk in primary_keys]
                cur.execute(sql.SQL("ALTER TABLE {}.{} ADD PRIMARY KEY ({})").format(
                    sql.Identifier(schema_name),
                    sql.Identifier(table_lower),
                    sql.SQL(', ').join(map(sql.Identifier, [pk.lower() for pk in primary_keys]))
                ))
                print(f"      ✅ Created PRIMARY KEY on ({', '.join(primary_keys)})")
            except Exception as e:
                print(f"      ❌ Failed to create PRIMARY KEY: {e}")

        # 3. Create secondary indexes
        for idx in indexes:
            try:
                idx_name_lower = idx['name'].lower()
                col_list = ', '.join([f'"{col.lower()}"' for col in idx['columns']])

                if idx['unique']:
                    cur.execute(sql.SQL("CREATE UNIQUE INDEX {} ON {}.{} ({})").format(
                        sql.Identifier(idx_name_lower),
                        sql.Identifier(schema_name),
                        sql.Identifier(table_lower),
                        sql.SQL(', ').join(map(sql.Identifier, [col.lower() for col in idx['columns']]))
                    ))
                    print(f"      ✅ Created UNIQUE INDEX {idx_name_lower} on ({', '.join(idx['columns'])})")
                else:
                    cur.execute(sql.SQL("CREATE INDEX {} ON {}.{} ({})").format(
                        sql.Identifier(idx_name_lower),
                        sql.Identifier(schema_name),
                        sql.Identifier(table_lower),
                        sql.SQL(', ').join(map(sql.Identifier, [col.lower() for col in idx['columns']]))
                    ))
                    print(f"      ✅ Created INDEX {idx_name_lower} on ({', '.join(idx['columns'])})")
            except Exception as e:
                print(f"      ❌ Failed to create index {idx['name']}: {e}")

        # 4. Create foreign keys (optional - can be slow, consider skipping for CDC use case)
        # Uncomment if you need FK constraints:
        # for fk in foreign_keys:
        #     try:
        #         fk_name_lower = fk['name'].lower()
        #         src_cols = ', '.join([f'"{col.lower()}"' for col in fk['columns']])
        #         ref_cols = ', '.join([f'"{col.lower()}"' for col in fk['ref_columns']])
        #         ref_table_lower = fk['ref_table'].lower()
        #
        #         cur.execute(sql.SQL(
        #             "ALTER TABLE {}.{} ADD CONSTRAINT {} FOREIGN KEY ({}) REFERENCES {}.{} ({})"
        #         ).format(
        #             sql.Identifier(schema_name),
        #             sql.Identifier(table_lower),
        #             sql.Identifier(fk_name_lower),
        #             sql.SQL(src_cols),
        #             sql.Identifier(schema_name),
        #             sql.Identifier(ref_table_lower),
        #             sql.SQL(ref_cols)
        #         ))
        #         print(f"      ✅ Created FK {fk_name_lower} -> {fk['ref_table']}")
        #     except Exception as e:
        #         print(f"      ⚠️  Failed to create FK {fk['name']}: {e}")

        pg_conn.commit()

    elapsed = (datetime.now() - start_time).total_seconds()
    print(f"      ⏱️  Index creation took {elapsed:.1f}s")

def find_date_column(mysql_conn, table_name):
    """Find a suitable date/timestamp column for chunking (prioritize created_date, created_at, etc.)"""
    with mysql_conn.cursor() as cur:
        cur.execute(f"DESCRIBE {table_name}")
        date_columns = []
        for row in cur.fetchall():
            col_name, col_type, nullable, key, default, extra = row
            col_type_lower = col_type.lower()
            col_name_lower = col_name.lower()

            # Look for datetime/timestamp columns
            if 'datetime' in col_type_lower or 'timestamp' in col_type_lower:
                # Prioritize created_date, created_at, date_created
                priority = 0
                if 'created' in col_name_lower:
                    priority = 3
                elif 'date' in col_name_lower or 'time' in col_name_lower:
                    priority = 2
                else:
                    priority = 1

                date_columns.append((priority, col_name))

        # Return highest priority column
        if date_columns:
            date_columns.sort(reverse=True)
            return date_columns[0][1]
        return None

def get_year_ranges(mysql_conn, table_name, date_column):
    """Get year ranges for chunking large tables"""
    with mysql_conn.cursor() as cur:
        # Get min/max years
        query = f"""
            SELECT
                YEAR(MIN({date_column})) as min_year,
                YEAR(MAX({date_column})) as max_year,
                COUNT(*) as total_rows
            FROM {table_name}
            WHERE {date_column} IS NOT NULL
        """
        cur.execute(query)
        result = cur.fetchone()

        if not result or result[0] is None:
            return []

        min_year, max_year, total_rows = result

        # Generate year ranges
        years = []
        for year in range(min_year, max_year + 1):
            years.append(year)

        return years

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

def migrate_year_chunk(mysql_config, pg_schema, table_name, date_column, year, columns):
    """Migrate a single year chunk of data"""
    mysql_conn = get_mysql_conn(mysql_config)
    pg_conn = get_pg_conn()

    try:
        table_lower = table_name.lower()
        start_time = datetime.now()

        # Count rows for this year
        with mysql_conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE YEAR({date_column}) = {year}")
            total = cur.fetchone()[0]

        if total == 0:
            return year, 0, 0

        # Prepare column names for COPY
        col_names = [col["name"].lower() for col in columns]

        processed = 0
        last_progress = 0
        pg_cur = pg_conn.cursor()

        try:
            # Fetch data for this year
            chunk_size = BATCH_SIZE
            offset = 0

            while offset < total:
                mysql_cur = mysql_conn.cursor()
                mysql_cur.execute(f"""
                    SELECT * FROM {table_name}
                    WHERE YEAR({date_column}) = {year}
                    LIMIT {chunk_size} OFFSET {offset}
                """)

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
                                converted_values.append('\\N')
                            elif isinstance(converted_val, bool):
                                converted_values.append('t' if converted_val else 'f')
                            elif isinstance(converted_val, (bytes, bytearray)):
                                converted_values.append('\\\\x' + converted_val.hex())
                            elif isinstance(converted_val, str):
                                escaped = converted_val.replace('\\', '\\\\').replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
                                converted_values.append(escaped)
                            else:
                                converted_values.append(str(converted_val))
                        except Exception:
                            converted_values.append('\\N')

                    csv_buffer.write('\t'.join(converted_values) + '\n')
                    batch_count += 1

                mysql_cur.close()

                # COPY batch to Postgres
                if batch_count > 0:
                    try:
                        csv_buffer.seek(0)
                        copy_sql = sql.SQL("COPY {}.{} ({}) FROM STDIN WITH (FORMAT TEXT, NULL '\\N')").format(
                            sql.Identifier(pg_schema),
                            sql.Identifier(table_lower),
                            sql.SQL(', ').join(map(sql.Identifier, col_names))
                        )
                        pg_cur.copy_expert(copy_sql.as_string(pg_conn), csv_buffer)
                        processed += batch_count

                        # Show progress every 10% within this year chunk
                        progress = 100 * processed // total
                        if progress >= last_progress + 10:
                            elapsed = (datetime.now() - start_time).total_seconds()
                            rate = processed / elapsed if elapsed > 0 else 0
                            print(f"      Year {year}: {processed:,}/{total:,} ({progress}%) - {rate:.0f} rows/sec")
                            last_progress = progress
                    except Exception as e:
                        print(f"      Year {year}: Error copying batch at offset {offset}: {e}")
                        pg_conn.rollback()
                    finally:
                        csv_buffer.close()

                offset += chunk_size

            # Commit all batches for this year
            pg_conn.commit()

        finally:
            pg_cur.close()

        elapsed = (datetime.now() - start_time).total_seconds()
        return year, processed, elapsed

    except Exception as e:
        print(f"      Year {year}: ERROR: {e}")
        return year, 0, 0
    finally:
        mysql_conn.close()
        pg_conn.close()

def migrate_table_data(mysql_config, pg_schema, table_name):
    """Migrate data for a single table - uses parallel year-chunking for large tables"""
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

        # FOR LARGE TABLES: Use parallel year-based chunking
        if total >= LARGE_TABLE_THRESHOLD:
            date_column = find_date_column(mysql_conn, table_name)

            if date_column:
                print(f"    {table_name}: Large table ({total:,} rows) - using parallel year-based chunking on '{date_column}'")
                years = get_year_ranges(mysql_conn, table_name, date_column)

                if years and len(years) > 1:
                    print(f"    {table_name}: Chunking into {len(years)} years ({min(years)}-{max(years)})")

                    # Close connections - each worker will create their own
                    mysql_conn.close()
                    pg_conn.close()

                    # Migrate years in parallel
                    with ThreadPoolExecutor(max_workers=CHUNK_WORKERS) as executor:
                        futures = {
                            executor.submit(migrate_year_chunk, mysql_config, pg_schema, table_name, date_column, year, columns): year
                            for year in years
                        }

                        total_processed = 0
                        for future in as_completed(futures):
                            year = futures[future]
                            year_result, rows_processed, elapsed = future.result()
                            total_processed += rows_processed
                            if rows_processed > 0:
                                rate = rows_processed / elapsed if elapsed > 0 else 0
                                print(f"      Year {year}: ✅ {rows_processed:,} rows in {elapsed:.1f}s ({rate:.0f} rows/sec)")

                    overall_elapsed = (datetime.now() - start_time).total_seconds()
                    overall_rate = total_processed / overall_elapsed if overall_elapsed > 0 else 0
                    print(f"    {table_name}: ✅ {total_processed:,} rows in {overall_elapsed:.1f}s ({overall_rate:.0f} rows/sec)")
                    return True
                else:
                    print(f"    {table_name}: Only 1 year found, falling back to sequential migration")
            else:
                print(f"    {table_name}: No date column found, falling back to sequential migration")

        # FALLBACK: Sequential migration for small tables or tables without date columns

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
                        # Don't commit per batch - commit once at end for speed
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

            # Commit all batches at once
            pg_conn.commit()

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

def migrate_table(mysql_config, pg_schema, table_name, database_name):
    """Migrate a single table (3-phase: create table -> load data -> add indexes)"""
    try:
        mysql_conn = get_mysql_conn(mysql_config)
        pg_conn = get_pg_conn()

        # PHASE 1: Extract schema info
        print(f"    {table_name}: Extracting schema...")
        columns = get_table_schema(mysql_conn, table_name)
        indexes = get_table_indexes(mysql_conn, table_name)
        foreign_keys = get_table_foreign_keys(mysql_conn, database_name, table_name)

        # Get primary keys for later
        primary_keys = [col['name'] for col in columns if col['is_primary']]

        # Create UNLOGGED table WITHOUT constraints for fast loading
        create_table(pg_conn, pg_schema, table_name, columns, skip_constraints=True, unlogged=True)
        print(f"    {table_name}: ✅ Table created (UNLOGGED, no indexes)")

        mysql_conn.close()
        pg_conn.close()

        # PHASE 2: Load data (uses fresh connections)
        print(f"    {table_name}: Loading data...")
        success = migrate_table_data(mysql_config, pg_schema, table_name)

        if not success:
            return False

        # PHASE 3: Add indexes and constraints
        print(f"    {table_name}: Creating indexes and constraints...")
        pg_conn = get_pg_conn()
        try:
            create_indexes_and_constraints(pg_conn, pg_schema, table_name, primary_keys, indexes, foreign_keys)
            print(f"    {table_name}: ✅ All indexes created")
        finally:
            pg_conn.close()

        return True

    except Exception as e:
        print(f"    {table_name}: ❌ ERROR during migration: {e}")
        import traceback
        traceback.print_exc()
        return False

def migrate_database(db_name, config, table_filter=None, max_workers=None):
    """Migrate entire database using optimized 3-phase approach"""
    print(f"\n{'='*60}")
    print(f"Migrating {db_name.upper()}")
    print(f"{'='*60}")

    # Determine workers
    workers = max_workers if max_workers else MAX_WORKERS

    # Discover or use configured tables
    if table_filter:
        tables = table_filter
        print(f"  Using user-specified table list: {len(tables)} tables")
    elif config.get('include_tables'):
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

    # Create schema (does not drop)
    pg_conn = get_pg_conn()
    create_schema(pg_conn, config['schema'])
    pg_conn.close()
    print(f"  ✅ Schema {config['schema']} ready")

    # Get database name for FK extraction
    database_name = get_env_value(config['mysql']['database'])

    # OPTIMIZED MIGRATION: Each table does 3 phases independently
    print(f"\n  Migrating {len(tables)} tables (workers={workers}, batch_size={BATCH_SIZE})...")
    print(f"  Strategy: UNLOGGED tables → Load data → Add indexes")
    print(f"  Tables: {', '.join(tables[:5])}{', ...' if len(tables) > 5 else ''}")

    start_time = datetime.now()

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(migrate_table, config['mysql'], config['schema'], table, database_name): table
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

def parse_args():
    """Parse command-line arguments"""
    parser = argparse.ArgumentParser(
        description='MariaDB to Postgres migration tool with parallel processing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate specific tables from trading database with 5 workers
  python3 migrate.py --database trading --tables T_ABSTRACT_OFFER,T_DEAL --max-workers 5

  # Migrate single table from finance database with default workers
  python3 migrate.py --database finance --tables T_INVOICE

  # Migrate with custom batch size and threshold
  python3 migrate.py --database trading --tables T_DEAL --batch-size 50000 --threshold 500000
        """
    )

    parser.add_argument(
        '--database',
        type=str,
        required=True,
        choices=['trading', 'finance', 'live'],
        help='Database to migrate (trading, finance, or live)'
    )

    parser.add_argument(
        '--tables',
        type=str,
        required=True,
        help='Comma-separated list of tables to migrate (e.g., T_DEAL,T_CARRIER).'
    )

    parser.add_argument(
        '--max-workers',
        type=int,
        help=f'Number of parallel workers for table migration (default: {MAX_WORKERS})'
    )

    parser.add_argument(
        '--chunk-workers',
        type=int,
        help=f'Number of parallel workers for year-chunk processing in large tables (default: {CHUNK_WORKERS})'
    )

    parser.add_argument(
        '--batch-size',
        type=int,
        help=f'Rows per batch (default: {BATCH_SIZE})'
    )

    parser.add_argument(
        '--threshold',
        type=int,
        help=f'Row threshold for large table chunking (default: {LARGE_TABLE_THRESHOLD:,})'
    )

    return parser.parse_args()

def main():
    args = parse_args()

    # Override global settings if provided
    global MAX_WORKERS, CHUNK_WORKERS, BATCH_SIZE, LARGE_TABLE_THRESHOLD

    if args.max_workers:
        MAX_WORKERS = args.max_workers
    if args.chunk_workers:
        CHUNK_WORKERS = args.chunk_workers
    if args.batch_size:
        BATCH_SIZE = args.batch_size
    if args.threshold:
        LARGE_TABLE_THRESHOLD = args.threshold

    print("="*60)
    print("MariaDB to Postgres Migration Tool")
    print("="*60)
    print(f"Target Database: {args.database.upper()}")
    table_list = [t.strip() for t in args.tables.split(',')]
    print(f"Target Tables: {', '.join(table_list)}")
    print()
    print(f"Performance settings:")
    print(f"  BATCH_SIZE: {BATCH_SIZE}")
    print(f"  MAX_WORKERS: {MAX_WORKERS} (parallel tables)")
    print(f"  CHUNK_WORKERS: {CHUNK_WORKERS} (parallel year chunks)")
    print(f"  LARGE_TABLE_THRESHOLD: {LARGE_TABLE_THRESHOLD:,} rows")
    print(f"  PREFETCH_SIZE: {PREFETCH_SIZE}")
    print()
    print(f"Optimizations:")
    print(f"  ✅ UNLOGGED tables during data load")
    print(f"  ✅ Deferred index creation")
    print(f"  ✅ Single commit per table")
    print(f"  ✅ Parallel year-chunking for large tables")
    print(f"  ✅ Non-destructive mode (existing tables/schemas preserved)")

    overall_start = datetime.now()

    # Get database config
    if args.database not in DATABASES:
        print(f"\n❌ Database '{args.database}' not found in configuration")
        print(f"Available databases: {', '.join(DATABASES.keys())}")
        sys.exit(1)

    config = DATABASES[args.database]

    # Parse table list (required)
    table_filter = [t.strip() for t in args.tables.split(',')]

    # Migrate the specified database
    if not migrate_database(args.database, config, table_filter=table_filter, max_workers=MAX_WORKERS):
        print(f"\n❌ Migration failed for {args.database}")
        sys.exit(1)

    overall_elapsed = (datetime.now() - overall_start).total_seconds()

    print("\n" + "="*60)
    print(f"✅ Migration completed successfully in {overall_elapsed:.1f}s!")
    print("="*60)

if __name__ == '__main__':
    main()
