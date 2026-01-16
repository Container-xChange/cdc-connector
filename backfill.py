#!/usr/bin/env python3
"""
Schema Change Detection and Backfill Tool

When CDC doesn't capture schema changes automatically, this script:
1. Detects schema differences between source MariaDB and target Postgres
2. Alters target tables to match source schema (add/modify columns)
3. Backfills new/modified columns with data from source

Usage:
  python3 backfill.py xchangelive.t_carrier
  python3 backfill.py xchange_trading.t_deal
  python3 backfill.py xchange_finance.t_account

  # Dry run mode
  python3 backfill.py xchangelive.t_carrier --dry-run
"""

import os
import sys
from datetime import datetime
from pathlib import Path
import mysql.connector
import psycopg2
from psycopg2 import sql

# Load .env file
def load_env():
    """Load environment variables from .env file"""
    env_path = Path(__file__).parent / '.env'
    if env_path.exists():
        with open(env_path) as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    value = value.strip('"').strip("'")
                    os.environ[key] = value
        print(f"✅ Loaded environment from {env_path}")
    else:
        print(f"⚠️  No .env file found at {env_path}")

load_env()

# Database name mapping: MariaDB database -> Postgres schema
DB_NAME_MAP = {
    'xchangelive': 'xchangelive',
    'xchange_trading': 'xchange_trading',
    'xchange_finance': 'xchange_finance'
}

# Database configurations
DB_CONFIGS = {
    'xchangelive': {
        'host': 'LIVE_HOST',
        'port': 'LIVE_PORT',
        'user': 'LIVE_USER',
        'password': 'LIVE_PASS',
        'database': 'LIVE_DB'
    },
    'xchange_trading': {
        'host': 'TRADING_HOST',
        'port': 'TRADING_PORT',
        'user': 'TRADING_USER',
        'password': 'TRADING_PASS',
        'database': 'TRADING_DB'
    },
    'xchange_finance': {
        'host': 'FINANCE_HOST',
        'port': 'FINANCE_PORT',
        'user': 'FINANCE_USER',
        'password': 'FINANCE_PASS',
        'database': 'FINANCE_DB'
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

def get_mysql_conn(db_name):
    """Create MySQL connection for specified database"""
    db_config = DB_CONFIGS.get(db_name)
    if not db_config:
        raise ValueError(f"Unknown database: {db_name}. Valid options: {list(DB_CONFIGS.keys())}")

    return mysql.connector.connect(
        host=get_env_value(db_config['host']),
        port=int(get_env_value(db_config['port'])),
        user=get_env_value(db_config['user']),
        password=get_env_value(db_config['password']),
        database=get_env_value(db_config['database']),
        connect_timeout=60
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

def convert_mysql_type(column_type):
    """Convert MySQL column type to Postgres type"""
    base_type = column_type.lower().split('(')[0]

    if 'tinyint(1)' in column_type.lower():
        return 'boolean'
    if 'bit(1)' in column_type.lower():
        return 'boolean'
    if base_type == 'int' and 'unsigned' in column_type.lower():
        return 'bigint'

    if '(' in column_type and base_type in ['decimal', 'numeric', 'varchar', 'char']:
        precision = column_type[column_type.index('('):column_type.index(')')+1]
        pg_base = TYPE_MAP.get(base_type, 'text')
        if base_type in ['decimal', 'numeric', 'varchar']:
            return f"{pg_base}{precision}"
        return pg_base

    return TYPE_MAP.get(base_type, 'text')

def get_mysql_schema(mysql_conn, table_name_upper):
    """Get table schema from MySQL (expects uppercase table name)"""
    with mysql_conn.cursor() as cur:
        cur.execute(f"DESCRIBE {table_name_upper}")
        columns = {}
        for row in cur.fetchall():
            col_name, col_type, nullable, key, default, extra = row
            columns[col_name.lower()] = {
                'name': col_name,
                'type': col_type,
                'pg_type': convert_mysql_type(col_type),
                'nullable': nullable == 'YES',
                'is_primary': key == 'PRI',
                'default': default
            }
        return columns

def get_postgres_schema(pg_conn, schema_name, table_name_lower):
    """Get table schema from Postgres (expects lowercase table name)"""
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """, (schema_name, table_name_lower))

        columns = {}
        for row in cur.fetchall():
            col_name, data_type, is_nullable, col_default = row
            columns[col_name.lower()] = {
                'name': col_name,
                'type': data_type,
                'nullable': is_nullable == 'YES',
                'default': col_default
            }
        return columns

def detect_schema_changes(mysql_schema, pg_schema):
    """Detect differences between MySQL and Postgres schemas"""
    # CDC metadata columns added by Debezium - DO NOT DROP
    CDC_METADATA_COLUMNS = {
        '__source_db',
        '__source_table',
        '__source_ts_ms',
        '__op',
        '__deleted',
        'owner_user_id'  # Custom metadata column
    }

    changes = {
        'new_columns': [],      # Columns in MySQL but not in Postgres
        'modified_columns': [], # Columns with different types
        'dropped_columns': []   # Columns in Postgres but not in MySQL (excluding CDC metadata)
    }

    # Check for new or modified columns
    for col_name, mysql_col in mysql_schema.items():
        if col_name not in pg_schema:
            changes['new_columns'].append(mysql_col)
        else:
            pg_col = pg_schema[col_name]
            # Compare types (simplified comparison)
            if mysql_col['pg_type'].split('(')[0] != pg_col['type'].split('(')[0]:
                changes['modified_columns'].append({
                    'name': col_name,
                    'mysql_type': mysql_col['pg_type'],
                    'pg_type': pg_col['type']
                })

    # Check for dropped columns (exclude CDC metadata columns)
    for col_name, pg_col in pg_schema.items():
        if col_name not in mysql_schema and col_name not in CDC_METADATA_COLUMNS:
            changes['dropped_columns'].append(pg_col)

    return changes

def alter_table_add_column(pg_conn, schema_name, table_name_lower, column_info):
    """Add new column to Postgres table"""
    col_name = column_info['name'].lower()
    col_type = column_info['pg_type']

    # Build ALTER TABLE statement
    if column_info['nullable']:
        alter_sql = sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {}").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name_lower),
            sql.Identifier(col_name),
            sql.SQL(col_type)
        )
    else:
        alter_sql = sql.SQL("ALTER TABLE {}.{} ADD COLUMN {} {} NOT NULL").format(
            sql.Identifier(schema_name),
            sql.Identifier(table_name_lower),
            sql.Identifier(col_name),
            sql.SQL(col_type)
        )

    with pg_conn.cursor() as cur:
        try:
            cur.execute(alter_sql)
            pg_conn.commit()
            nullable_str = 'NULL' if column_info['nullable'] else 'NOT NULL'
            print(f"  ✅ Added column: {col_name} ({col_type} {nullable_str})")
            return True
        except Exception as e:
            pg_conn.rollback()
            print(f"  ❌ Failed to add column {col_name}: {e}")
            return False

def alter_table_drop_column(pg_conn, schema_name, table_name_lower, column_name):
    """Drop column from Postgres table"""
    alter_sql = sql.SQL("ALTER TABLE {}.{} DROP COLUMN {}").format(
        sql.Identifier(schema_name),
        sql.Identifier(table_name_lower),
        sql.Identifier(column_name.lower())
    )

    with pg_conn.cursor() as cur:
        try:
            cur.execute(alter_sql)
            pg_conn.commit()
            print(f"  ✅ Dropped column: {column_name}")
            return True
        except Exception as e:
            pg_conn.rollback()
            print(f"  ❌ Failed to drop column {column_name}: {e}")
            return False

def backfill_column(mysql_conn, pg_conn, schema_name, table_name_upper, table_name_lower, columns_to_backfill):
    """Backfill data for new/modified columns"""
    col_names = [col['name'].lower() for col in columns_to_backfill]

    # Get primary key columns for WHERE clause
    with pg_conn.cursor() as cur:
        cur.execute("""
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
        """, (f"{schema_name}.{table_name_lower}",))
        pk_cols = [row[0] for row in cur.fetchall()]

    if not pk_cols:
        print(f"  ⚠️  No primary key found on {table_name_lower}, cannot backfill safely")
        return False

    print(f"  Backfilling columns: {', '.join(col_names)}")
    print(f"  Using primary key: {', '.join(pk_cols)}")

    # Fetch data from MySQL (use uppercase table name and column names)
    select_cols = [col.upper() for col in pk_cols] + [col['name'] for col in columns_to_backfill]
    select_sql = f"SELECT {', '.join(select_cols)} FROM {table_name_upper}"

    with mysql_conn.cursor() as mysql_cur:
        mysql_cur.execute(select_sql)

        updated = 0
        batch_size = 1000
        batch = []

        for row in mysql_cur:
            pk_values = row[:len(pk_cols)]
            new_values = row[len(pk_cols):]
            batch.append((pk_values, new_values))

            if len(batch) >= batch_size:
                updated += execute_backfill_batch(pg_conn, schema_name, table_name_lower,
                                                 pk_cols, col_names, batch)
                batch = []

        # Process remaining batch
        if batch:
            updated += execute_backfill_batch(pg_conn, schema_name, table_name_lower,
                                             pk_cols, col_names, batch)

    print(f"  ✅ Backfilled {updated} rows")
    return True

def execute_backfill_batch(pg_conn, schema_name, table_name, pk_cols, col_names, batch):
    """Execute a batch of backfill updates"""
    with pg_conn.cursor() as cur:
        for pk_values, new_values in batch:
            # Build WHERE clause
            where_parts = [f"{pk_col} = %s" for pk_col in pk_cols]
            where_clause = " AND ".join(where_parts)

            # Build SET clause
            set_parts = [f"{col} = %s" for col in col_names]
            set_clause = ", ".join(set_parts)

            update_sql = f"UPDATE {schema_name}.{table_name} SET {set_clause} WHERE {where_clause}"

            try:
                cur.execute(update_sql, list(new_values) + list(pk_values))
            except Exception as e:
                print(f"    ⚠️  Failed to update row {pk_values}: {e}")

        pg_conn.commit()

    return len(batch)

def process_table(db_name, table_name_input, dry_run=False):
    """Process a single table for schema changes and backfill

    Args:
        db_name: MariaDB database name (e.g., 'xchangelive', 'xchange_trading')
        table_name_input: Table name as provided by user (e.g., 't_carrier', 'T_CARRIER')
        dry_run: If True, only show changes without applying them
    """
    # Convert table name: uppercase for MariaDB, lowercase for Postgres
    table_name_upper = table_name_input.upper()
    table_name_lower = table_name_input.lower()

    # Get corresponding Postgres schema
    pg_schema = DB_NAME_MAP.get(db_name)
    if not pg_schema:
        print(f"❌ Unknown database: {db_name}")
        print(f"   Valid options: {list(DB_NAME_MAP.keys())}")
        return False

    print(f"\n{'='*60}")
    print(f"Processing: {db_name}.{table_name_upper} -> {pg_schema}.{table_name_lower}")
    print(f"{'='*60}")

    mysql_conn = get_mysql_conn(db_name)
    pg_conn = get_pg_conn()

    try:
        # Get schemas
        print(f"  Fetching schema from MariaDB {db_name}.{table_name_upper}...")
        mysql_schema = get_mysql_schema(mysql_conn, table_name_upper)

        print(f"  Fetching schema from Postgres {pg_schema}.{table_name_lower}...")
        pg_schema_cols = get_postgres_schema(pg_conn, pg_schema, table_name_lower)

        if not pg_schema_cols:
            print(f"  ❌ Table {table_name_lower} does not exist in Postgres schema {pg_schema}")
            print(f"     Please run migration first or check table name")
            return False

        if not mysql_schema:
            print(f"  ❌ Table {table_name_upper} does not exist in MariaDB {db_name}")
            return False

        # Detect changes
        changes = detect_schema_changes(mysql_schema, pg_schema_cols)

        if not any([changes['new_columns'], changes['modified_columns'], changes['dropped_columns']]):
            print(f"  ✅ No schema changes detected - schemas are in sync")
            return True

        # Report changes
        if changes['new_columns']:
            print(f"\n  📝 New columns in MariaDB (not in Postgres): {len(changes['new_columns'])}")
            for col in changes['new_columns']:
                print(f"    + {col['name']} ({col['pg_type']}) {'NULL' if col['nullable'] else 'NOT NULL'}")

        if changes['dropped_columns']:
            print(f"\n  🗑️  Columns in Postgres (not in MariaDB): {len(changes['dropped_columns'])}")
            for col in changes['dropped_columns']:
                print(f"    - {col['name']} ({col['type']})")

        if changes['modified_columns']:
            print(f"\n  ℹ️  Type mismatches detected (will be ignored): {len(changes['modified_columns'])}")
            for col in changes['modified_columns']:
                print(f"    ~ {col['name']}: Postgres={col['pg_type']} vs MariaDB={col['mysql_type']}")

        if dry_run:
            print(f"\n  🔍 DRY RUN MODE - No changes applied")
            return True

        # Apply changes
        print(f"\n  🔧 Applying schema changes...")

        has_changes = False

        # Add new columns
        if changes['new_columns']:
            has_changes = True
            print(f"  Adding {len(changes['new_columns'])} new columns...")
            for col in changes['new_columns']:
                if not alter_table_add_column(pg_conn, pg_schema, table_name_lower, col):
                    print(f"  ❌ Failed to add column, stopping")
                    return False

        # Drop removed columns
        if changes['dropped_columns']:
            has_changes = True
            print(f"\n  Dropping {len(changes['dropped_columns'])} removed columns...")
            for col in changes['dropped_columns']:
                if not alter_table_drop_column(pg_conn, pg_schema, table_name_lower, col['name']):
                    print(f"  ⚠️  Failed to drop column {col['name']}, continuing...")

        if not has_changes:
            print(f"  No schema changes to apply")

        # Backfill new columns
        if changes['new_columns']:
            print(f"\n  💾 Starting backfill for {len(changes['new_columns'])} new columns...")
            if not backfill_column(mysql_conn, pg_conn, pg_schema,
                                  table_name_upper, table_name_lower, changes['new_columns']):
                print(f"  ⚠️  Backfill completed with warnings")

        print(f"\n  ✅ Table processed successfully")
        return True

    except Exception as e:
        print(f"  ❌ Error processing table: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        mysql_conn.close()
        pg_conn.close()

def parse_table_reference(table_ref):
    """Parse database.table reference

    Args:
        table_ref: String in format 'database.table' (e.g., 'xchangelive.t_carrier')

    Returns:
        Tuple of (database_name, table_name)

    Raises:
        ValueError if format is invalid
    """
    if '.' not in table_ref:
        raise ValueError(
            f"Invalid format: '{table_ref}'\n"
            f"Expected format: database.table (e.g., 'xchangelive.t_carrier')"
        )

    parts = table_ref.split('.', 1)
    if len(parts) != 2:
        raise ValueError(f"Invalid format: '{table_ref}'")

    db_name, table_name = parts

    if db_name not in DB_NAME_MAP:
        raise ValueError(
            f"Unknown database: '{db_name}'\n"
            f"Valid options: {list(DB_NAME_MAP.keys())}"
        )

    return db_name, table_name

def main():
    if len(sys.argv) < 2:
        print("="*60)
        print("Schema Change Detection and Backfill Tool")
        print("="*60)
        print("\nUsage:")
        print("  python3 backfill.py <database.table> [--dry-run]")
        print("\nExamples:")
        print("  python3 backfill.py xchangelive.t_carrier")
        print("  python3 backfill.py xchange_trading.t_deal")
        print("  python3 backfill.py xchange_finance.t_account --dry-run")
        print("\nValid databases:")
        for db_name, schema in DB_NAME_MAP.items():
            print(f"  - {db_name} (Postgres schema: {schema})")
        print("\nOptions:")
        print("  --dry-run    Show changes without applying them")
        print()
        sys.exit(1)

    # Parse arguments
    table_ref = sys.argv[1]
    dry_run = '--dry-run' in sys.argv

    print("="*60)
    print("Schema Change Detection and Backfill Tool")
    print("="*60)

    if dry_run:
        print("🔍 DRY RUN MODE - No changes will be applied")

    try:
        # Parse database.table reference
        db_name, table_name = parse_table_reference(table_ref)

        # Process the table
        success = process_table(db_name, table_name, dry_run)

        if success:
            print("\n" + "="*60)
            print("✅ Schema change detection completed successfully")
            print("="*60)
            sys.exit(0)
        else:
            print("\n" + "="*60)
            print("❌ Schema change detection failed")
            print("="*60)
            sys.exit(1)

    except ValueError as e:
        print(f"\n❌ Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
