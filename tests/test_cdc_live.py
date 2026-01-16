#!/usr/bin/env python3
"""
CDC Live Replication Test

Tests that CDC is actively capturing and replicating changes in real-time.

This is different from validation.py which checks if data CURRENTLY matches.
This test MAKES A CHANGE and verifies CDC propagates it.

Usage:
    python tests/test_cdc_live.py --source trading --table T_ABSTRACT_OFFER
    python tests/test_cdc_live.py --source finance --table T_INVOICE
"""

import os
import sys
import argparse
import time
from datetime import datetime
from typing import Dict, Any, Optional
import pymysql
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# ANSI colors for output
class Color:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def get_mariadb_connection(source: str):
    """Connect to MariaDB source"""
    source_upper = source.upper()
    return pymysql.connect(
        host=os.getenv(f'{source_upper}_HOST'),
        port=int(os.getenv(f'{source_upper}_PORT', 3306)),
        database=os.getenv(f'{source_upper}_DB'),
        user=os.getenv(f'{source_upper}_USER'),
        password=os.getenv(f'{source_upper}_PASS'),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )


def get_postgres_connection():
    """Connect to PostgreSQL sink"""
    return psycopg2.connect(
        host=os.getenv('PG_HOST', 'localhost'),
        port=int(os.getenv('PG_PORT', 5432)),
        database=os.getenv('PG_DB', 'cdc_pipeline'),
        user=os.getenv('PG_USER', 'postgres'),
        password=os.getenv('PG_PASS')
    )


def get_primary_key(maria_conn, source_db: str, table: str) -> list:
    """Get primary key columns for table"""
    with maria_conn.cursor() as cur:
        cur.execute(f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """, (source_db, table))
        return [row['COLUMN_NAME'] for row in cur.fetchall()]


def get_test_column(maria_conn, source_db: str, table: str) -> Optional[tuple]:
    """Find a safe column to use for testing CDC

    Returns: (column_name, column_type) or None

    Priority:
    1. Comment/remarks columns (safest - won't break business logic)
    2. Timestamp columns (last resort)
    """
    with maria_conn.cursor() as cur:
        cur.execute(f"""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """, (source_db, table))
        columns = [(row['COLUMN_NAME'], row['DATA_TYPE'], row['IS_NULLABLE'])
                   for row in cur.fetchall()]

    # Priority 1: Look for comment/remarks fields (case-insensitive)
    for col_name, col_type, nullable in columns:
        if any(keyword in col_name.lower() for keyword in ['comment', 'remark', 'note']):
            if nullable == 'YES' or 'varchar' in col_type.lower() or 'text' in col_type.lower():
                return (col_name, 'string')

    # Priority 2: Look for timestamp columns (case-insensitive)
    timestamp_candidates = ['last_modified_date', 'updated_at', 'modified_date', 'last_updated']
    for col_name, col_type, nullable in columns:
        if col_name.lower() in timestamp_candidates:
            if 'time' in col_type.lower() or 'date' in col_type.lower():
                return (col_name, 'timestamp')

    return None


def get_random_row_id(maria_conn, source_db: str, table: str, pk_cols: list) -> Dict[str, Any]:
    """Get a random row's primary key"""
    pk_str = ', '.join([f'`{col}`' for col in pk_cols])

    with maria_conn.cursor() as cur:
        cur.execute(f"""
            SELECT {pk_str}
            FROM `{source_db}`.`{table}`
            ORDER BY RAND()
            LIMIT 1
        """)
        return cur.fetchone()


def read_row_from_source(maria_conn, source_db: str, table: str, pk_cols: list, pk_values: Dict) -> Dict:
    """Read row from MariaDB"""
    where_clause = ' AND '.join([f"`{col}` = %s" for col in pk_cols])
    values = [pk_values[col] for col in pk_cols]

    with maria_conn.cursor() as cur:
        cur.execute(f"""
            SELECT *
            FROM `{source_db}`.`{table}`
            WHERE {where_clause}
        """, values)
        return cur.fetchone()


def read_row_from_sink(pg_conn, schema: str, table: str, pk_cols: list, pk_values: Dict) -> Dict:
    """Read row from PostgreSQL"""
    where_clause = ' AND '.join([f"{col.lower()} = %s" for col in pk_cols])
    values = [pk_values[col] for col in pk_cols]

    with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT *
            FROM {schema}.{table.lower()}
            WHERE {where_clause}
        """, values)
        return cur.fetchone()


def update_test_column(maria_conn, source_db: str, table: str, pk_cols: list, pk_values: Dict,
                       test_col: str, col_type: str) -> Any:
    """Update test column in MariaDB

    Args:
        test_col: Column name to update
        col_type: 'string' or 'timestamp'

    Returns: The new value that was set
    """
    where_clause = ' AND '.join([f"`{col}` = %s" for col in pk_cols])
    pk_where_values = [pk_values[col] for col in pk_cols]

    if col_type == 'string':
        new_value = f'CDC_TEST_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
        values = [new_value] + pk_where_values
    else:  # timestamp
        new_value = datetime.now()
        values = [new_value] + pk_where_values

    with maria_conn.cursor() as cur:
        cur.execute(f"""
            UPDATE `{source_db}`.`{table}`
            SET `{test_col}` = %s
            WHERE {where_clause}
        """, values)
        maria_conn.commit()

    return new_value


def print_header(text: str):
    """Print formatted header"""
    print(f"\n{Color.HEADER}{Color.BOLD}{'='*70}{Color.ENDC}")
    print(f"{Color.HEADER}{Color.BOLD}{text}{Color.ENDC}")
    print(f"{Color.HEADER}{Color.BOLD}{'='*70}{Color.ENDC}\n")


def print_step(step: int, total: int, text: str):
    """Print step header"""
    print(f"{Color.OKBLUE}[{step}/{total}] {text}{Color.ENDC}")


def print_success(text: str):
    """Print success message"""
    print(f"{Color.OKGREEN}✅ {text}{Color.ENDC}")


def print_error(text: str):
    """Print error message"""
    print(f"{Color.FAIL}❌ {text}{Color.ENDC}")


def print_warning(text: str):
    """Print warning message"""
    print(f"{Color.WARNING}⚠️  {text}{Color.ENDC}")


def test_cdc_live(source: str, table: str, wait_seconds: int = 10):
    """
    Test live CDC replication by making a change and verifying it propagates.

    Args:
        source: Source database (trading/finance/live)
        table: Table name to test
        wait_seconds: Seconds to wait for CDC propagation
    """

    print_header(f"CDC Live Replication Test: {table}")

    source_db = os.getenv(f'{source.upper()}_DB')
    sink_schema = f'xchange_{source.lower()}'

    # Connect to databases
    print_step(1, 6, "Connecting to databases...")
    maria_conn = get_mariadb_connection(source)
    pg_conn = get_postgres_connection()
    print_success(f"Connected to {source} (MariaDB) and PostgreSQL")

    try:
        # Get table metadata
        print_step(2, 6, "Discovering table metadata...")
        pk_cols = get_primary_key(maria_conn, source_db, table)
        test_col_info = get_test_column(maria_conn, source_db, table)

        if not pk_cols:
            print_error(f"No primary key found for {table}")
            return False

        if not test_col_info:
            print_error(f"No suitable test column found for {table}")
            print_warning("Need either a comment/remarks field or timestamp column")
            return False

        test_col, col_type = test_col_info
        print_success(f"Primary key: {', '.join(pk_cols)}")
        print_success(f"Test column: {test_col} ({col_type})")

        # Get random row
        print_step(3, 6, "Selecting random test row...")
        pk_values = get_random_row_id(maria_conn, source_db, table, pk_cols)

        if not pk_values:
            print_error("No rows available for testing")
            return False

        pk_display = ', '.join([f"{k}={v}" for k, v in pk_values.items()])
        print_success(f"Test row: {pk_display}")

        # Read initial state
        print_step(4, 6, "Reading current state from both databases...")
        source_row_before = read_row_from_source(maria_conn, source_db, table, pk_cols, pk_values)
        sink_row_before = read_row_from_sink(pg_conn, sink_schema, table, pk_cols, pk_values)

        if not sink_row_before:
            print_error("Row does not exist in PostgreSQL sink")
            return False

        before_val_source = source_row_before.get(test_col)
        before_val_sink = sink_row_before.get(test_col.lower())
        print(f"   MariaDB {test_col}: {before_val_source}")
        print(f"   PostgreSQL {test_col.lower()}: {before_val_sink}")

        # Make change
        print_step(5, 6, f"Making change in MariaDB (updating {test_col})...")
        new_value = update_test_column(maria_conn, source_db, table, pk_cols, pk_values, test_col, col_type)
        print_success(f"Updated {test_col} to: {new_value}")

        # Wait for CDC
        print_step(6, 6, f"Waiting {wait_seconds} seconds for CDC propagation...")
        for i in range(wait_seconds, 0, -1):
            print(f"   {i}...", end='\r', flush=True)
            time.sleep(1)
        print()

        # Verify change
        print_header("Verification Results")

        source_row_after = read_row_from_source(maria_conn, source_db, table, pk_cols, pk_values)
        sink_row_after = read_row_from_sink(pg_conn, sink_schema, table, pk_cols, pk_values)

        source_val_after = source_row_after[test_col]
        sink_val_after = sink_row_after[test_col.lower()]

        # Normalize timestamps for comparison
        if hasattr(source_val_after, 'tzinfo') and source_val_after.tzinfo is not None:
            source_val_after = source_val_after.replace(tzinfo=None)
        if hasattr(sink_val_after, 'tzinfo') and sink_val_after.tzinfo is not None:
            sink_val_after = sink_val_after.replace(tzinfo=None)

        print(f"Source (MariaDB)  {test_col}: {source_val_after}")
        print(f"Sink (PostgreSQL) {test_col.lower()}: {sink_val_after}")
        print()

        # Compare
        if source_val_after == sink_val_after:
            print_success("CDC WORKING CORRECTLY - Values match!")
            print_success("Change successfully propagated from MariaDB to PostgreSQL")
            return True
        else:
            print_error("CDC NOT WORKING - Values do not match")
            print_warning(f"Expected: {source_val_after}")
            print_warning(f"Got:      {sink_val_after}")
            return False

    finally:
        maria_conn.close()
        pg_conn.close()
        print("\n" + Color.OKCYAN + "Database connections closed" + Color.ENDC)


def main():
    parser = argparse.ArgumentParser(description='Test CDC live replication')
    parser.add_argument('--source', required=True, choices=['trading', 'finance', 'live'],
                        help='Source database')
    parser.add_argument('--table', required=True,
                        help='Table name to test')
    parser.add_argument('--wait', type=int, default=10,
                        help='Seconds to wait for CDC propagation (default: 10)')

    args = parser.parse_args()

    success = test_cdc_live(args.source, args.table, args.wait)
    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
