#!/usr/bin/env python3
"""
CDC Drift Analysis Tool

Investigates root causes of drift by comparing source and sink data:
- Identifies which specific PKs are duplicated
- Checks if orphan rows are old (pre-migration) or new (post-migration)
- Analyzes timestamp patterns to detect replay windows
- Identifies which connector events caused duplicates

Usage:
    python drift_analysis.py --database trading --table T_ABSTRACT_OFFER_ATTACHMENT
    python drift_analysis.py --database trading --table T_ABSTRACT_OFFER_ATTACHMENT --show-duplicates
"""

import os
import sys
import argparse
import logging
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
from collections import defaultdict
import pymysql
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration
# ============================================================================

DATABASE_CONFIGS = {
    'trading': {'schema': 'xchange_trading', 'prefix': 'trading_'},
    'finance': {'schema': 'xchange_finance', 'prefix': 'finance_'},
    'live': {'schema': 'xchangelive', 'prefix': 'live_'},
    'chat': {'schema': 'xchange_chat', 'prefix': 'chat_'},
    'performance': {'schema': 'xchange_company_performance', 'prefix': 'performance_'},
    'concontrol': {'schema': 'xchange_concontrol', 'prefix': 'concontrol_'},
    'claim': {'schema': 'xchange_claim', 'prefix': 'claim_'},
    'payment': {'schema': 'xchange_payment', 'prefix': 'payment_'},
}


@dataclass
class DriftAnalysis:
    """Results of drift analysis"""
    table_name: str
    database: str
    source_count: int
    sink_count: int
    distinct_sink_pks: int
    duplicate_count: int
    orphan_count: int
    orphan_pks: List[Tuple]
    duplicate_pks: List[Tuple]
    duplicate_details: List[Dict[str, Any]]
    timestamp_analysis: Optional[Dict[str, Any]]


# ============================================================================
# Database Connections
# ============================================================================

class DatabaseConfig:
    @staticmethod
    def get_mariadb_config(database: str) -> Dict[str, Any]:
        db_upper = database.upper()
        return {
            'host': os.getenv(f'{db_upper}_HOST'),
            'port': int(os.getenv(f'{db_upper}_PORT', 3306)),
            'database': os.getenv(f'{db_upper}_DB'),
            'user': os.getenv(f'{db_upper}_USER'),
            'password': os.getenv(f'{db_upper}_PASS'),
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }

    @staticmethod
    def get_postgres_config() -> Dict[str, Any]:
        return {
            'host': os.getenv('SINK_DB_HOST', os.getenv('PG_HOST', 'localhost')),
            'port': int(os.getenv('SINK_DB_PORT', os.getenv('PG_PORT', 5432))),
            'database': os.getenv('SINK_DB_NAME', os.getenv('PG_DB', 'postgres')),
            'user': os.getenv('SINK_DB_USER', os.getenv('PG_USER', 'postgres')),
            'password': os.getenv('SINK_DB_PASSWORD', os.getenv('PG_PASS'))
        }


# ============================================================================
# Metadata Discovery
# ============================================================================

def get_primary_key_columns(maria_conn, source_schema: str, table_name: str) -> List[str]:
    """Get primary key columns from MariaDB"""
    with maria_conn.cursor() as cur:
        cur.execute("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
              AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY ORDINAL_POSITION
        """, (source_schema, table_name))
        pk_rows = cur.fetchall()
        return [row['COLUMN_NAME'] for row in pk_rows]


def get_timestamp_column(maria_conn, source_schema: str, table_name: str) -> Optional[str]:
    """Find timestamp column for temporal analysis"""
    with maria_conn.cursor() as cur:
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """, (source_schema, table_name))
        columns = cur.fetchall()

    # Look for timestamp columns
    timestamp_patterns = ['LAST_MODIFIED_DATE', 'MODIFIED_DATE', 'UPDATED_AT', 'CREATED_DATE', 'CREATED_AT']
    for pattern in timestamp_patterns:
        for col in columns:
            if col['COLUMN_NAME'].upper() == pattern:
                return col['COLUMN_NAME']
    return None


# ============================================================================
# Drift Analysis Functions
# ============================================================================

def analyze_duplicates(pg_conn, sink_schema: str, sink_table: str, pk_cols: List[str]) -> Tuple[int, List[Tuple], List[Dict]]:
    """
    Find duplicate PKs in sink and their details
    Returns: (duplicate_count, duplicate_pks, duplicate_details)
    """
    pk_str = ', '.join([col.lower() for col in pk_cols])

    with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
        # Find PKs that appear more than once
        cur.execute(f"""
            SELECT {pk_str}, COUNT(*) as occurrence_count
            FROM {sink_schema}.{sink_table}
            GROUP BY {pk_str}
            HAVING COUNT(*) > 1
            ORDER BY occurrence_count DESC
            LIMIT 100
        """)
        duplicate_groups = cur.fetchall()

    duplicate_pks = []
    duplicate_details = []
    total_duplicate_rows = 0

    for group in duplicate_groups:
        pk_tuple = tuple(group[col.lower()] for col in pk_cols)
        occurrence_count = group['occurrence_count']
        duplicate_pks.append(pk_tuple)
        total_duplicate_rows += (occurrence_count - 1)  # Subtract 1 for the "legitimate" row

        # Get all rows for this PK to analyze differences
        where_conditions = ' AND '.join([f"{col.lower()} = %s" for col in pk_cols])
        with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(f"""
                SELECT *
                FROM {sink_schema}.{sink_table}
                WHERE {where_conditions}
            """, pk_tuple)
            rows = cur.fetchall()

        duplicate_details.append({
            'pk': pk_tuple,
            'count': occurrence_count,
            'rows': rows
        })

    return total_duplicate_rows, duplicate_pks, duplicate_details


def analyze_orphans(maria_conn, pg_conn, source_schema: str, sink_schema: str,
                   source_table: str, sink_table: str, pk_cols: List[str]) -> Tuple[int, List[Tuple]]:
    """
    Find rows in sink that don't exist in source (orphans)
    Returns: (orphan_count, orphan_pks)
    """
    # Get all source PKs
    pk_select_source = ', '.join([f'`{col}`' for col in pk_cols])
    with maria_conn.cursor() as cur:
        cur.execute(f"""
            SELECT {pk_select_source}
            FROM `{source_schema}`.`{source_table}`
        """)
        source_pks = cur.fetchall()

    source_pk_set = set()
    for row in source_pks:
        pk_tuple = tuple(row[col] for col in pk_cols)
        source_pk_set.add(pk_tuple)

    # Get all sink PKs
    pk_select_sink = ', '.join([col.lower() for col in pk_cols])
    with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT DISTINCT {pk_select_sink}
            FROM {sink_schema}.{sink_table}
        """)
        sink_pks = cur.fetchall()

    orphan_pks = []
    for row in sink_pks:
        pk_tuple = tuple(row[col.lower()] for col in pk_cols)
        if pk_tuple not in source_pk_set:
            orphan_pks.append(pk_tuple)

    return len(orphan_pks), orphan_pks[:100]  # Limit to first 100


def analyze_timestamps(maria_conn, pg_conn, source_schema: str, sink_schema: str,
                      source_table: str, sink_table: str, timestamp_col: str) -> Dict[str, Any]:
    """
    Analyze timestamp patterns to detect replay windows
    """
    if not timestamp_col:
        return None

    # Get source timestamp range
    with maria_conn.cursor() as cur:
        cur.execute(f"""
            SELECT
                MIN(`{timestamp_col}`) as min_ts,
                MAX(`{timestamp_col}`) as max_ts,
                COUNT(*) as total_count
            FROM `{source_schema}`.`{source_table}`
        """)
        source_stats = cur.fetchone()

    # Get sink timestamp range
    with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(f"""
            SELECT
                MIN({timestamp_col.lower()}) as min_ts,
                MAX({timestamp_col.lower()}) as max_ts,
                COUNT(*) as total_count
            FROM {sink_schema}.{sink_table}
        """)
        sink_stats = cur.fetchone()

    # Normalize timezones for comparison
    source_min = source_stats['min_ts']
    source_max = source_stats['max_ts']
    sink_min = sink_stats['min_ts']
    sink_max = sink_stats['max_ts']

    # Strip timezone info if present
    if source_min and hasattr(source_min, 'tzinfo') and source_min.tzinfo:
        source_min = source_min.replace(tzinfo=None)
    if source_max and hasattr(source_max, 'tzinfo') and source_max.tzinfo:
        source_max = source_max.replace(tzinfo=None)
    if sink_min and hasattr(sink_min, 'tzinfo') and sink_min.tzinfo:
        sink_min = sink_min.replace(tzinfo=None)
    if sink_max and hasattr(sink_max, 'tzinfo') and sink_max.tzinfo:
        sink_max = sink_max.replace(tzinfo=None)

    return {
        'source_min': source_min,
        'source_max': source_max,
        'source_count': source_stats['total_count'],
        'sink_min': sink_min,
        'sink_max': sink_max,
        'sink_count': sink_stats['total_count']
    }


def run_drift_analysis(database: str, table_name: str, show_duplicates: bool = False) -> DriftAnalysis:
    """
    Run comprehensive drift analysis
    """
    source_schema = DATABASE_CONFIGS[database]['schema']
    sink_schema = 'mp_cdc'
    sink_table = f"{DATABASE_CONFIGS[database]['prefix']}{table_name.lower()}"

    logger.info(f"Analyzing drift: {database}.{table_name}")

    # Connect to databases
    maria_config = DatabaseConfig.get_mariadb_config(database)
    pg_config = DatabaseConfig.get_postgres_config()

    maria_conn = pymysql.connect(**maria_config)
    pg_conn = psycopg2.connect(**pg_config)

    try:
        # Get metadata
        pk_cols = get_primary_key_columns(maria_conn, source_schema, table_name)
        if not pk_cols:
            raise ValueError(f"No primary key found for {table_name}")

        timestamp_col = get_timestamp_column(maria_conn, source_schema, table_name)

        # Get basic counts
        with maria_conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) as cnt FROM `{source_schema}`.`{table_name}`")
            source_count = cur.fetchone()['cnt']

        with pg_conn.cursor(cursor_factory=RealDictCursor) as cur:
            pk_str = ', '.join([col.lower() for col in pk_cols])
            cur.execute(f"""
                SELECT
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT({pk_str})) as distinct_pks
                FROM {sink_schema}.{sink_table}
            """)
            result = cur.fetchone()
            sink_count = result['total_rows']
            distinct_sink_pks = result['distinct_pks']

        # Analyze duplicates
        logger.info("Analyzing duplicates...")
        duplicate_count, duplicate_pks, duplicate_details = analyze_duplicates(
            pg_conn, sink_schema, sink_table, pk_cols
        )

        # Analyze orphans
        logger.info("Analyzing orphans...")
        orphan_count, orphan_pks = analyze_orphans(
            maria_conn, pg_conn, source_schema, sink_schema,
            table_name, sink_table, pk_cols
        )

        # Analyze timestamps
        timestamp_analysis = None
        if timestamp_col:
            logger.info("Analyzing timestamps...")
            timestamp_analysis = analyze_timestamps(
                maria_conn, pg_conn, source_schema, sink_schema,
                table_name, sink_table, timestamp_col
            )

        return DriftAnalysis(
            table_name=table_name,
            database=database,
            source_count=source_count,
            sink_count=sink_count,
            distinct_sink_pks=distinct_sink_pks,
            duplicate_count=duplicate_count,
            orphan_count=orphan_count,
            orphan_pks=orphan_pks,
            duplicate_pks=duplicate_pks,
            duplicate_details=duplicate_details if show_duplicates else [],
            timestamp_analysis=timestamp_analysis
        )

    finally:
        maria_conn.close()
        pg_conn.close()


# ============================================================================
# Reporting
# ============================================================================

def print_analysis(analysis: DriftAnalysis, show_duplicates: bool = False):
    """Print drift analysis results"""

    print(f"\n{'='*80}")
    print(f"DRIFT ANALYSIS: {analysis.database}.{analysis.table_name}")
    print(f"{'='*80}\n")

    # Basic counts
    print("ROW COUNTS:")
    print(f"  Source: {analysis.source_count:,}")
    print(f"  Sink (total): {analysis.sink_count:,}")
    print(f"  Sink (distinct PKs): {analysis.distinct_sink_pks:,}")
    print(f"  Difference: {analysis.sink_count - analysis.source_count:,} excess rows in sink")
    print()

    # Duplicates
    if analysis.duplicate_count > 0:
        dup_ratio = (analysis.duplicate_count / analysis.sink_count * 100)
        print(f"DUPLICATES: {analysis.duplicate_count:,} duplicate rows ({dup_ratio:.2f}%)")
        print(f"  Affected PKs: {len(analysis.duplicate_pks):,}")
        print(f"  Sample duplicate PKs (first 10):")
        for pk in analysis.duplicate_pks[:10]:
            print(f"    {pk}")

        if show_duplicates and analysis.duplicate_details:
            print(f"\n  DETAILED DUPLICATE ANALYSIS:")
            for detail in analysis.duplicate_details[:5]:  # Show first 5
                print(f"\n    PK: {detail['pk']} (appears {detail['count']} times)")
                for i, row in enumerate(detail['rows'], 1):
                    print(f"      Row {i}: {dict(row)}")
        print()

    # Orphans
    if analysis.orphan_count > 0:
        orphan_ratio = (analysis.orphan_count / analysis.distinct_sink_pks * 100)
        print(f"ORPHANS: {analysis.orphan_count:,} rows in sink not in source ({orphan_ratio:.2f}%)")
        print(f"  Sample orphan PKs (first 10):")
        for pk in analysis.orphan_pks[:10]:
            print(f"    {pk}")
        print()

    # Timestamp analysis
    if analysis.timestamp_analysis:
        ts = analysis.timestamp_analysis
        print("TIMESTAMP ANALYSIS:")
        print(f"  Source range: {ts['source_min']} → {ts['source_max']}")
        print(f"  Sink range:   {ts['sink_min']} → {ts['sink_max']}")

        # Check if sink has older data
        if ts['sink_min'] < ts['source_min']:
            print(f"  ⚠️  Sink has OLDER data than source (pre-migration orphans?)")

        # Check if sink has newer data
        if ts['sink_max'] > ts['source_max']:
            print(f"  ⚠️  Sink has NEWER data than source (impossible under CDC)")
        print()

    # Root cause analysis
    print("ROOT CAUSE ANALYSIS:")

    if analysis.duplicate_count > 0:
        print("  ✗ Duplicates detected → Likely snapshot + streaming replay overlap")
        print("    Possible causes:")
        print("      - Source connector restarted with snapshot.mode != 'schema_only'")
        print("      - Binlog position lost/expired, forcing re-snapshot")
        print("      - Migration script ran while connector was streaming")

    if analysis.orphan_count > 0:
        if analysis.timestamp_analysis and analysis.timestamp_analysis['sink_min'] < analysis.timestamp_analysis['source_min']:
            print("  ✗ Orphans are OLDER than source data → Pre-migration artifacts")
            print("    Possible causes:")
            print("      - Source data was deleted after migration")
            print("      - Migration captured data that was subsequently purged")
        else:
            print("  ✗ Orphans detected → Data in sink not in source")
            print("    Possible causes:")
            print("      - Source data was deleted (soft/hard delete)")
            print("      - Sink has stale data from previous connector runs")

    if analysis.duplicate_count == 0 and analysis.orphan_count == 0:
        print("  ✓ No duplicates or orphans detected")
        print("    If row counts still differ, check for:")
        print("      - In-flight transactions during snapshot")
        print("      - Race conditions in concurrent writes")

    print(f"\n{'='*80}\n")


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Analyze CDC drift root causes',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python drift_analysis.py --database trading --table T_ABSTRACT_OFFER_ATTACHMENT
  python drift_analysis.py --database trading --table T_INVOICE --show-duplicates
        """
    )
    parser.add_argument('--database', required=True, choices=list(DATABASE_CONFIGS.keys()),
                       help='Database name')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--show-duplicates', action='store_true',
                       help='Show detailed duplicate row analysis')

    args = parser.parse_args()

    try:
        analysis = run_drift_analysis(args.database, args.table, args.show_duplicates)
        print_analysis(analysis, args.show_duplicates)
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
