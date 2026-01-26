#!/usr/bin/env python3
"""
CDC Pipeline Validation Framework

Validates CDC correctness between MariaDB sources and PostgreSQL sink.
Designed to detect snapshot+streaming replay, duplication, and drift.

CDC Principles:
- At-least-once delivery (not exactly-once)
- Any row count difference is suspicious
- Sink PK uniqueness is necessary but not sufficient
- Must compare source vs sink keys, not just sink internals

Usage:
    python validation.py --database trading
    python validation.py --database finance --tables T_INVOICE,T_ACCOUNT
    python validation.py --all
"""

import os
import sys
import argparse
import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any
from enum import Enum
from datetime import datetime, timedelta
import pymysql
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import ssl
import certifi

# ============================================================================
# Configuration & Constants
# ============================================================================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ValidationStatus(Enum):
    """Validation check status"""
    PASS = "✓ PASS"
    WARN = "⚠ WARN"
    FAIL = "✗ FAIL"
    SKIP = "⊘ SKIP"


class TableType(Enum):
    """Table classification based on mutation patterns"""
    STATIC = "static"
    APPEND_ONLY = "append_only"
    MUTABLE = "mutable"
    HOT = "hot"


@dataclass
class TableMetadata:
    """Table-level metadata for validation"""
    name: str
    primary_key: List[str]
    timestamp_column: Optional[str] = None
    table_type: TableType = TableType.MUTABLE
    freshness_threshold_minutes: int = 10
    row_count_warn_threshold_percent: float = 0.1  # 0.1% = small drift warning
    row_count_fail_threshold_percent: float = 1.0  # 1% = major drift failure
    sample_size: int = 50
    bit_columns: List[str] = field(default_factory=list)


@dataclass
class CheckResult:
    """Result of a single validation check"""
    check_name: str
    status: ValidationStatus
    metrics: Dict[str, Any]
    explanation: str
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class TableValidationReport:
    """Complete validation report for a table"""
    table_name: str
    database: str
    source_schema: str
    sink_schema: str
    sink_table: str
    timestamp: datetime
    checks: List[CheckResult]
    overall_status: ValidationStatus
    summary: str
    cdc_consistent: bool  # True only if no correctness violations


# ============================================================================
# Database Connection Managers
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


class DatabaseConfig:
    """Database connection configuration"""

    @staticmethod
    def get_mariadb_config(database: str) -> Dict[str, Any]:
        """Get MariaDB connection config for database"""
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
        """Get PostgreSQL connection config"""
        return {
            'host': os.getenv('SINK_DB_HOST', os.getenv('PG_HOST', 'localhost')),
            'port': int(os.getenv('SINK_DB_PORT', os.getenv('PG_PORT', 5432))),
            'database': os.getenv('SINK_DB_NAME', os.getenv('PG_DB', 'postgres')),
            'user': os.getenv('SINK_DB_USER', os.getenv('PG_USER', 'postgres')),
            'password': os.getenv('SINK_DB_PASSWORD', os.getenv('PG_PASS'))
        }


class DatabaseConnection:
    """Manages database connections"""

    def __init__(self, database: str):
        self.database = database
        self.mariadb_conn = None
        self.postgres_conn = None

    def __enter__(self):
        """Connect to both databases"""
        try:
            maria_config = DatabaseConfig.get_mariadb_config(self.database)
            self.mariadb_conn = pymysql.connect(**maria_config)

            pg_config = DatabaseConfig.get_postgres_config()
            self.postgres_conn = psycopg2.connect(**pg_config)

            logger.info(f"Connected to {self.database} (MariaDB) and PostgreSQL")
            return self

        except Exception as e:
            logger.error(f"Failed to establish connections: {e}")
            self.__exit__(None, None, None)
            raise

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close database connections"""
        if self.mariadb_conn:
            self.mariadb_conn.close()
        if self.postgres_conn:
            self.postgres_conn.close()

    def get_source_cursor(self):
        """Get MariaDB cursor"""
        return self.mariadb_conn.cursor()

    def get_sink_cursor(self):
        """Get PostgreSQL cursor"""
        return self.postgres_conn.cursor(cursor_factory=RealDictCursor)


# ============================================================================
# Table Discovery
# ============================================================================

def get_tables_from_env(database: str) -> List[str]:
    """Extract table list from environment variable allowlist"""
    allowlist_var = f'{database.upper()}_TABLE_ALLOWLIST'
    allowlist = os.getenv(allowlist_var, '')

    if not allowlist:
        logger.warning(f"No allowlist found for {database} (${allowlist_var})")
        return []

    tables = []
    for item in allowlist.split(','):
        item = item.strip()
        if '.' in item:
            table = item.split('.')[1]
        else:
            table = item
        tables.append(table)

    return tables


def discover_table_metadata(db_conn: DatabaseConnection, database: str, table_name: str) -> Optional[TableMetadata]:
    """Auto-discover table metadata from database schema"""
    try:
        source_schema = DATABASE_CONFIGS[database]['schema']

        # Get primary key columns
        with db_conn.get_source_cursor() as cur:
            cur.execute("""
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                  AND CONSTRAINT_NAME = 'PRIMARY'
                ORDER BY ORDINAL_POSITION
            """, (source_schema, table_name))
            pk_rows = cur.fetchall()
            primary_keys = [row['COLUMN_NAME'] for row in pk_rows]

        # STRICT: Fail if no PK found
        if not primary_keys:
            logger.error(f"No primary key found for {table_name} - CDC validation requires explicit PK")
            return None

        # Get timestamp column and bit columns
        with db_conn.get_source_cursor() as cur:
            cur.execute("""
                SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
            """, (source_schema, table_name))
            columns = cur.fetchall()

        # Find timestamp column
        timestamp_col = None
        timestamp_patterns = ['LAST_MODIFIED_DATE', 'MODIFIED_DATE', 'UPDATED_AT', 'CREATED_DATE', 'CREATED_AT']
        for pattern in timestamp_patterns:
            for col in columns:
                if col['COLUMN_NAME'].upper() == pattern:
                    timestamp_col = col['COLUMN_NAME']
                    break
            if timestamp_col:
                break

        # Find bit(1)/tinyint(1) columns
        bit_columns = []
        for col in columns:
            if col['COLUMN_TYPE'] == 'bit(1)' or col['COLUMN_TYPE'] == 'tinyint(1)':
                bit_columns.append(col['COLUMN_NAME'])

        return TableMetadata(
            name=table_name,
            primary_key=primary_keys,
            timestamp_column=timestamp_col,
            table_type=TableType.MUTABLE,
            freshness_threshold_minutes=15,
            row_count_warn_threshold_percent=0.1,
            row_count_fail_threshold_percent=1.0,
            sample_size=50,
            bit_columns=bit_columns
        )

    except Exception as e:
        logger.error(f"Failed to discover metadata for {table_name}: {e}")
        return None


# ============================================================================
# CDC-Aware Validation Checks
# ============================================================================

class ValidationChecks:
    """CDC correctness validation checks"""

    def __init__(self, db_conn: DatabaseConnection, metadata: TableMetadata,
                 database: str, source_schema: str, sink_schema: str, table_prefix: str):
        self.db = db_conn
        self.meta = metadata
        self.database = database
        self.source_schema = source_schema
        self.sink_schema = sink_schema
        self.table_prefix = table_prefix
        self.sink_table = f"{table_prefix}{metadata.name.lower()}"

    def check_1_table_exists(self) -> CheckResult:
        """Verify table exists in sink"""
        logger.info(f"[Check 1] Verifying table exists: {self.sink_table}")

        try:
            with self.db.get_sink_cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) as cnt
                    FROM information_schema.tables
                    WHERE table_schema = %s AND table_name = %s
                """, (self.sink_schema, self.sink_table))
                exists = cur.fetchone()['cnt'] > 0

            if not exists:
                return CheckResult(
                    check_name="1_table_exists",
                    status=ValidationStatus.FAIL,
                    metrics={},
                    explanation=f"Table {self.sink_schema}.{self.sink_table} does not exist in sink"
                )

            return CheckResult(
                check_name="1_table_exists",
                status=ValidationStatus.PASS,
                metrics={},
                explanation=f"Table exists: {self.sink_schema}.{self.sink_table}"
            )

        except Exception as e:
            return CheckResult(
                check_name="1_table_exists",
                status=ValidationStatus.FAIL,
                metrics={"error": str(e)},
                explanation=f"Error checking table existence: {e}"
            )

    def check_2_row_count_smoke_test(self) -> CheckResult:
        """
        Compare row counts (SMOKE TEST only - not a correctness guarantee)

        CDC Principle: Any non-zero difference is suspicious
        - PASS: counts exactly match
        - WARN: small difference (< 0.1%)
        - FAIL: difference exceeds tolerance
        """
        logger.info(f"[Check 2] Row count smoke test")

        try:
            # Source count
            with self.db.get_source_cursor() as cur:
                cur.execute(f"SELECT COUNT(*) as cnt FROM `{self.source_schema}`.`{self.meta.name}`")
                source_count = cur.fetchone()['cnt']

            # Sink count
            with self.db.get_sink_cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*) as cnt
                    FROM {self.sink_schema}.{self.sink_table}
                """)
                sink_count = cur.fetchone()['cnt']

            diff = abs(source_count - sink_count)
            diff_percent = (diff / source_count * 100) if source_count > 0 else 0

            # STRICT: Any non-zero difference is at least WARN
            if diff == 0:
                status = ValidationStatus.PASS
                explanation = f"Row counts exactly match: {source_count}"
            elif diff_percent < self.meta.row_count_warn_threshold_percent:
                status = ValidationStatus.WARN
                explanation = f"Row count drift: {diff} rows ({diff_percent:.4f}%) - within warn threshold but requires investigation"
            elif diff_percent < self.meta.row_count_fail_threshold_percent:
                status = ValidationStatus.WARN
                explanation = f"Row count drift: {diff} rows ({diff_percent:.2f}%) - approaching failure threshold"
            else:
                status = ValidationStatus.FAIL
                explanation = f"Row count divergence: {diff} rows ({diff_percent:.2f}%) exceeds tolerance - likely CDC replay or duplication"

            return CheckResult(
                check_name="2_row_count_smoke",
                status=status,
                metrics={
                    "source_count": source_count,
                    "sink_count": sink_count,
                    "difference": diff,
                    "difference_percent": round(diff_percent, 4),
                    "excess_rows": sink_count - source_count
                },
                explanation=explanation
            )

        except Exception as e:
            return CheckResult(
                check_name="2_row_count_smoke",
                status=ValidationStatus.FAIL,
                metrics={"error": str(e)},
                explanation=f"Error comparing row counts: {e}"
            )

    def check_3_distinct_pk_cardinality(self) -> CheckResult:
        """
        Compare DISTINCT primary key counts (CDC CORRECTNESS CHECK)

        This is the primary CDC correctness test:
        - Source DISTINCT(PK) == Sink DISTINCT(PK) → OK
        - Source DISTINCT(PK) < Sink DISTINCT(PK) → FAIL (impossible - sink has phantom keys)
        - Source DISTINCT(PK) > Sink DISTINCT(PK) → FAIL (missing data in sink)
        """
        logger.info(f"[Check 3] Distinct PK cardinality (CDC correctness)")

        try:
            pk_cols_source = ', '.join([f'`{col}`' for col in self.meta.primary_key])
            pk_cols_sink = ', '.join([col.lower() for col in self.meta.primary_key])

            # Source distinct PK count
            with self.db.get_source_cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(DISTINCT({pk_cols_source})) as distinct_pks
                    FROM `{self.source_schema}`.`{self.meta.name}`
                """)
                source_distinct = cur.fetchone()['distinct_pks']

            # Sink distinct PK count
            with self.db.get_sink_cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(DISTINCT({pk_cols_sink})) as distinct_pks
                    FROM {self.sink_schema}.{self.sink_table}
                """)
                sink_distinct = cur.fetchone()['distinct_pks']

            diff = sink_distinct - source_distinct
            diff_percent = (abs(diff) / source_distinct * 100) if source_distinct > 0 else 0

            if diff == 0:
                status = ValidationStatus.PASS
                explanation = f"Distinct PK counts match exactly: {source_distinct} keys"
            elif diff > 0:
                status = ValidationStatus.FAIL
                explanation = f"Sink has {diff} MORE distinct PKs than source ({diff_percent:.4f}%) - IMPOSSIBLE under CDC - indicates phantom data or corruption"
            else:  # diff < 0
                status = ValidationStatus.FAIL
                explanation = f"Sink is MISSING {abs(diff)} distinct PKs ({diff_percent:.4f}%) - CDC data loss detected"

            return CheckResult(
                check_name="3_distinct_pk_cardinality",
                status=status,
                metrics={
                    "source_distinct_pks": source_distinct,
                    "sink_distinct_pks": sink_distinct,
                    "difference": diff,
                    "difference_percent": round(diff_percent, 4)
                },
                explanation=explanation
            )

        except Exception as e:
            return CheckResult(
                check_name="3_distinct_pk_cardinality",
                status=ValidationStatus.FAIL,
                metrics={"error": str(e)},
                explanation=f"Error checking distinct PK cardinality: {e}"
            )

    def check_4_sink_duplication_quantification(self) -> CheckResult:
        """
        Quantify sink-side duplication (CDC REPLAY DETECTOR)

        If sink row count > sink distinct PK count:
        - Compute exact duplicate count
        - Compute duplicate ratio
        - This indicates snapshot + streaming replay overlap
        """
        logger.info(f"[Check 4] Sink duplication quantification")

        try:
            pk_cols = [col.lower() for col in self.meta.primary_key]
            pk_str = ', '.join(pk_cols)

            with self.db.get_sink_cursor() as cur:
                # Get total rows and distinct PKs
                cur.execute(f"""
                    SELECT
                        COUNT(*) as total_rows,
                        COUNT(DISTINCT({pk_str})) as distinct_pks
                    FROM {self.sink_schema}.{self.sink_table}
                """)
                result = cur.fetchone()
                total_rows = result['total_rows']
                distinct_pks = result['distinct_pks']

                # Check for NULL PKs
                null_conditions = ' OR '.join([f"{col} IS NULL" for col in pk_cols])
                cur.execute(f"""
                    SELECT COUNT(*) as null_pk_count
                    FROM {self.sink_schema}.{self.sink_table}
                    WHERE ({null_conditions})
                """)
                null_pk_count = cur.fetchone()['null_pk_count']

            duplicate_rows = total_rows - distinct_pks
            duplicate_ratio = (duplicate_rows / total_rows * 100) if total_rows > 0 else 0

            issues = []
            if null_pk_count > 0:
                issues.append(f"{null_pk_count} NULL PKs")

            if duplicate_rows > 0:
                status = ValidationStatus.FAIL
                explanation = f"Sink has {duplicate_rows} duplicate rows ({duplicate_ratio:.4f}% duplication rate) - CDC replay detected (likely snapshot + streaming overlap)"
                issues.append(f"{duplicate_rows} duplicates")
            elif null_pk_count > 0:
                status = ValidationStatus.FAIL
                explanation = f"PK integrity violations: {'; '.join(issues)}"
            else:
                status = ValidationStatus.PASS
                explanation = f"All {total_rows} rows have unique, non-null PKs"

            return CheckResult(
                check_name="4_sink_duplication",
                status=status,
                metrics={
                    "total_rows": total_rows,
                    "distinct_pks": distinct_pks,
                    "duplicate_rows": duplicate_rows,
                    "duplicate_ratio_percent": round(duplicate_ratio, 6),
                    "null_pk_count": null_pk_count
                },
                explanation=explanation
            )

        except Exception as e:
            return CheckResult(
                check_name="4_sink_duplication",
                status=ValidationStatus.FAIL,
                metrics={"error": str(e)},
                explanation=f"Error checking sink duplication: {e}"
            )

    def check_5_source_vs_sink_anti_join(self) -> CheckResult:
        """
        Detect rows in sink that don't exist in source (REPLAY/ORPHAN DETECTOR)

        This catches:
        - Replay artifacts from connector restarts
        - Orphan rows from dirty resets
        - Phantom data

        Uses COUNT-only approach for large tables
        """
        logger.info(f"[Check 5] Source vs Sink anti-join (orphan detection)")

        try:
            pk_cols_source = self.meta.primary_key
            pk_cols_sink = [col.lower() for col in self.meta.primary_key]

            # Create temporary table in PostgreSQL with source PKs
            pk_join_conditions = ' AND '.join([
                f"sink.{pk_cols_sink[i]} = source_pks.{pk_cols_source[i].lower()}"
                for i in range(len(pk_cols_source))
            ])

            # For safety, limit this check to reasonable table sizes
            # For very large tables (>10M rows), this may be too expensive
            with self.db.get_sink_cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*) as cnt
                    FROM {self.sink_schema}.{self.sink_table}
                """)
                sink_count = cur.fetchone()['cnt']

            if sink_count > 10_000_000:
                return CheckResult(
                    check_name="5_anti_join",
                    status=ValidationStatus.SKIP,
                    metrics={"sink_count": sink_count},
                    explanation=f"Table too large ({sink_count:,} rows) - anti-join check skipped for performance"
                )

            # Get source PKs
            pk_select = ', '.join([f'`{col}`' for col in pk_cols_source])
            with self.db.get_source_cursor() as cur:
                cur.execute(f"""
                    SELECT {pk_select}
                    FROM `{self.source_schema}`.`{self.meta.name}`
                """)
                source_pks = cur.fetchall()

            # Create temp dict for fast lookup
            source_pk_set = set()
            for row in source_pks:
                pk_tuple = tuple(row[col] for col in pk_cols_source)
                source_pk_set.add(pk_tuple)

            # Find orphan rows in sink
            with self.db.get_sink_cursor() as cur:
                pk_select_sink = ', '.join(pk_cols_sink)
                cur.execute(f"""
                    SELECT {pk_select_sink}
                    FROM {self.sink_schema}.{self.sink_table}
                """)
                sink_pks = cur.fetchall()

            orphan_count = 0
            for row in sink_pks:
                pk_tuple = tuple(row[col] for col in pk_cols_sink)
                if pk_tuple not in source_pk_set:
                    orphan_count += 1

            if orphan_count > 0:
                orphan_ratio = (orphan_count / sink_count * 100) if sink_count > 0 else 0
                return CheckResult(
                    check_name="5_anti_join",
                    status=ValidationStatus.FAIL,
                    metrics={
                        "orphan_rows": orphan_count,
                        "orphan_ratio_percent": round(orphan_ratio, 6),
                        "sink_count": sink_count
                    },
                    explanation=f"Found {orphan_count} rows in sink ({orphan_ratio:.4f}%) that don't exist in source - indicates replay artifacts or dirty reset"
                )

            return CheckResult(
                check_name="5_anti_join",
                status=ValidationStatus.PASS,
                metrics={
                    "orphan_rows": 0,
                    "sink_count": sink_count
                },
                explanation="No orphan rows detected - all sink PKs exist in source"
            )

        except Exception as e:
            logger.error(f"Anti-join check failed: {e}")
            return CheckResult(
                check_name="5_anti_join",
                status=ValidationStatus.FAIL,
                metrics={"error": str(e)},
                explanation=f"Error checking anti-join: {e}"
            )

    def check_6_freshness(self) -> CheckResult:
        """
        Verify data freshness and CDC lag
        """
        logger.info(f"[Check 6] Checking data freshness")

        if not self.meta.timestamp_column:
            return CheckResult(
                check_name="6_freshness",
                status=ValidationStatus.WARN,
                metrics={},
                explanation="No timestamp column - freshness check skipped"
            )

        try:
            ts_col = self.meta.timestamp_column

            with self.db.get_source_cursor() as cur:
                cur.execute(f"""
                    SELECT MAX(`{ts_col}`) as max_ts
                    FROM `{self.source_schema}`.`{self.meta.name}`
                """)
                source_max = cur.fetchone()['max_ts']

            with self.db.get_sink_cursor() as cur:
                cur.execute(f"""
                    SELECT MAX({ts_col.lower()}) as max_ts
                    FROM {self.sink_schema}.{self.sink_table}
                """)
                sink_max = cur.fetchone()['max_ts']

            if source_max is None or sink_max is None:
                return CheckResult(
                    check_name="6_freshness",
                    status=ValidationStatus.WARN,
                    metrics={"source_max": str(source_max), "sink_max": str(sink_max)},
                    explanation="No timestamp data available"
                )

            # Normalize timezones
            if hasattr(source_max, 'tzinfo') and source_max.tzinfo:
                source_max = source_max.replace(tzinfo=None)
            if hasattr(sink_max, 'tzinfo') and sink_max.tzinfo:
                sink_max = sink_max.replace(tzinfo=None)

            lag = source_max - sink_max if isinstance(source_max, datetime) else timedelta(0)
            lag_minutes = lag.total_seconds() / 60 if isinstance(lag, timedelta) else 0

            threshold = self.meta.freshness_threshold_minutes
            if lag_minutes > threshold:
                status = ValidationStatus.FAIL
                explanation = f"Data lag {lag_minutes:.1f} min exceeds threshold ({threshold} min) - CDC may be stalled"
            elif lag_minutes > (threshold * 0.8):
                status = ValidationStatus.WARN
                explanation = f"Data lag {lag_minutes:.1f} min approaching threshold"
            else:
                status = ValidationStatus.PASS
                explanation = f"Data is fresh: {lag_minutes:.1f} min lag"

            return CheckResult(
                check_name="6_freshness",
                status=status,
                metrics={
                    "source_max_ts": str(source_max),
                    "sink_max_ts": str(sink_max),
                    "lag_minutes": round(lag_minutes, 2),
                    "threshold_minutes": threshold
                },
                explanation=explanation
            )

        except Exception as e:
            return CheckResult(
                check_name="6_freshness",
                status=ValidationStatus.FAIL,
                metrics={"error": str(e)},
                explanation=f"Error checking freshness: {e}"
            )


# ============================================================================
# Validation Orchestrator
# ============================================================================

class TableValidator:
    """Orchestrates CDC validation checks for a table"""

    def __init__(self, db_conn: DatabaseConnection, metadata: TableMetadata, database: str):
        self.db = db_conn
        self.meta = metadata
        self.database = database
        self.source_schema = DATABASE_CONFIGS[database]['schema']
        self.sink_schema = 'mp_cdc'
        self.table_prefix = DATABASE_CONFIGS[database]['prefix']

    def run_all_checks(self) -> TableValidationReport:
        """Run all CDC validation checks"""
        logger.info(f"\n{'='*80}")
        logger.info(f"Validating: {self.database}.{self.meta.name}")
        logger.info(f"{'='*80}")

        checks_runner = ValidationChecks(
            self.db, self.meta, self.database,
            self.source_schema, self.sink_schema, self.table_prefix
        )

        # Run all checks
        checks = [
            checks_runner.check_1_table_exists(),
            checks_runner.check_2_row_count_smoke_test(),
            checks_runner.check_3_distinct_pk_cardinality(),
            checks_runner.check_4_sink_duplication_quantification(),
            checks_runner.check_5_source_vs_sink_anti_join(),
            checks_runner.check_6_freshness(),
        ]

        # Determine overall status and CDC consistency
        has_fail = any(c.status == ValidationStatus.FAIL for c in checks)
        has_warn = any(c.status == ValidationStatus.WARN for c in checks)

        if has_fail:
            overall_status = ValidationStatus.FAIL
            cdc_consistent = False
        elif has_warn:
            overall_status = ValidationStatus.WARN
            cdc_consistent = False  # Warnings also indicate potential CDC issues
        else:
            overall_status = ValidationStatus.PASS
            cdc_consistent = True

        fail_count = sum(1 for c in checks if c.status == ValidationStatus.FAIL)
        warn_count = sum(1 for c in checks if c.status == ValidationStatus.WARN)
        pass_count = sum(1 for c in checks if c.status == ValidationStatus.PASS)
        skip_count = sum(1 for c in checks if c.status == ValidationStatus.SKIP)

        summary = f"{pass_count} PASS, {warn_count} WARN, {fail_count} FAIL, {skip_count} SKIP"

        return TableValidationReport(
            table_name=self.meta.name,
            database=self.database,
            source_schema=self.source_schema,
            sink_schema=self.sink_schema,
            sink_table=f"{self.table_prefix}{self.meta.name.lower()}",
            timestamp=datetime.now(),
            checks=checks,
            overall_status=overall_status,
            summary=summary,
            cdc_consistent=cdc_consistent
        )


# ============================================================================
# Slack Notifications
# ============================================================================

class SlackNotifier:
    """Send validation results to Slack"""

    def __init__(self):
        self.slack_token = os.getenv('SLACK_BOT_TOKEN')
        self.slack_channel = os.getenv('SLACK_CHANNEL_ID', os.getenv('SLACK_CHANNEL', '#cdc-validation'))
        self.slack_mention_user = os.getenv('SLACK_MENTION_USER', '')  # e.g., "<@U12345678>"

        # Create SSL context with certifi CA bundle
        if self.slack_token:
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            self.client = WebClient(token=self.slack_token, ssl=ssl_context)
        else:
            self.client = None

    def send_summary(self, reports: List[TableValidationReport], database: str = None):
        """Send validation summary to Slack"""
        if not self.client:
            logger.warning("Slack token not configured - skipping notification")
            return

        try:
            total = len(reports)
            passed = sum(1 for r in reports if r.overall_status == ValidationStatus.PASS)
            warned = sum(1 for r in reports if r.overall_status == ValidationStatus.WARN)
            failed = sum(1 for r in reports if r.overall_status == ValidationStatus.FAIL)
            cdc_consistent = sum(1 for r in reports if r.cdc_consistent is True)
            cdc_applicable = sum(1 for r in reports if r.cdc_consistent is not None)

            # Determine overall status
            if failed > 0:
                status_emoji = ":x:"
                status_text = "FAILURES DETECTED"
                color = "#FF0000"
            elif warned > 0:
                status_emoji = ":warning:"
                status_text = "WARNINGS DETECTED"
                color = "#FFA500"
            else:
                status_emoji = ":white_check_mark:"
                status_text = "ALL PASSED"
                color = "#00FF00"

            # Build header with optional database name
            if database:
                header_text = f"{status_emoji} CDC Validation Report - {database.upper()} - {status_text}"
            else:
                header_text = f"{status_emoji} CDC Validation Report - {status_text}"

            # Build message
            blocks = [
                {
                    "type": "header",
                    "text": {
                        "type": "plain_text",
                        "text": header_text
                    }
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Total Tables:*\n{total}"},
                        {"type": "mrkdwn", "text": f"*CDC Consistent:*\n{cdc_consistent}/{cdc_applicable}"},
                        {"type": "mrkdwn", "text": f"*Passed:*\n{passed}"},
                        {"type": "mrkdwn", "text": f"*Warned:*\n{warned}"},
                        {"type": "mrkdwn", "text": f"*Failed:*\n{failed}"},
                        {"type": "mrkdwn", "text": f"*Timestamp:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}"}
                    ]
                }
            ]

            # Add failure details if any
            failed_reports = [r for r in reports if r.cdc_consistent is False]
            if failed_reports:
                # Mention user if configured
                mention_text = f"{self.slack_mention_user} " if self.slack_mention_user else ""

                failure_text = f"{mention_text}*Failed Tables ({len(failed_reports)}):*\n\n"

                for report in failed_reports[:10]:  # Limit to first 10 to avoid message size limits
                    failed_checks = [c for c in report.checks if c.status in [ValidationStatus.FAIL, ValidationStatus.WARN]]

                    failure_text += f"• *{report.database}.{report.table_name}* ({report.overall_status.value})\n"

                    for check in failed_checks[:3]:  # Limit to first 3 checks per table
                        metric_summary = []

                        if check.check_name == "2_row_count_smoke":
                            excess = check.metrics.get('excess_rows', 0)
                            diff_pct = check.metrics.get('difference_percent', 0)
                            if excess > 0:
                                metric_summary.append(f"{excess:,} excess rows ({diff_pct}%)")
                            elif excess < 0:
                                metric_summary.append(f"{abs(excess):,} missing rows ({diff_pct}%)")

                        elif check.check_name == "4_sink_duplication":
                            dup_rows = check.metrics.get('duplicate_rows', 0)
                            dup_pct = check.metrics.get('duplicate_ratio_percent', 0)
                            if dup_rows > 0:
                                metric_summary.append(f"{dup_rows:,} duplicates ({dup_pct}%)")

                        elif check.check_name == "5_anti_join":
                            orphans = check.metrics.get('orphan_rows', 0)
                            if orphans > 0:
                                metric_summary.append(f"{orphans:,} orphan rows")

                        summary_str = f" - {', '.join(metric_summary)}" if metric_summary else ""
                        failure_text += f"  ◦ {check.check_name}{summary_str}\n"

                    failure_text += "\n"

                if len(failed_reports) > 10:
                    failure_text += f"_...and {len(failed_reports) - 10} more_\n"

                blocks.append({
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": failure_text}
                })

                # Add common causes
                blocks.append({
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*Common Causes:*\n• Snapshot + streaming replay overlap\n• Connector restart without proper offset management\n• Missing primary key enforcement\n• Dirty database resets"
                    }
                })

            # Send to Slack
            self.client.chat_postMessage(
                channel=self.slack_channel,
                blocks=blocks,
                text=f"CDC Validation Report - {status_text}"  # Fallback text
            )

            logger.info(f"Slack notification sent to {self.slack_channel}")

        except SlackApiError as e:
            logger.error(f"Failed to send Slack notification: {e.response['error']}")
        except Exception as e:
            logger.error(f"Unexpected error sending Slack notification: {e}")


# ============================================================================
# Reporting
# ============================================================================

def print_report(report: TableValidationReport):
    """Print validation report to console"""

    if report.cdc_consistent is None:
        cdc_status = "SKIPPED"
    elif report.cdc_consistent:
        cdc_status = "CDC-CONSISTENT"
    else:
        cdc_status = "CDC-INCONSISTENT"

    print(f"\n{'='*80}")
    print(f"TABLE: {report.database}.{report.table_name} → {report.sink_schema}.{report.sink_table}")
    print(f"Status: {report.overall_status.value} | {cdc_status}")
    print(f"Summary: {report.summary}")
    print(f"{'='*80}\n")

    for check in report.checks:
        print(f"{check.status.value:12} {check.check_name:30} {check.explanation}")

        # Always show metrics for FAIL and WARN
        if check.metrics and check.status in [ValidationStatus.FAIL, ValidationStatus.WARN]:
            for key, val in check.metrics.items():
                if key not in ['error']:
                    # Format large numbers with commas
                    if isinstance(val, int) and val > 999:
                        val_str = f"{val:,}"
                    else:
                        val_str = str(val)
                    print(f"             └─ {key}: {val_str}")

    print()


def print_summary(reports: List[TableValidationReport], database: str = None):
    """Print overall summary with detailed failure breakdown"""
    total = len(reports)
    passed = sum(1 for r in reports if r.overall_status == ValidationStatus.PASS)
    warned = sum(1 for r in reports if r.overall_status == ValidationStatus.WARN)
    failed = sum(1 for r in reports if r.overall_status == ValidationStatus.FAIL)
    skipped = sum(1 for r in reports if r.overall_status == ValidationStatus.SKIP)
    cdc_consistent = sum(1 for r in reports if r.cdc_consistent is True)
    cdc_applicable = sum(1 for r in reports if r.cdc_consistent is not None)

    print(f"\n{'='*80}")
    if database:
        print(f"OVERALL SUMMARY - {database.upper()}")
    else:
        print(f"OVERALL SUMMARY")
    print(f"{'='*80}")
    print(f"Total tables validated: {total}")
    print(f"  {ValidationStatus.PASS.value}: {passed}")
    print(f"  {ValidationStatus.WARN.value}: {warned}")
    print(f"  {ValidationStatus.FAIL.value}: {failed}")
    print(f"  {ValidationStatus.SKIP.value}: {skipped}")
    print(f"\nCDC Consistency: {cdc_consistent}/{cdc_applicable} tables")

    # Detailed failure breakdown
    failed_reports = [r for r in reports if r.cdc_consistent is False]

    if failed_reports:
        print(f"\n{'='*80}")
        print(f"FAILED TABLES BREAKDOWN ({len(failed_reports)} table(s))")
        print(f"{'='*80}\n")

        for report in failed_reports:
            # Collect all failed/warned checks
            failed_checks = [c for c in report.checks if c.status in [ValidationStatus.FAIL, ValidationStatus.WARN]]

            print(f"❌ {report.database}.{report.table_name}")
            print(f"   Status: {report.overall_status.value}")
            print(f"   Reasons:")

            for check in failed_checks:
                # Extract key metrics for display
                metric_summary = []

                if check.check_name == "2_row_count_smoke":
                    diff = check.metrics.get('difference', 0)
                    diff_pct = check.metrics.get('difference_percent', 0)
                    excess = check.metrics.get('excess_rows', 0)
                    if excess > 0:
                        metric_summary.append(f"sink has {excess:,} excess rows ({diff_pct}%)")
                    elif excess < 0:
                        metric_summary.append(f"sink missing {abs(excess):,} rows ({diff_pct}%)")

                elif check.check_name == "3_distinct_pk_cardinality":
                    diff = check.metrics.get('difference', 0)
                    if diff != 0:
                        metric_summary.append(f"{abs(diff):,} PK difference")

                elif check.check_name == "4_sink_duplication":
                    dup_rows = check.metrics.get('duplicate_rows', 0)
                    dup_pct = check.metrics.get('duplicate_ratio_percent', 0)
                    null_pks = check.metrics.get('null_pk_count', 0)
                    if dup_rows > 0:
                        metric_summary.append(f"{dup_rows:,} duplicates ({dup_pct}%)")
                    if null_pks > 0:
                        metric_summary.append(f"{null_pks:,} NULL PKs")

                elif check.check_name == "5_anti_join":
                    orphans = check.metrics.get('orphan_rows', 0)
                    if orphans > 0:
                        orphan_pct = check.metrics.get('orphan_ratio_percent', 0)
                        metric_summary.append(f"{orphans:,} orphan rows ({orphan_pct}%)")

                elif check.check_name == "6_freshness":
                    lag_min = check.metrics.get('lag_minutes', 0)
                    threshold = check.metrics.get('threshold_minutes', 0)
                    if lag_min > 0:
                        metric_summary.append(f"{lag_min:.1f} min lag (threshold: {threshold} min)")

                # Build reason string
                reason = f"[{check.status.value}] {check.check_name}"
                if metric_summary:
                    reason += f" - {', '.join(metric_summary)}"

                print(f"      • {reason}")

            print()  # Empty line between tables

        print(f"{'='*80}")
        print(f"\n⚠️  CDC CORRECTNESS VIOLATIONS DETECTED")
        print("Common causes:")
        print("  - Snapshot + streaming replay overlap (duplicates)")
        print("  - Connector restart without proper offset management")
        print("  - Missing primary key enforcement")
        print("  - Dirty database resets")
        print(f"{'='*80}\n")
    else:
        print(f"\n✓ All tables are CDC-consistent")
        print(f"{'='*80}\n")


# ============================================================================
# CLI Interface
# ============================================================================

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='CDC Pipeline Validation - Detect replay, duplication, and drift',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python validation.py --database trading
  python validation.py --database finance --tables T_INVOICE,T_ACCOUNT
  python validation.py --all

CDC Validation Principles:
  - At-least-once delivery (not exactly-once)
  - Any row count difference is suspicious
  - Compares source vs sink keys, not just sink internals
  - Detects snapshot + streaming replay overlap
        """
    )
    parser.add_argument('--database', choices=list(DATABASE_CONFIGS.keys()),
                        help='Database to validate')
    parser.add_argument('--tables', help='Comma-separated list of tables (default: all from allowlist)')
    parser.add_argument('--all', action='store_true', help='Validate all databases')

    args = parser.parse_args()

    if not args.database and not args.all:
        parser.error("Either --database or --all is required")

    databases_to_validate = list(DATABASE_CONFIGS.keys()) if args.all else [args.database]
    all_reports = []

    for database in databases_to_validate:
        logger.info(f"\n{'#'*80}")
        logger.info(f"# DATABASE: {database.upper()}")
        logger.info(f"{'#'*80}")

        tables = get_tables_from_env(database)

        if args.tables and not args.all:
            specified = [t.strip() for t in args.tables.split(',')]
            tables = [t for t in tables if t in specified]

        if not tables:
            logger.warning(f"No tables found for {database}")
            continue

        logger.info(f"Validating {len(tables)} table(s): {', '.join(tables)}")

        try:
            with DatabaseConnection(database) as db_conn:
                for table_name in tables:
                    metadata = discover_table_metadata(db_conn, database, table_name)

                    if not metadata:
                        logger.error(f"Skipping {table_name} - no primary key found (CDC validation requires explicit PK)")
                        continue

                    validator = TableValidator(db_conn, metadata, database)
                    report = validator.run_all_checks()
                    print_report(report)
                    all_reports.append(report)

        except Exception as e:
            logger.error(f"Error validating {database}: {e}")
            continue

    if all_reports:
        # Pass database name only if single database validation
        db_name = args.database if args.database and not args.all else None
        print_summary(all_reports, database=db_name)

        # Send Slack notification
        notifier = SlackNotifier()
        notifier.send_summary(all_reports, database=db_name)

        # Exit with error if any CDC inconsistencies
        if any(not r.cdc_consistent for r in all_reports):
            sys.exit(1)
    else:
        logger.warning("No tables validated")
        sys.exit(1)


if __name__ == '__main__':
    main()
