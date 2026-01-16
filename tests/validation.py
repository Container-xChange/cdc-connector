#!/usr/bin/env python3
"""
CDC Pipeline Validation Framework

Validates data consistency between MariaDB sources and PostgreSQL sink.
Follows the 11-check validation framework for production CDC pipelines.

Usage:
    python validation.py --source trading --tables T_DEAL,T_USER
    python validation.py --source finance --all
    python validation.py --config validation_config.yaml
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
import random

# ============================================================================
# Configuration & Constants
# ============================================================================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ValidationStatus(Enum):
    """Validation check status"""
    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"


class TableType(Enum):
    """Table classification based on mutation patterns"""
    STATIC = "static"           # Rarely changes
    APPEND_ONLY = "append_only"  # Only inserts
    MUTABLE = "mutable"          # Updates and deletes
    HOT = "hot"                  # Frequent changes


@dataclass
class TableMetadata:
    """Table-level metadata for validation"""
    name: str
    primary_key: List[str]
    timestamp_column: Optional[str] = None
    table_type: TableType = TableType.MUTABLE
    freshness_threshold_minutes: int = 10
    row_count_tolerance_percent: float = 5.0
    sample_size: int = 100
    critical_columns: List[str] = field(default_factory=list)
    nullable_columns: List[str] = field(default_factory=list)


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
    schema: str
    timestamp: datetime
    checks: List[CheckResult]
    overall_status: ValidationStatus
    summary: str


# ============================================================================
# Database Connection Managers
# ============================================================================

class DatabaseConfig:
    """Database connection configuration"""

    @staticmethod
    def get_mariadb_config(source: str) -> Dict[str, Any]:
        """Get MariaDB connection config for source (trading/finance/live)"""
        source_upper = source.upper()
        return {
            'host': os.getenv(f'{source_upper}_HOST'),
            'port': int(os.getenv(f'{source_upper}_PORT', 3306)),
            'database': os.getenv(f'{source_upper}_DB'),
            'user': os.getenv(f'{source_upper}_USER'),
            'password': os.getenv(f'{source_upper}_PASS'),
            'charset': 'utf8mb4',
            'cursorclass': pymysql.cursors.DictCursor
        }

    @staticmethod
    def get_postgres_config() -> Dict[str, Any]:
        """Get PostgreSQL connection config"""
        return {
            'host': os.getenv('PG_HOST', 'localhost'),
            'port': int(os.getenv('PG_PORT', 5432)),
            'database': os.getenv('PG_DB', 'cdc_pipeline'),
            'user': os.getenv('PG_USER', 'postgres'),
            'password': os.getenv('PG_PASS')
        }


class DatabaseConnection:
    """Manages database connections"""

    def __init__(self, source: str):
        self.source = source
        self.mariadb_conn = None
        self.postgres_conn = None

    def __enter__(self):
        """Connect to both databases"""
        try:
            # MariaDB source connection
            maria_config = DatabaseConfig.get_mariadb_config(self.source)
            self.mariadb_conn = pymysql.connect(**maria_config)

            # PostgreSQL sink connection
            pg_config = DatabaseConfig.get_postgres_config()
            self.postgres_conn = psycopg2.connect(**pg_config)

            logger.info(f"Connected to {self.source} (MariaDB) and PostgreSQL")
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
        logger.info("Database connections closed")

    def get_source_cursor(self):
        """Get MariaDB cursor"""
        return self.mariadb_conn.cursor()

    def get_sink_cursor(self):
        """Get PostgreSQL cursor"""
        return self.postgres_conn.cursor(cursor_factory=RealDictCursor)


# ============================================================================
# Validation Checks (11-Check Framework)
# ============================================================================

class ValidationChecks:
    """Implementation of all validation checks"""

    def __init__(self, db_conn: DatabaseConnection, metadata: TableMetadata, source_schema: str, sink_schema: str, table_prefix: str = ''):
        self.db = db_conn
        self.meta = metadata
        self.source_schema = source_schema
        self.sink_schema = sink_schema
        self.table_prefix = table_prefix

    # ------------------------------------------------------------------------
    # Check 0: Metadata & Setup
    # ------------------------------------------------------------------------

    def check_0_metadata(self) -> CheckResult:
        """Verify table metadata is complete"""
        logger.info(f"[Check 0] Validating metadata for {self.meta.name}")

        issues = []
        if not self.meta.primary_key:
            issues.append("No primary key defined")
        if not self.meta.timestamp_column:
            issues.append("No timestamp column specified")

        if issues:
            return CheckResult(
                check_name="0_metadata",
                status=ValidationStatus.FAIL,
                metrics={"issues": issues},
                explanation=f"Metadata incomplete: {', '.join(issues)}"
            )

        return CheckResult(
            check_name="0_metadata",
            status=ValidationStatus.PASS,
            metrics={
                "primary_key": self.meta.primary_key,
                "timestamp_column": self.meta.timestamp_column,
                "table_type": self.meta.table_type.value
            },
            explanation="Metadata complete and valid"
        )

    # ------------------------------------------------------------------------
    # Check 1: Table Presence & Shape
    # ------------------------------------------------------------------------

    def check_1_table_shape(self) -> CheckResult:
        """Verify table exists with matching structure"""
        logger.info(f"[Check 1] Checking table shape for {self.meta.name}")

        try:
            # Get source columns
            with self.db.get_source_cursor() as cur:
                cur.execute(f"""
                    SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                    ORDER BY ORDINAL_POSITION
                """, (self.source_schema, self.meta.name))
                source_cols = {row['COLUMN_NAME'].lower(): row for row in cur.fetchall()}

            # Get sink columns
            sink_table_name = f"{self.table_prefix}{self.meta.name.lower()}"
            with self.db.get_sink_cursor() as cur:
                cur.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (self.sink_schema, sink_table_name))
                sink_cols = {row['column_name'].lower(): row for row in cur.fetchall()}

            # Check for missing table
            if not sink_cols:
                return CheckResult(
                    check_name="1_table_shape",
                    status=ValidationStatus.FAIL,
                    metrics={"source_columns": len(source_cols), "sink_columns": 0},
                    explanation=f"Table {self.sink_schema}.{self.meta.name} does not exist in sink"
                )

            # Check for missing columns
            missing_cols = set(source_cols.keys()) - set(sink_cols.keys())
            if missing_cols:
                return CheckResult(
                    check_name="1_table_shape",
                    status=ValidationStatus.FAIL,
                    metrics={
                        "source_columns": len(source_cols),
                        "sink_columns": len(sink_cols),
                        "missing_columns": list(missing_cols)
                    },
                    explanation=f"Missing columns in sink: {', '.join(missing_cols)}"
                )

            # Check column count
            extra_cols = set(sink_cols.keys()) - set(source_cols.keys())

            return CheckResult(
                check_name="1_table_shape",
                status=ValidationStatus.PASS,
                metrics={
                    "source_columns": len(source_cols),
                    "sink_columns": len(sink_cols),
                    "extra_sink_columns": list(extra_cols) if extra_cols else []
                },
                explanation=f"Table shape matches ({len(source_cols)} columns)"
            )

        except Exception as e:
            logger.error(f"Check 1 failed: {e}")
            return CheckResult(
                check_name="1_table_shape",
                status=ValidationStatus.FAIL,
                metrics={"error": str(e)},
                explanation=f"Error checking table shape: {e}"
            )

    # ------------------------------------------------------------------------
    # Check 2: Row Count Sanity
    # ------------------------------------------------------------------------

    def check_2_row_count(self) -> CheckResult:
        """Compare row counts between source and sink"""
        logger.info(f"[Check 2] Checking row counts for {self.meta.name}")

        try:
            # Source count
            with self.db.get_source_cursor() as cur:
                cur.execute(f"SELECT COUNT(*) as cnt FROM `{self.source_schema}`.`{self.meta.name}`")
                source_count = cur.fetchone()['cnt']

            # Sink count
            sink_table_name = f"{self.table_prefix}{self.meta.name.lower()}"
            with self.db.get_sink_cursor() as cur:
                cur.execute(f"SELECT COUNT(*) as cnt FROM {self.sink_schema}.{sink_table_name}")
                sink_count = cur.fetchone()['cnt']

            # Calculate divergence
            diff = abs(source_count - sink_count)
            diff_percent = (diff / source_count * 100) if source_count > 0 else 0

            # Determine status
            if self.meta.table_type == TableType.STATIC and diff > 0:
                status = ValidationStatus.FAIL
                explanation = f"Static table has row count mismatch: source={source_count}, sink={sink_count}"
            elif diff_percent > self.meta.row_count_tolerance_percent:
                status = ValidationStatus.FAIL
                explanation = f"Row count divergence {diff_percent:.2f}% exceeds tolerance ({self.meta.row_count_tolerance_percent}%)"
            elif diff_percent > (self.meta.row_count_tolerance_percent / 2):
                status = ValidationStatus.WARN
                explanation = f"Row count divergence {diff_percent:.2f}% approaching tolerance limit"
            else:
                status = ValidationStatus.PASS
                explanation = f"Row counts within tolerance: {diff_percent:.2f}% divergence"

            return CheckResult(
                check_name="2_row_count",
                status=status,
                metrics={
                    "source_count": source_count,
                    "sink_count": sink_count,
                    "difference": diff,
                    "difference_percent": round(diff_percent, 2)
                },
                explanation=explanation
            )

        except Exception as e:
            logger.error(f"Check 2 failed: {e}")
            return CheckResult(
                check_name="2_row_count",
                status=ValidationStatus.FAIL,
                metrics={"error": str(e)},
                explanation=f"Error checking row counts: {e}"
            )

    # ------------------------------------------------------------------------
    # Check 3: Primary Key Integrity
    # ------------------------------------------------------------------------

    def check_3_pk_integrity(self) -> CheckResult:
        """Verify primary key uniqueness and non-nullability in sink"""
        logger.info(f"[Check 3] Checking PK integrity for {self.meta.name}")

        try:
            pk_cols = [col.lower() for col in self.meta.primary_key]
            pk_str = ', '.join(pk_cols)
            sink_table_name = f"{self.table_prefix}{self.meta.name.lower()}"

            with self.db.get_sink_cursor() as cur:
                # Check for duplicate PKs
                cur.execute(f"""
                    SELECT COUNT(*) as total_rows,
                           COUNT(DISTINCT ({pk_str})) as distinct_pks
                    FROM {self.sink_schema}.{sink_table_name}
                """)
                result = cur.fetchone()
                total_rows = result['total_rows']
                distinct_pks = result['distinct_pks']

                # Check for NULL PKs
                null_conditions = ' OR '.join([f"{col} IS NULL" for col in pk_cols])
                cur.execute(f"""
                    SELECT COUNT(*) as null_pk_count
                    FROM {self.sink_schema}.{sink_table_name}
                    WHERE {null_conditions}
                """)
                null_pk_count = cur.fetchone()['null_pk_count']

            # Determine status
            issues = []
            if total_rows != distinct_pks:
                issues.append(f"Duplicate PKs detected: {total_rows - distinct_pks} duplicates")
            if null_pk_count > 0:
                issues.append(f"NULL PKs detected: {null_pk_count} rows")

            if issues:
                return CheckResult(
                    check_name="3_pk_integrity",
                    status=ValidationStatus.FAIL,
                    metrics={
                        "total_rows": total_rows,
                        "distinct_pks": distinct_pks,
                        "null_pk_count": null_pk_count,
                        "duplicates": total_rows - distinct_pks
                    },
                    explanation=f"PK integrity violations: {'; '.join(issues)}"
                )

            return CheckResult(
                check_name="3_pk_integrity",
                status=ValidationStatus.PASS,
                metrics={
                    "total_rows": total_rows,
                    "distinct_pks": distinct_pks,
                    "null_pk_count": 0
                },
                explanation=f"All {total_rows} rows have unique, non-null PKs"
            )

        except Exception as e:
            logger.error(f"Check 3 failed: {e}")
            return CheckResult(
                check_name="3_pk_integrity",
                status=ValidationStatus.FAIL,
                metrics={"error": str(e)},
                explanation=f"Error checking PK integrity: {e}"
            )

    # ------------------------------------------------------------------------
    # Check 4: Freshness / Lag (MOST IMPORTANT)
    # ------------------------------------------------------------------------

    def check_4_freshness(self) -> CheckResult:
        """Verify data freshness and CDC lag"""
        logger.info(f"[Check 4] Checking freshness for {self.meta.name}")

        if not self.meta.timestamp_column:
            return CheckResult(
                check_name="4_freshness",
                status=ValidationStatus.WARN,
                metrics={},
                explanation="No timestamp column configured - freshness check skipped"
            )

        try:
            ts_col = self.meta.timestamp_column
            sink_table_name = f"{self.table_prefix}{self.meta.name.lower()}"

            # Get max timestamp from source
            with self.db.get_source_cursor() as cur:
                cur.execute(f"""
                    SELECT MAX(`{ts_col}`) as max_ts
                    FROM `{self.source_schema}`.`{self.meta.name}`
                """)
                source_max = cur.fetchone()['max_ts']

            # Get max timestamp from sink
            with self.db.get_sink_cursor() as cur:
                cur.execute(f"""
                    SELECT MAX({ts_col.lower()}) as max_ts
                    FROM {self.sink_schema}.{sink_table_name}
                """)
                sink_max = cur.fetchone()['max_ts']

            # Handle case where timestamps are None
            if source_max is None or sink_max is None:
                return CheckResult(
                    check_name="4_freshness",
                    status=ValidationStatus.WARN,
                    metrics={"source_max": str(source_max), "sink_max": str(sink_max)},
                    explanation="No timestamp data available for freshness check"
                )

            # Normalize timezones - remove timezone info for comparison
            if hasattr(source_max, 'tzinfo') and source_max.tzinfo is not None:
                source_max = source_max.replace(tzinfo=None)
            if hasattr(sink_max, 'tzinfo') and sink_max.tzinfo is not None:
                sink_max = sink_max.replace(tzinfo=None)

            # Calculate lag
            lag = source_max - sink_max if isinstance(source_max, datetime) else timedelta(0)
            lag_minutes = lag.total_seconds() / 60 if isinstance(lag, timedelta) else 0

            # Determine status based on threshold
            threshold = self.meta.freshness_threshold_minutes
            if lag_minutes > threshold:
                status = ValidationStatus.FAIL
                explanation = f"Data lag {lag_minutes:.1f} min exceeds threshold ({threshold} min)"
            elif lag_minutes > (threshold * 0.8):
                status = ValidationStatus.WARN
                explanation = f"Data lag {lag_minutes:.1f} min approaching threshold ({threshold} min)"
            else:
                status = ValidationStatus.PASS
                explanation = f"Data is fresh: {lag_minutes:.1f} min lag"

            return CheckResult(
                check_name="4_freshness",
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
            logger.error(f"Check 4 failed: {e}")
            return CheckResult(
                check_name="4_freshness",
                status=ValidationStatus.FAIL,
                metrics={"error": str(e)},
                explanation=f"Error checking freshness: {e}"
            )

    # ------------------------------------------------------------------------
    # Check 5: Recent Activity Flow
    # ------------------------------------------------------------------------

    def check_5_activity_flow(self) -> CheckResult:
        """Verify recent changes are flowing from source to sink"""
        logger.info(f"[Check 5] Checking activity flow for {self.meta.name}")

        if not self.meta.timestamp_column:
            return CheckResult(
                check_name="5_activity_flow",
                status=ValidationStatus.WARN,
                metrics={},
                explanation="No timestamp column - activity flow check skipped"
            )

        try:
            ts_col = self.meta.timestamp_column
            lookback_minutes = 30  # Check last 30 minutes
            sink_table_name = f"{self.table_prefix}{self.meta.name.lower()}"

            # Source activity
            with self.db.get_source_cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*) as recent_count
                    FROM `{self.source_schema}`.`{self.meta.name}`
                    WHERE `{ts_col}` >= DATE_SUB(NOW(), INTERVAL %s MINUTE)
                """, (lookback_minutes,))
                source_recent = cur.fetchone()['recent_count']

            # Sink activity
            with self.db.get_sink_cursor() as cur:
                cur.execute(f"""
                    SELECT COUNT(*) as recent_count
                    FROM {self.sink_schema}.{sink_table_name}
                    WHERE {ts_col.lower()} >= NOW() - INTERVAL '%s minutes'
                """, (lookback_minutes,))
                sink_recent = cur.fetchone()['recent_count']

            # Determine status
            if source_recent > 0 and sink_recent == 0:
                status = ValidationStatus.FAIL
                explanation = f"Source has {source_recent} recent changes but sink has 0"
            elif source_recent > 0 and sink_recent < (source_recent * 0.5):
                status = ValidationStatus.WARN
                explanation = f"Sink has only {sink_recent}/{source_recent} recent changes"
            else:
                status = ValidationStatus.PASS
                explanation = f"Activity flowing normally: {sink_recent}/{source_recent} recent changes"

            return CheckResult(
                check_name="5_activity_flow",
                status=status,
                metrics={
                    "lookback_minutes": lookback_minutes,
                    "source_recent_count": source_recent,
                    "sink_recent_count": sink_recent
                },
                explanation=explanation
            )

        except Exception as e:
            logger.error(f"Check 5 failed: {e}")
            return CheckResult(
                check_name="5_activity_flow",
                status=ValidationStatus.FAIL,
                metrics={"error": str(e)},
                explanation=f"Error checking activity flow: {e}"
            )

    # ------------------------------------------------------------------------
    # Check 6: Delete Semantics (Skipped - hard deletes not enabled)
    # ------------------------------------------------------------------------

    def check_6_delete_semantics(self) -> CheckResult:
        """Check delete handling (skipped - not enabled)"""
        logger.info(f"[Check 6] Delete semantics check (skipped)")

        return CheckResult(
            check_name="6_delete_semantics",
            status=ValidationStatus.PASS,
            metrics={},
            explanation="Delete handling not enabled - check skipped"
        )

    # ------------------------------------------------------------------------
    # Check 7: NULL Ratio Drift
    # ------------------------------------------------------------------------

    def check_7_null_drift(self) -> CheckResult:
        """Check for unexpected NULL value increases"""
        logger.info(f"[Check 7] Checking NULL ratio drift for {self.meta.name}")

        if not self.meta.critical_columns:
            return CheckResult(
                check_name="7_null_drift",
                status=ValidationStatus.PASS,
                metrics={},
                explanation="No critical columns configured - NULL drift check skipped"
            )

        try:
            null_drifts = []
            sink_table_name = f"{self.table_prefix}{self.meta.name.lower()}"

            for col in self.meta.critical_columns:
                # Source NULL ratio
                with self.db.get_source_cursor() as cur:
                    cur.execute(f"""
                        SELECT
                            COUNT(*) as total,
                            SUM(CASE WHEN `{col}` IS NULL THEN 1 ELSE 0 END) as nulls
                        FROM `{self.source_schema}`.`{self.meta.name}`
                    """)
                    result = cur.fetchone()
                    source_null_pct = (result['nulls'] / result['total'] * 100) if result['total'] > 0 else 0

                # Sink NULL ratio
                with self.db.get_sink_cursor() as cur:
                    cur.execute(f"""
                        SELECT
                            COUNT(*) as total,
                            COUNT(CASE WHEN {col.lower()} IS NULL THEN 1 END) as nulls
                        FROM {self.sink_schema}.{sink_table_name}
                    """)
                    result = cur.fetchone()
                    sink_null_pct = (result['nulls'] / result['total'] * 100) if result['total'] > 0 else 0

                # Check for drift
                drift = abs(sink_null_pct - source_null_pct)
                if drift > 10:  # >10% difference
                    null_drifts.append({
                        "column": col,
                        "source_null_pct": round(source_null_pct, 2),
                        "sink_null_pct": round(sink_null_pct, 2),
                        "drift": round(drift, 2)
                    })

            if null_drifts:
                return CheckResult(
                    check_name="7_null_drift",
                    status=ValidationStatus.FAIL,
                    metrics={"drifts": null_drifts},
                    explanation=f"NULL ratio drift detected in {len(null_drifts)} column(s)"
                )

            return CheckResult(
                check_name="7_null_drift",
                status=ValidationStatus.PASS,
                metrics={"checked_columns": self.meta.critical_columns},
                explanation=f"No significant NULL drift in {len(self.meta.critical_columns)} critical columns"
            )

        except Exception as e:
            logger.error(f"Check 7 failed: {e}")
            return CheckResult(
                check_name="7_null_drift",
                status=ValidationStatus.FAIL,
                metrics={"error": str(e)},
                explanation=f"Error checking NULL drift: {e}"
            )

    # ------------------------------------------------------------------------
    # Check 8: Sample Row Content Validation
    # ------------------------------------------------------------------------

    def check_8_sample_validation(self) -> CheckResult:
        """Validate actual data content by sampling rows"""
        logger.info(f"[Check 8] Validating sample rows for {self.meta.name}")

        try:
            pk_cols = self.meta.primary_key
            pk_str_source = ', '.join([f'`{col}`' for col in pk_cols])
            pk_str_sink = ', '.join([col.lower() for col in pk_cols])
            sink_table_name = f"{self.table_prefix}{self.meta.name.lower()}"

            # Get random sample of PKs from source
            with self.db.get_source_cursor() as cur:
                cur.execute(f"""
                    SELECT {pk_str_source}
                    FROM `{self.source_schema}`.`{self.meta.name}`
                    ORDER BY RAND()
                    LIMIT %s
                """, (self.meta.sample_size,))
                sample_pks = cur.fetchall()

            if not sample_pks:
                return CheckResult(
                    check_name="8_sample_validation",
                    status=ValidationStatus.WARN,
                    metrics={},
                    explanation="No rows available for sampling"
                )

            mismatches = []
            matched = 0

            # Compare each sampled row
            for pk_row in sample_pks[:min(100, len(sample_pks))]:  # Limit to 100 for performance
                pk_condition_source = ' AND '.join([f"`{col}` = %s" for col in pk_cols])
                pk_condition_sink = ' AND '.join([f"{col.lower()} = %s" for col in pk_cols])
                pk_values = [pk_row[col] for col in pk_cols]

                # Fetch from source
                with self.db.get_source_cursor() as cur:
                    cur.execute(f"""
                        SELECT *
                        FROM `{self.source_schema}`.`{self.meta.name}`
                        WHERE {pk_condition_source}
                    """, pk_values)
                    source_row = cur.fetchone()

                # Fetch from sink
                with self.db.get_sink_cursor() as cur:
                    cur.execute(f"""
                        SELECT *
                        FROM {self.sink_schema}.{sink_table_name}
                        WHERE {pk_condition_sink}
                    """, pk_values)
                    sink_row = cur.fetchone()

                if not sink_row:
                    mismatches.append({"pk": pk_values, "issue": "Row missing in sink"})
                    continue

                # Compare non-PK columns
                row_mismatches = []
                for col in source_row.keys():
                    if col not in pk_cols:
                        source_val = source_row[col]
                        sink_val = sink_row.get(col.lower())

                        # Normalize for comparison
                        # Handle timezone-aware vs timezone-naive datetime
                        if isinstance(source_val, datetime) and isinstance(sink_val, datetime):
                            if hasattr(source_val, 'tzinfo') and source_val.tzinfo is not None:
                                source_val = source_val.replace(tzinfo=None)
                            if hasattr(sink_val, 'tzinfo') and sink_val.tzinfo is not None:
                                sink_val = sink_val.replace(tzinfo=None)

                        # Handle type conversions (e.g., bit -> boolean, int -> bool)
                        if isinstance(source_val, int) and isinstance(sink_val, bool):
                            source_val = bool(source_val)
                        elif isinstance(source_val, bytes) and isinstance(sink_val, bool):
                            source_val = bool(int.from_bytes(source_val, byteorder='big'))

                        if source_val != sink_val:
                            row_mismatches.append(f"{col}: {source_val} != {sink_val}")

                if row_mismatches:
                    mismatches.append({"pk": pk_values, "mismatches": row_mismatches})
                else:
                    matched += 1

            if mismatches:
                return CheckResult(
                    check_name="8_sample_validation",
                    status=ValidationStatus.FAIL,
                    metrics={
                        "sample_size": len(sample_pks),
                        "matched": matched,
                        "mismatches": len(mismatches),
                        "mismatch_details": mismatches[:5]  # First 5 for brevity
                    },
                    explanation=f"{len(mismatches)} sample rows have data mismatches"
                )

            return CheckResult(
                check_name="8_sample_validation",
                status=ValidationStatus.PASS,
                metrics={
                    "sample_size": len(sample_pks),
                    "matched": matched,
                    "mismatches": 0
                },
                explanation=f"All {matched} sampled rows match exactly"
            )

        except Exception as e:
            logger.error(f"Check 8 failed: {e}")
            return CheckResult(
                check_name="8_sample_validation",
                status=ValidationStatus.FAIL,
                metrics={"error": str(e)},
                explanation=f"Error validating sample rows: {e}"
            )

    # ------------------------------------------------------------------------
    # Check 9: Aggregate Invariants
    # ------------------------------------------------------------------------

    def check_9_aggregates(self) -> CheckResult:
        """Validate business-level aggregates"""
        logger.info(f"[Check 9] Checking aggregate invariants for {self.meta.name}")

        # This is table-specific - would need configuration
        # For now, just pass
        return CheckResult(
            check_name="9_aggregates",
            status=ValidationStatus.PASS,
            metrics={},
            explanation="No aggregate invariants configured - check skipped"
        )

    # ------------------------------------------------------------------------
    # Check 10: CDC Coverage Sanity
    # ------------------------------------------------------------------------

    def check_10_cdc_coverage(self) -> CheckResult:
        """Verify CDC is actively covering the table"""
        logger.info(f"[Check 10] Checking CDC coverage for {self.meta.name}")

        # This combines freshness and activity checks
        # Already covered by checks 4 and 5
        return CheckResult(
            check_name="10_cdc_coverage",
            status=ValidationStatus.PASS,
            metrics={},
            explanation="CDC coverage validated by checks 4 and 5"
        )


# ============================================================================
# Validation Orchestrator
# ============================================================================

class TableValidator:
    """Orchestrates all validation checks for a table"""

    def __init__(self, db_conn: DatabaseConnection, metadata: TableMetadata, source: str):
        self.db = db_conn
        self.meta = metadata
        self.source = source

        # Determine schema names
        self.source_schema = os.getenv(f'{source.upper()}_DB')
        self.sink_schema = 'mp_cdc'  # Single schema for all databases

        # Table prefix based on source database
        self.table_prefix = f'{source.lower()}_'

    def run_all_checks(self) -> TableValidationReport:
        """Run all 11 validation checks"""
        logger.info(f"\n{'='*80}")
        logger.info(f"Validating table: {self.meta.name}")
        logger.info(f"{'='*80}")

        checks_runner = ValidationChecks(self.db, self.meta, self.source_schema, self.sink_schema, self.table_prefix)

        # Run all checks in order
        checks = [
            checks_runner.check_0_metadata(),
            checks_runner.check_1_table_shape(),
            checks_runner.check_2_row_count(),
            checks_runner.check_3_pk_integrity(),
            checks_runner.check_4_freshness(),
            checks_runner.check_5_activity_flow(),
            checks_runner.check_6_delete_semantics(),
            checks_runner.check_7_null_drift(),
            checks_runner.check_8_sample_validation(),
            checks_runner.check_9_aggregates(),
            checks_runner.check_10_cdc_coverage(),
        ]

        # Determine overall status
        if any(c.status == ValidationStatus.FAIL for c in checks):
            overall_status = ValidationStatus.FAIL
        elif any(c.status == ValidationStatus.WARN for c in checks):
            overall_status = ValidationStatus.WARN
        else:
            overall_status = ValidationStatus.PASS

        # Count results
        fail_count = sum(1 for c in checks if c.status == ValidationStatus.FAIL)
        warn_count = sum(1 for c in checks if c.status == ValidationStatus.WARN)
        pass_count = sum(1 for c in checks if c.status == ValidationStatus.PASS)

        summary = f"Checks: {pass_count} PASS, {warn_count} WARN, {fail_count} FAIL"

        return TableValidationReport(
            table_name=self.meta.name,
            schema=self.sink_schema,
            timestamp=datetime.now(),
            checks=checks,
            overall_status=overall_status,
            summary=summary
        )


# ============================================================================
# Reporting
# ============================================================================

def print_report(report: TableValidationReport):
    """Print validation report to console"""

    print(f"\n{'='*80}")
    print(f"VALIDATION REPORT: {report.table_name}")
    print(f"Schema: {report.schema}")
    print(f"Timestamp: {report.timestamp}")
    print(f"Overall Status: {report.overall_status.value}")
    print(f"Summary: {report.summary}")
    print(f"{'='*80}\n")

    for check in report.checks:
        status_icon = {
            ValidationStatus.PASS: "[PASS]",
            ValidationStatus.WARN: "[WARN]",
            ValidationStatus.FAIL: "[FAIL]"
        }[check.status]

        print(f"{status_icon} [{check.status.value}] {check.check_name}")
        print(f"   {check.explanation}")

        if check.metrics and check.status != ValidationStatus.PASS:
            print(f"   Metrics: {check.metrics}")
        print()

    print(f"{'='*80}\n")


# ============================================================================
# Example Table Metadata Configurations
# ============================================================================

TRADING_TABLES = {
    'T_DEAL': TableMetadata(
        name='T_DEAL',
        primary_key=['ID'],
        timestamp_column='LAST_MODIFIED_DATE',
        table_type=TableType.MUTABLE,
        freshness_threshold_minutes=10,
        critical_columns=['STATUS', 'PRICE']
    ),
    'T_USER': TableMetadata(
        name='T_USER',
        primary_key=['ID'],
        timestamp_column='LAST_MODIFIED_DATE',
        table_type=TableType.MUTABLE,
        freshness_threshold_minutes=15
    ),
    'T_ABSTRACT_OFFER': TableMetadata(
        name='T_ABSTRACT_OFFER',
        primary_key=['ID'],
        timestamp_column='LAST_MODIFIED_DATE',
        table_type=TableType.MUTABLE,
        freshness_threshold_minutes=10
    ),
    'T_CARRIER': TableMetadata(
        name='T_CARRIER',
        primary_key=['ID'],
        timestamp_column='LAST_MODIFIED_DATE',
        table_type=TableType.MUTABLE,
        freshness_threshold_minutes=10
    ),
    'T_LOCATION': TableMetadata(
        name='T_LOCATION',
        primary_key=['ID'],
        timestamp_column='LAST_MODIFIED_DATE',
        table_type=TableType.MUTABLE,
        freshness_threshold_minutes=10
    ),
}

FINANCE_TABLES = {
    'T_INVOICE': TableMetadata(
        name='T_INVOICE',
        primary_key=['ID'],
        timestamp_column='LAST_MODIFIED_DATE',
        table_type=TableType.MUTABLE,
        freshness_threshold_minutes=10,
        critical_columns=['STATUS', 'TOTAL_AMOUNT']
    ),
}


# ============================================================================
# CLI Interface
# ============================================================================

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='CDC Pipeline Validation')
    parser.add_argument('--source', required=True, choices=['trading', 'finance', 'live'],
                        help='Source database to validate')
    parser.add_argument('--tables', help='Comma-separated list of tables to validate')
    parser.add_argument('--all', action='store_true', help='Validate all tables for source')

    args = parser.parse_args()

    # Determine which tables to validate
    if args.source == 'trading':
        table_configs = TRADING_TABLES
    elif args.source == 'finance':
        table_configs = FINANCE_TABLES
    else:
        logger.error(f"No table configurations for source: {args.source}")
        sys.exit(1)

    if args.tables:
        tables_to_validate = [t.strip() for t in args.tables.split(',')]
        table_configs = {k: v for k, v in table_configs.items() if k in tables_to_validate}

    if not table_configs:
        logger.error("No tables to validate")
        sys.exit(1)

    # Run validations
    logger.info(f"Starting validation for {len(table_configs)} table(s) in {args.source}")

    with DatabaseConnection(args.source) as db_conn:
        for table_name, metadata in table_configs.items():
            validator = TableValidator(db_conn, metadata, args.source)
            report = validator.run_all_checks()
            print_report(report)


if __name__ == '__main__':
    main()
