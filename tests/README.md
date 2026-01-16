# CDC Testing Suite

Two complementary test scripts for validating CDC pipeline correctness.

## Test Scripts

### 1. `validation.py` - Comprehensive Data Validation

**What it does**: Validates current state of data between source and sink using an 11-check framework.

**When to use**:
- After initial migration to verify data integrity
- Periodic health checks
- Troubleshooting data inconsistencies

**Checks performed**:
- ✅ **Check 0**: Metadata completeness
- ✅ **Check 1**: Table shape (schema matching)
- ✅ **Check 2**: Row count sanity
- ✅ **Check 3**: Primary key integrity
- ✅ **Check 4**: Data freshness/lag (CRITICAL)
- ✅ **Check 5**: Recent activity flow
- ✅ **Check 6**: Delete semantics (skipped - not enabled)
- ✅ **Check 7**: NULL ratio drift
- ✅ **Check 8**: Sample row content validation
- ✅ **Check 9**: Aggregate invariants
- ✅ **Check 10**: CDC coverage

**Usage**:
```bash
# Single table
python tests/validation.py --source trading --tables T_ABSTRACT_OFFER

# Multiple tables
python tests/validation.py --source trading --tables T_DEAL,T_USER

# All configured tables
python tests/validation.py --source trading --all
```

---

### 2. `test_cdc_live.py` - Real-Time CDC Replication Test

**What it does**: Actively tests CDC by making a change in MariaDB and verifying it propagates to PostgreSQL.

**When to use**:
- Verify CDC is actively working RIGHT NOW
- After connector registration
- After CDC configuration changes
- Debugging replication lag issues

**How it works**:
1. Selects a random row from source table
2. Reads current timestamp from both MariaDB and PostgreSQL
3. **Makes a NEW update** in MariaDB (updates `LAST_MODIFIED_DATE`)
4. Waits 10 seconds for CDC to propagate
5. Verifies the change appears in PostgreSQL

**Usage**:
```bash
# Test T_ABSTRACT_OFFER
python tests/test_cdc_live.py --source trading --table T_ABSTRACT_OFFER

# Test with custom wait time
python tests/test_cdc_live.py --source finance --table T_INVOICE --wait 15

# Test other sources
python tests/test_cdc_live.py --source live --table T_CARRIER
```

---

## Key Differences

| Aspect | `validation.py` | `test_cdc_live.py` |
|--------|----------------|-------------------|
| **Type** | Passive validation | Active test |
| **Scope** | Comprehensive (11 checks) | Focused (replication) |
| **Changes data?** | ❌ No (read-only) | ✅ Yes (updates timestamp) |
| **Use case** | Verify current state | Prove CDC is working now |
| **Run frequency** | Scheduled/on-demand | Ad-hoc testing |

---

## Recommended Testing Workflow

### Initial Setup Validation
```bash
# 1. Run validation after migration
python tests/validation.py --source trading --all

# 2. Test live CDC on key tables
python tests/test_cdc_live.py --source trading --table T_DEAL
python tests/test_cdc_live.py --source trading --table T_ABSTRACT_OFFER
```

### Ongoing Monitoring
```bash
# Daily: Run validation
python tests/validation.py --source trading --all

# Weekly: Spot-check live CDC
python tests/test_cdc_live.py --source trading --table T_ABSTRACT_OFFER
```

### Troubleshooting
```bash
# If validation shows issues, run live test to isolate problem
python tests/validation.py --source trading --tables T_DEAL
python tests/test_cdc_live.py --source trading --table T_DEAL
```

---

## Environment Requirements

Both scripts require:
- `.env` file with database credentials
- Python packages: `pymysql`, `psycopg2`, `python-dotenv`

Install dependencies:
```bash
pip install -r requirements-validation.txt
```

---

## Exit Codes

- **0**: Success (all checks pass)
- **1**: Failure (one or more checks failed)

Useful for CI/CD pipelines:
```bash
python tests/test_cdc_live.py --source trading --table T_DEAL || echo "CDC test failed!"
```
