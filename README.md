# iceberg-fgac — Fine-Grained Access Control for Apache Iceberg

Engine-agnostic FGAC framework providing row-level filtering via native
Iceberg Expressions, column masking via pure-Java UDFs, versioned policy
storage as an Iceberg table in Nessie, and an immutable audit log.

---

## Module structure

```
iceberg-fgac/
├── fgac-api               Pure Java interfaces + immutable domain objects
├── fgac-core              IcebergExpressionBuilder, PolicyCacheManager, ColumnPolicyEngine
├── fgac-policy-store      IcebergPolicyStore (Nessie-backed), JsonPolicyReader, SeedPolicyLoader
├── fgac-masking           MaskingFunctionLibrary (pure static methods, no engine deps)
├── fgac-audit             AsyncAuditEventEmitter, IcebergAuditSink, LoggingAuditSink
├── fgac-spark-adapter     FgacSparkExtension, CatalystEntitlementRule, SparkUdfRegistrar
├── fgac-testing           MockPolicyStore, CapturingAuditEmitter, PolicyFixtures
├── fgac-integration-tests Testcontainers-based end-to-end tests
└── fgac-bom               Bill of Materials for version alignment
```

---

## Build

```bash
mvn clean package -DskipTests

# With tests (requires no external services for unit tests)
mvn clean verify

# Build the Spark fat JAR only
mvn clean package -pl fgac-spark-adapter -am
# Output: fgac-spark-adapter/target/fgac-spark-adapter-1.0.0-SNAPSHOT-spark-fat.jar
```

---

## Integrating with Spark

Add two config keys when building the SparkSession (or in spark-defaults.conf):

```python
spark = (
    SparkSession.builder
    .config(
        "spark.sql.extensions",
        "com.banking.lakehouse.fgac.spark.extension.FgacSparkExtension,"
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
        "org.projectnessie.spark.extensions.NessieSparkSessionExtensions"
    )
    .config("spark.app.current_user", "svc_airflow")   # identity for FGAC lookup
    # FGAC framework configuration (override defaults from fgac-defaults.properties)
    .config("spark.driver.extraJavaOptions",
            "-Dfgac.nessie.uri=http://nessie:19120/api/v1 "
            "-Dfgac.s3a.endpoint=http://minio:9000")
    .getOrCreate()
)
```

Add the fat JAR to the Spark driver and executor classpaths:

```bash
spark-submit \
  --jars fgac-spark-adapter-1.0.0-SNAPSHOT-spark-fat.jar \
  --conf spark.sql.extensions=com.banking.lakehouse.fgac.spark.extension.FgacSparkExtension,... \
  your_job.py
```

---

## Policy management

### Seed file (first startup)

Edit `config/policies/seed-policies.json`. On first startup, `SeedPolicyLoader`
creates `nessie.entitlements.rules` and inserts all rows. Subsequent startups
skip seeding if the table already exists.

### Adding a new entitlement at runtime

Use Spark SQL against the Nessie catalog:

```sql
INSERT INTO nessie.entitlements.rules
  (role_name, table_name, row_predicate, allowed_columns,
   masked_columns, redacted_columns, audit_enabled, description)
VALUES (
  'analyst_sg',
  'silver.cleansed_transactions',
  'jurisdiction = ''SG''',
  '["transaction_id","account_number","transaction_amount","transaction_ts","jurisdiction"]',
  '{"account_number":"fgac_mask_account_number"}',
  '["data_classification","_ingested_at","_nessie_ref"]',
  true,
  'Singapore analyst scope'
);
```

The change takes effect within `fgac.policy.cache.ttl-minutes` (default 5 min).
For immediate effect, restart the Spark session or call `PolicyCacheManager.invalidateAll()`.

---

## Configuration reference

All properties can be overridden via system properties (`-Dfgac.*`),
environment variables, or an external properties file at `fgac.config.path`.

| Property | Default | Description |
|---|---|---|
| `fgac.nessie.uri` | `http://nessie:19120/api/v1` | Nessie REST endpoint |
| `fgac.nessie.ref` | `main` | Default Nessie branch |
| `fgac.nessie.warehouse` | `s3a://warehouse/` | Iceberg warehouse root |
| `fgac.s3a.endpoint` | `http://minio:9000` | S3A / MinIO endpoint |
| `fgac.s3a.access.key` | `minioadmin` | S3A access key |
| `fgac.s3a.secret.key` | `minioadmin` | S3A secret key |
| `fgac.policy.cache.ttl-minutes` | `5` | Policy cache TTL |
| `fgac.policy.cache.max-size` | `10000` | Max cached (role, table) pairs |
| `fgac.policy.seed.path` | `classpath:/config/policies/seed-policies.json` | Seed file location |
| `fgac.audit.queue-capacity` | `10000` | Bounded audit event queue size |
| `fgac.audit.batch-size` | `200` | Events per Iceberg append |
| `fgac.audit.flush-interval-seconds` | `10` | Background drainer interval |
| `fgac.audit.retention.days` | `2555` | Audit table snapshot retention (7 years) |
| `fgac.identity.spark.conf-key` | `spark.app.current_user` | Spark conf key for principal ID |

---

## Masking UDFs registered in Spark

All UDFs are prefixed `fgac_` to avoid namespace collision.

| UDF name | Input → Output | Example |
|---|---|---|
| `fgac_mask_account_number` | String → String | `1234567890123456` → `************3456` |
| `fgac_mask_email` | String → String | `john@bank.com` → `j***@bank.com` |
| `fgac_redact_national_id` | String → String | `AB123456C` → `REDACTED` |
| `fgac_mask_phone` | String → String | `+447911123456` → `+4479111234**` |
| `fgac_mask_date_of_birth` | String → String | `1985-04-23` → `1985-**-**` |
| `fgac_mask_sort_code` | String → String | `20-00-00` → `**-**-00` |

Use them in Spark SQL directly (also applied automatically by the policy engine):

```sql
SELECT fgac_mask_account_number(account_number) AS account_number
FROM nessie.silver.cleansed_transactions
```

---

## Adding a new engine adapter (future)

1. Create `fgac-trino-adapter` module depending on `fgac-core`
2. Implement `SystemAccessControl`:
    - `getRowFilter(context, table)` → call `policyCache.findMostPermissive()`,
      call `expressionBuilder.build()`, serialise to SQL string
    - `getColumnMask(context, table, col)` → call `columnPolicyEngine.project()`,
      return masking SQL expression
3. Register masking functions as `@ScalarFunction` classes delegating to
   `MaskingFunctionLibrary` static methods
4. No changes to `fgac-api`, `fgac-core`, `fgac-policy-store`, or `fgac-audit`

---

## Running tests

```bash
# Unit tests only (no external services needed)
mvn test -pl fgac-core,fgac-masking,fgac-audit,fgac-policy-store

# Integration tests (requires Docker for Testcontainers)
mvn verify -pl fgac-integration-tests
```