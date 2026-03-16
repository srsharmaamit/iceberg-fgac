package com.banking.lakehouse.fgac.store.iceberg;

import com.banking.lakehouse.fgac.api.entitlement.ColumnPolicy;
import com.banking.lakehouse.fgac.api.entitlement.RowPolicy;
import com.banking.lakehouse.fgac.api.entitlement.TableEntitlement;
import com.banking.lakehouse.fgac.api.exception.FgacException.FgacPolicyStoreException;
import com.banking.lakehouse.fgac.api.identity.UserIdentity;
import com.banking.lakehouse.fgac.api.policy.EntitlementPolicyStore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Policy store backed by an Iceberg table in Nessie.
 *
 * <p>Table: {@code nessie.entitlements.rules}
 *
 * <h3>Schema</h3>
 * <pre>
 *   role_name         STRING NOT NULL
 *   table_name        STRING NOT NULL   -- fully qualified: namespace.table
 *   row_predicate     STRING NOT NULL   -- "ALLOW_ALL" | "DENY_ALL" | SQL predicate
 *   allowed_columns   STRING            -- JSON array, null = all columns
 *   masked_columns    STRING            -- JSON object: {"col": "mask_fn_id"}
 *   redacted_columns  STRING            -- JSON array
 *   audit_enabled     BOOLEAN
 *   effective_from    TIMESTAMPTZ
 *   effective_to      TIMESTAMPTZ       -- null = no expiry
 *   description       STRING
 * </pre>
 *
 * <p>On first startup the {@link SeedPolicyLoader} creates this table and
 * populates it from the seed JSON file if the table does not exist.
 *
 * <p>All reads use {@link IcebergGenerics} — no Spark dependency.
 * The Iceberg Java API reads the Parquet data files directly from MinIO
 * via the S3A connector configured in the Iceberg catalog.
 */
@Component
public class IcebergPolicyStore implements EntitlementPolicyStore {

    private static final Logger log = LoggerFactory.getLogger(IcebergPolicyStore.class);

    public static final String NAMESPACE  = "entitlements";
    public static final String TABLE_NAME = "rules";

    private final Catalog catalog;
    private final ObjectMapper objectMapper;
    private final String catalogName;

    // In-memory snapshot: role::table → entitlement
    private volatile Map<String, TableEntitlement> snapshot = new ConcurrentHashMap<>();
    private volatile boolean healthy = false;

    public IcebergPolicyStore(
            Catalog catalog,
            ObjectMapper objectMapper,
            @Value("${fgac.nessie.catalog.name:nessie}") String catalogName) {
        this.catalog      = catalog;
        this.objectMapper = objectMapper;
        this.catalogName  = catalogName;
    }

    @PostConstruct
    public void load() {
        log.info("Loading entitlement policy from Iceberg table {}.{}.{}",
                catalogName, NAMESPACE, TABLE_NAME);
        try {
            Map<String, TableEntitlement> loaded = readAllFromTable();
            this.snapshot = new ConcurrentHashMap<>(loaded);
            this.healthy  = true;
            log.info("Policy store loaded: {} entitlements", loaded.size());
        } catch (Exception e) {
            this.healthy = false;
            throw new FgacPolicyStoreException(
                    "Failed to load entitlement policy from Iceberg table: " + e.getMessage(), e);
        }
    }

    /** Reload from the latest Iceberg snapshot. Called by PolicyCacheManager on TTL expiry. */
    public void reload() {
        log.debug("Reloading policy store from Iceberg snapshot");
        try {
            Map<String, TableEntitlement> reloaded = readAllFromTable();
            this.snapshot = new ConcurrentHashMap<>(reloaded);
            this.healthy  = true;
            log.info("Policy store reloaded: {} entitlements", reloaded.size());
        } catch (Exception e) {
            log.error("Policy reload failed — retaining stale cache: {}", e.getMessage());
        }
    }

    // ── EntitlementPolicyStore implementation ─────────────────────────────

    @Override
    public TableEntitlement find(String roleName, String tableName) {
        return snapshot.getOrDefault(
                cacheKey(roleName, tableName),
                TableEntitlement.DEFAULT_DENY);
    }

    @Override
    public TableEntitlement findMostPermissive(UserIdentity identity, String tableName) {
        return identity.getRoles().stream()
                .map(role -> find(role, tableName))
                .filter(e -> !e.isDefaultDeny() && e.isActive())
                .findFirst()
                .orElse(TableEntitlement.DEFAULT_DENY);
    }

    @Override
    public List<TableEntitlement> findAll() {
        return new ArrayList<>(snapshot.values());
    }

    @Override
    public List<TableEntitlement> findByRole(String roleName) {
        return snapshot.entrySet().stream()
                .filter(e -> e.getKey().startsWith(roleName + "::"))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    @Override
    public List<TableEntitlement> findByTable(String tableName) {
        return snapshot.entrySet().stream()
                .filter(e -> e.getKey().endsWith("::" + tableName))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    @Override
    public boolean isHealthy() { return healthy; }

    @Override
    public String storeType() { return "IcebergPolicyStore[nessie.entitlements.rules]"; }

    // ── Private read logic ────────────────────────────────────────────────

    private Map<String, TableEntitlement> readAllFromTable() {
        TableIdentifier tableId = TableIdentifier.of(NAMESPACE, TABLE_NAME);
        Table table = catalog.loadTable(tableId);

        Map<String, TableEntitlement> result = new ConcurrentHashMap<>();

        try (CloseableIterable<Record> records = IcebergGenerics.read(table).build()) {
            for (Record record : records) {
                try {
                    TableEntitlement entitlement = fromRecord(record);
                    result.put(
                            cacheKey(entitlement.getRoleName(), entitlement.getTableName()),
                            entitlement);
                } catch (Exception e) {
                    log.warn("Skipping malformed policy record: {}", e.getMessage());
                }
            }
        } catch (Exception e) {
            throw new FgacPolicyStoreException("Error reading policy table: " + e.getMessage(), e);
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private TableEntitlement fromRecord(Record record) throws Exception {
        String roleName    = (String) record.getField("role_name");
        String tableName   = (String) record.getField("table_name");
        String rowPred     = (String) record.getField("row_predicate");
        String allowedJson = (String) record.getField("allowed_columns");
        String maskedJson  = (String) record.getField("masked_columns");
        String redactJson  = (String) record.getField("redacted_columns");
        Boolean audit      = (Boolean) record.getField("audit_enabled");
        Object effFrom     = record.getField("effective_from");
        Object effTo       = record.getField("effective_to");
        String desc        = (String) record.getField("description");

        RowPolicy rowPolicy = RowPolicy.of(rowPred != null ? rowPred : "DENY_ALL", desc);

        List<String> allowed   = allowedJson != null
                ? objectMapper.readValue(allowedJson, objectMapper.getTypeFactory()
                        .constructCollectionType(List.class, String.class))
                : List.of();
        Map<String, String> masked = maskedJson != null
                ? objectMapper.readValue(maskedJson, objectMapper.getTypeFactory()
                        .constructMapType(Map.class, String.class, String.class))
                : Map.of();
        List<String> redacted  = redactJson != null
                ? objectMapper.readValue(redactJson, objectMapper.getTypeFactory()
                        .constructCollectionType(List.class, String.class))
                : List.of();

        ColumnPolicy colPolicy = new ColumnPolicy(allowed, masked, redacted, desc);

        return TableEntitlement.builder(roleName, tableName)
                .rowPolicy(rowPolicy)
                .columnPolicy(colPolicy)
                .auditEnabled(audit != null ? audit : true)
                .effectiveFrom(effFrom != null ? Instant.ofEpochMilli((Long) effFrom) : Instant.EPOCH)
                .effectiveTo(effTo != null ? Instant.ofEpochMilli((Long) effTo) : null)
                .build();
    }

    private String cacheKey(String role, String table) { return role + "::" + table; }
}
