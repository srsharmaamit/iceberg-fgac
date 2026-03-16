package com.banking.lakehouse.fgac.store.seed;

import com.banking.lakehouse.fgac.api.entitlement.TableEntitlement;
import com.banking.lakehouse.fgac.api.exception.FgacException.FgacPolicyStoreException;
import com.banking.lakehouse.fgac.store.iceberg.IcebergPolicyStore;
import com.banking.lakehouse.fgac.store.json.JsonPolicyReader;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Bootstraps the {@code nessie.entitlements.rules} Iceberg table on first startup.
 *
 * Reads the seed policy from a JSON file, creates the table if it does not exist,
 * and inserts the seed rows. On subsequent startups the table already exists
 * and this class does nothing (idempotent).
 *
 * Fixes applied vs original:
 *   1. NAMESPACE/TABLE_NAME now public in IcebergPolicyStore — accessible cross-package.
 *   2. Namespace operations use SupportsNamespaces cast, not base Catalog interface.
 *   3. toDataFile(PartitionSpec, null) removed in Iceberg 1.4+.
 *      Replaced with DataFiles.builder() using appender.metrics().
 *   4. iceberg-parquet added as explicit dependency in fgac-policy-store/pom.xml.
 */
@Component
public class SeedPolicyLoader {

    private static final Logger log = LoggerFactory.getLogger(SeedPolicyLoader.class);

    public static final Schema POLICY_SCHEMA = new Schema(
            Types.NestedField.required(1,  "role_name",        Types.StringType.get()),
            Types.NestedField.required(2,  "table_name",       Types.StringType.get()),
            Types.NestedField.required(3,  "row_predicate",    Types.StringType.get()),
            Types.NestedField.optional(4,  "allowed_columns",  Types.StringType.get()),
            Types.NestedField.optional(5,  "masked_columns",   Types.StringType.get()),
            Types.NestedField.optional(6,  "redacted_columns", Types.StringType.get()),
            Types.NestedField.optional(7,  "audit_enabled",    Types.BooleanType.get()),
            Types.NestedField.optional(8,  "effective_from",   Types.LongType.get()),
            Types.NestedField.optional(9,  "effective_to",     Types.LongType.get()),
            Types.NestedField.optional(10, "description",      Types.StringType.get())
    );

    private final Catalog catalog;
    private final JsonPolicyReader jsonReader;
    private final ObjectMapper objectMapper;
    private final String seedPath;

    public SeedPolicyLoader(
            Catalog catalog,
            JsonPolicyReader jsonReader,
            ObjectMapper objectMapper,
            @Value("${fgac.policy.seed.path:classpath:/config/policies/seed-policies.json}")
            String seedPath) {
        this.catalog      = catalog;
        this.jsonReader   = jsonReader;
        this.objectMapper = objectMapper;
        this.seedPath     = seedPath;
    }

    public void seedIfAbsent() {
        TableIdentifier tableId = TableIdentifier.of(
                IcebergPolicyStore.NAMESPACE,
                IcebergPolicyStore.TABLE_NAME);

        // FIX 1: Namespace operations live on SupportsNamespaces, not Catalog
        Namespace ns = Namespace.of(IcebergPolicyStore.NAMESPACE);
        if (catalog instanceof SupportsNamespaces nsSupport) {
            if (!nsSupport.namespaceExists(ns)) {
                nsSupport.createNamespace(ns);
                log.info("Created namespace: {}", ns);
            }
        } else {
            log.warn("Catalog does not implement SupportsNamespaces — skipping namespace check");
        }

        if (catalog.tableExists(tableId)) {
            log.info("entitlements.rules already exists — skipping seed");
            return;
        }

        log.info("Creating entitlements.rules and seeding from: {}", seedPath);
        Table table = catalog.createTable(
                tableId, POLICY_SCHEMA, PartitionSpec.unpartitioned(),
                Map.of("write.parquet.compression-codec", "snappy"));

        List<TableEntitlement> seeds = loadSeedEntitlements();
        if (seeds.isEmpty()) {
            log.warn("Seed file had no entitlements — table created but empty");
            return;
        }
        writeSeedRows(table, seeds);
        log.info("Seeded {} entitlement rules", seeds.size());
    }

    private List<TableEntitlement> loadSeedEntitlements() {
        try {
            if (seedPath.startsWith("classpath:")) {
                String resource = seedPath.substring("classpath:".length());
                try (InputStream is = getClass().getResourceAsStream(resource)) {
                    if (is == null) {
                        log.warn("Seed file not found on classpath: {}", resource);
                        return List.of();
                    }
                    return jsonReader.readAll(is);
                }
            }
            return jsonReader.readAll(java.nio.file.Path.of(seedPath));
        } catch (IOException e) {
            log.error("Failed to load seed policies: {}", e.getMessage());
            return List.of();
        }
    }

    private void writeSeedRows(Table table, List<TableEntitlement> entitlements) {
        List<Record> records = buildRecords(entitlements);
        String fileLocation = table.location() + "/data/seed-" + UUID.randomUUID() + ".parquet";
        OutputFile outFile = table.io().newOutputFile(fileLocation);

        try {
            FileAppender<Record> appender = Parquet.write(outFile)
                    .schema(POLICY_SCHEMA)
                    .createWriterFunc(GenericParquetWriter::create)
                    .build();
            try (appender) {
                appender.addAll(records);
            }

            // FIX 2: toDataFile(PartitionSpec, null) removed in Iceberg 1.4+.
            // Correct API: DataFiles.builder() with appender.metrics() and .withInputFile()
            DataFile dataFile = DataFiles.builder(PartitionSpec.unpartitioned())
                    .withInputFile(outFile.toInputFile())
                    .withMetrics(appender.metrics())
                    .withFormat(FileFormat.PARQUET)
                    .build();

            table.newAppend()
                    .appendFile(dataFile)
                    .commit();

        } catch (IOException e) {
            throw new FgacPolicyStoreException(
                    "Failed to write seed rows: " + e.getMessage(), e);
        }
    }

    private List<Record> buildRecords(List<TableEntitlement> entitlements) {
        List<Record> records = new ArrayList<>(entitlements.size());
        for (TableEntitlement e : entitlements) {
            GenericRecord rec = GenericRecord.create(POLICY_SCHEMA);
            rec.setField("role_name",     e.getRoleName());
            rec.setField("table_name",    e.getTableName());
            rec.setField("row_predicate", e.getRowPolicy().getPredicate());
            rec.setField("audit_enabled", e.isAuditEnabled());
            rec.setField("effective_from", e.getEffectiveFrom().toEpochMilli());
            rec.setField("effective_to",
                    e.getEffectiveTo() != null ? e.getEffectiveTo().toEpochMilli() : null);
            rec.setField("description", e.getRowPolicy().getDescription());
            try {
                if (!e.getColumnPolicy().getAllowedColumns().isEmpty()) {
                    rec.setField("allowed_columns",
                            objectMapper.writeValueAsString(e.getColumnPolicy().getAllowedColumns()));
                }
                if (!e.getColumnPolicy().getMaskedColumns().isEmpty()) {
                    rec.setField("masked_columns",
                            objectMapper.writeValueAsString(e.getColumnPolicy().getMaskedColumns()));
                }
                if (!e.getColumnPolicy().getRedactedColumns().isEmpty()) {
                    rec.setField("redacted_columns",
                            objectMapper.writeValueAsString(e.getColumnPolicy().getRedactedColumns()));
                }
            } catch (Exception ex) {
                log.warn("Column policy serialisation failed for {}/{}: {}",
                        e.getRoleName(), e.getTableName(), ex.getMessage());
            }
            records.add(rec);
        }
        return records;
    }
}