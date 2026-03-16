package com.banking.lakehouse.fgac.audit.sink;

import com.banking.lakehouse.fgac.api.audit.AuditEvent;
import jakarta.annotation.PostConstruct;
import org.apache.iceberg.*;
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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Production audit sink: appends batches of {@link AuditEvent} records
 * to {@code nessie.audit.query_log} using the Iceberg Java API directly.
 *
 * <h3>No Spark dependency</h3>
 * This sink uses {@code IcebergGenerics} and {@code Parquet.write()} —
 * the same low-level API used by SeedPolicyLoader.
 * There is no Spark session, no DataFrame, no SparkContext involved.
 * This makes it safe to use from any engine (Trino adapter, CLI) without
 * a Spark dependency on the classpath.
 *
 * <h3>Iceberg append guarantees</h3>
 * Each call to {@link #writeBatch(List)} performs one atomic Iceberg
 * {@code AppendFiles} commit. If the commit fails, the data file is
 * orphaned in the object store but the table state is unchanged.
 * The {@link com.banking.lakehouse.fgac.audit.emitter.AsyncAuditEventEmitter}
 * handles the failure by logging and dropping — audit events are not
 * retried to avoid double-counting in the audit log.
 *
 * <h3>Table schema</h3>
 * Created on first startup if absent, partitioned by day(event_ts)
 * for efficient time-range queries by the compliance team.
 */
@Component("icebergAuditSink")
public class IcebergAuditSink implements AuditSink {

    private static final Logger log = LoggerFactory.getLogger(IcebergAuditSink.class);

    static final String AUDIT_NAMESPACE = "audit";
    static final String AUDIT_TABLE = "query_log";

    static final Schema AUDIT_SCHEMA = new Schema(
            Types.NestedField.required(1, "event_id", Types.StringType.get()),
            Types.NestedField.required(2, "event_ts", Types.LongType.get()),   // epoch ms
            Types.NestedField.required(3, "principal_id", Types.StringType.get()),
            Types.NestedField.required(4, "engine_name", Types.StringType.get()),
            Types.NestedField.required(5, "table_name", Types.StringType.get()),
            Types.NestedField.optional(6, "nessie_ref", Types.StringType.get()),
            Types.NestedField.required(7, "operation", Types.StringType.get()),
            Types.NestedField.required(8, "outcome", Types.StringType.get()),
            Types.NestedField.optional(9, "row_filter_applied", Types.StringType.get()),
            Types.NestedField.optional(10, "column_policy_applied", Types.StringType.get()),
            Types.NestedField.optional(11, "spark_app_id", Types.StringType.get()),
            Types.NestedField.optional(12, "query_hash", Types.StringType.get()),
            Types.NestedField.optional(13, "planning_duration_ms", Types.LongType.get())
    );

    private final Catalog catalog;
    private volatile boolean healthy = false;
    private volatile Table auditTable;

    public IcebergAuditSink(
            Catalog catalog,
            @Value("${fgac.audit.retention.days:2555}") int retentionDays) {
        this.catalog = catalog;
    }

    @PostConstruct
    public void initialise() {
        TableIdentifier tableId = TableIdentifier.of(AUDIT_NAMESPACE, AUDIT_TABLE);
        try {
            Namespace ns = Namespace.of(AUDIT_NAMESPACE);
            if (catalog instanceof SupportsNamespaces nsSupport) {
                if (!nsSupport.namespaceExists(ns)) {
                    nsSupport.createNamespace(ns);
                    log.info("Created audit namespace");
                }
            }
            if (!catalog.tableExists(tableId)) {
                auditTable = catalog.createTable(
                        tableId,
                        AUDIT_SCHEMA,
                        PartitionSpec.builderFor(AUDIT_SCHEMA)
                                .day("event_ts")
                                .build(),
                        java.util.Map.of(
                                "write.parquet.compression-codec", "snappy",
                                // Retain snapshots for 7 years (banking compliance)
                                "history.expire.max-snapshot-age-ms",
                                String.valueOf(2555L * 24 * 60 * 60 * 1000)
                        ));
                log.info("Created audit.query_log Iceberg table");
            } else {
                auditTable = catalog.loadTable(tableId);
                log.info("Loaded existing audit.query_log table");
            }
            healthy = true;
        } catch (Exception e) {
            healthy = false;
            log.error("Failed to initialise IcebergAuditSink — "
                    + "falling back to LoggingAuditSink: {}", e.getMessage());
        }
    }

    @Override
    public void writeBatch(List<AuditEvent> events) throws Exception {
        if (!healthy || auditTable == null) {
            throw new IllegalStateException("IcebergAuditSink is not healthy");
        }

        List<Record> records = toRecords(events);
        String fileLocation = auditTable.location()
                + "/data/audit-" + UUID.randomUUID() + ".parquet";
        OutputFile outFile = auditTable.io().newOutputFile(fileLocation);

        FileAppender<Record> appender = Parquet.write(outFile)
                .schema(AUDIT_SCHEMA)
                .createWriterFunc(GenericParquetWriter::create)
                .build();

        try (appender) {
            appender.addAll(records);
        }

        DataFile dataFile = DataFiles.builder(auditTable.spec())
                .withInputFile(outFile.toInputFile())
                .withMetrics(appender.metrics())
                .withFormat(FileFormat.PARQUET)
                .build();

        auditTable.newAppend()
                .appendFile(dataFile)
                .commit();

        log.debug("Audit batch of {} events committed to Iceberg", events.size());
    }

    @Override
    public boolean isHealthy() {
        return healthy;
    }

    @Override
    public String sinkType() {
        return "IcebergAuditSink[nessie.audit.query_log]";
    }

    // ── Record conversion ─────────────────────────────────────────────────

    private List<Record> toRecords(List<AuditEvent> events) {
        List<Record> records = new ArrayList<>(events.size());
        for (AuditEvent e : events) {
            GenericRecord rec = GenericRecord.create(AUDIT_SCHEMA);
            rec.setField("event_id", e.getEventId());
            rec.setField("event_ts", e.getEventTs().toEpochMilli());
            rec.setField("principal_id", e.getPrincipalId());
            rec.setField("engine_name", e.getEngineName());
            rec.setField("table_name", e.getTableName());
            rec.setField("nessie_ref", e.getNessieRef());
            rec.setField("operation", e.getOperation().name());
            rec.setField("outcome", e.getOutcome().name());
            rec.setField("row_filter_applied", e.getRowFilterApplied());
            rec.setField("column_policy_applied", e.getColumnPolicyApplied());
            rec.setField("spark_app_id", e.getSparkAppId());
            rec.setField("query_hash", e.getQueryHash());
            rec.setField("planning_duration_ms", e.getPlanningDurationMs());
            records.add(rec);
        }
        return records;
    }
}
