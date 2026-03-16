package com.banking.lakehouse.fgac.audit.sink;

import com.banking.lakehouse.fgac.api.audit.AuditEvent;

import java.util.List;

/**
 * SPI for persisting a batch of {@link AuditEvent} records.
 *
 * <p>Called by {@link com.banking.lakehouse.fgac.audit.emitter.AsyncAuditEventEmitter}
 * from its background drainer thread. Implementations must be thread-safe
 * (the drainer is single-threaded, but flush() may be called concurrently
 * on shutdown).
 *
 * <p>Known implementations:
 * <ul>
 *   <li>{@link IcebergAuditSink} — primary, appends to Iceberg table via
 *       Iceberg Java API (no Spark dependency)</li>
 *   <li>{@link LoggingAuditSink} — fallback, writes JSON to SLF4J logger</li>
 * </ul>
 */
public interface AuditSink {

    /**
     * Writes a batch of audit events atomically.
     * Must not return until the events are durably persisted.
     *
     * @param events the batch to write, never null, never empty
     * @throws Exception if the write fails — the caller handles error recovery
     */
    void writeBatch(List<AuditEvent> events) throws Exception;

    /** True if the sink can currently accept writes. */
    boolean isHealthy();

    /** Human-readable sink description for logging. */
    String sinkType();
}