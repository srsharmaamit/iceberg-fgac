package com.banking.lakehouse.fgac.api.audit;

/**
 * SPI for emitting audit events in a non-blocking manner.
 *
 * <p>Implementations must never block the calling thread (the Catalyst
 * optimizer rule runs in the query planning hot path). The contract is
 * fire-and-forget: place the event on an internal queue and return
 * immediately. A background thread drains the queue and persists batches
 * to the Iceberg audit table.
 *
 * <p>If the queue is at capacity, implementations should log a warning
 * and drop the event rather than block or throw. Audit is critical for
 * compliance but must never degrade query latency.
 *
 * <h3>Known implementations</h3>
 * <ul>
 *   <li>{@code AsyncIcebergAuditEmitter} — primary, writes to
 *       {@code nessie.audit.query_log}</li>
 *   <li>{@code LoggingAuditEmitter} — fallback, writes to SLF4J logger.
 *       Used in tests and when the Iceberg sink is unavailable.</li>
 *   <li>{@code NoOpAuditEmitter} — for unit tests that don't care about audit.</li>
 *   <li>{@code CompositeAuditEmitter} — fans out to multiple emitters simultaneously.</li>
 * </ul>
 */
public interface AuditEventEmitter {

    /**
     * Emits an audit event. Must return immediately without blocking.
     *
     * @param event the event to emit, never null
     */
    void emit(AuditEvent event);

    /**
     * Flushes any buffered events to the backing sink.
     * Called at Spark session shutdown and during graceful pod termination.
     * May block briefly to complete the flush.
     */
    void flush();

    /**
     * Returns the number of events currently buffered but not yet persisted.
     * Used for health checks and backpressure monitoring.
     */
    int pendingCount();

    /**
     * Returns true if the emitter is operational and events are being persisted.
     */
    boolean isHealthy();

    /** Human-readable name of this emitter implementation. */
    String emitterType();
}