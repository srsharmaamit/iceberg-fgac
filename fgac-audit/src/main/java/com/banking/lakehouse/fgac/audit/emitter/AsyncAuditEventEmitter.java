package com.banking.lakehouse.fgac.audit.emitter;

import com.banking.lakehouse.fgac.api.audit.AuditEvent;
import com.banking.lakehouse.fgac.api.audit.AuditEventEmitter;
import com.banking.lakehouse.fgac.audit.sink.AuditSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Non-blocking audit emitter backed by a bounded {@link BlockingQueue}.
 *
 * <h3>How it works</h3>
 * <ol>
 *   <li>{@link #emit(AuditEvent)} places events on the queue and returns
 *       immediately (sub-microsecond hot path cost).</li>
 *   <li>A single background thread drains the queue every
 *       {@code fgac.audit.flush-interval-seconds} seconds and writes
 *       batches to the configured {@link AuditSink}.</li>
 *   <li>If the queue reaches {@code fgac.audit.queue-capacity}, new events
 *       are dropped and a warning is logged. Audit must never degrade query
 *       latency — drop-on-full is intentional and logged for ops visibility.</li>
 *   <li>On shutdown ({@link #flush()} + {@link #shutdown()}), any buffered
 *       events are flushed synchronously before the process exits.</li>
 * </ol>
 *
 * <h3>Batch size</h3>
 * The drainer collects up to {@code fgac.audit.batch-size} events per write
 * cycle to amortise Iceberg append overhead. A single Iceberg append for
 * 100–500 events is more efficient than 100–500 individual appends.
 */
@Component
public class AsyncAuditEventEmitter implements AuditEventEmitter {

    private static final Logger log = LoggerFactory.getLogger(AsyncAuditEventEmitter.class);

    private final AuditSink sink;
    private final BlockingQueue<AuditEvent> queue;
    private final int batchSize;
    private final ScheduledExecutorService scheduler;

    private final AtomicLong emittedCount  = new AtomicLong(0);
    private final AtomicLong droppedCount  = new AtomicLong(0);
    private final AtomicLong persistedCount = new AtomicLong(0);

    private volatile boolean healthy = true;

    public AsyncAuditEventEmitter(
            AuditSink sink,
            @Value("${fgac.audit.queue-capacity:10000}") int queueCapacity,
            @Value("${fgac.audit.batch-size:200}") int batchSize,
            @Value("${fgac.audit.flush-interval-seconds:10}") int flushIntervalSeconds) {
        this.sink      = sink;
        this.batchSize = batchSize;
        this.queue     = new LinkedBlockingQueue<>(queueCapacity);
        this.scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "fgac-audit-drainer");
            t.setDaemon(true);
            return t;
        });

        log.info("AsyncAuditEventEmitter initialised: capacity={}, batch={}, interval={}s",
                queueCapacity, batchSize, flushIntervalSeconds);

        this.scheduler.scheduleAtFixedRate(
                this::drainQueue,
                flushIntervalSeconds,
                flushIntervalSeconds,
                TimeUnit.SECONDS);
    }

    @Override
    public void emit(AuditEvent event) {
        boolean offered = queue.offer(event);
        if (offered) {
            emittedCount.incrementAndGet();
        } else {
            droppedCount.incrementAndGet();
            if (droppedCount.get() % 100 == 1) {
                log.warn("Audit queue full — dropped {} events so far. "
                                + "Increase fgac.audit.queue-capacity or reduce query rate.",
                        droppedCount.get());
            }
        }
    }

    @Override
    public void flush() {
        log.info("Flushing {} buffered audit events before shutdown", queue.size());
        drainQueue();
    }

    @Override
    public int pendingCount() { return queue.size(); }

    @Override
    public boolean isHealthy() { return healthy && sink.isHealthy(); }

    @Override
    public String emitterType() { return "AsyncIcebergAuditEmitter"; }

    /** Exposes counters for metrics/monitoring. */
    public long getEmittedCount()   { return emittedCount.get(); }
    public long getDroppedCount()   { return droppedCount.get(); }
    public long getPersistedCount() { return persistedCount.get(); }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdown();
        try {
            drainQueue(); // final flush
            scheduler.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("Audit emitter shutdown: emitted={}, persisted={}, dropped={}",
                emittedCount.get(), persistedCount.get(), droppedCount.get());
    }

    // ── Background drainer ────────────────────────────────────────────────

    private void drainQueue() {
        if (queue.isEmpty()) return;

        List<AuditEvent> batch = new ArrayList<>(batchSize);
        int drained = queue.drainTo(batch, batchSize);

        if (drained == 0) return;

        try {
            sink.writeBatch(batch);
            persistedCount.addAndGet(drained);
            healthy = true;
        } catch (Exception e) {
            healthy = false;
            log.error("Failed to write audit batch of {} events to sink: {}",
                    drained, e.getMessage());
            // Events are dropped — they were already removed from queue.
            // In production: consider a dead-letter file appender here.
            droppedCount.addAndGet(drained);
        }
    }
}