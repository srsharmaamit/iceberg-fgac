package com.banking.lakehouse.fgac.audit.emitter;

import com.banking.lakehouse.fgac.api.audit.AuditEvent;
import com.banking.lakehouse.fgac.audit.sink.AuditSink;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.*;

/*
 * FIX 1: Use LENIENT strictness so that when(sink.isHealthy()) stubs
 * defined only in specific tests do not trigger UnnecessaryStubbingException
 * in tests that never call isHealthy().
 *
 * Alternative would be to move the stub into each individual test that
 * needs it — LENIENT is simpler here because isHealthy() is called by
 * the emitter's own background thread, not always from the test body.
 */
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
@DisplayName("AsyncAuditEventEmitter")
class AsyncAuditEventEmitterTest {

    @Mock
    private AuditSink sink;

    private AsyncAuditEventEmitter emitter;

    @BeforeEach
    void setUp() {
        /*
         * FIX 2: isHealthy() stub stays here — LENIENT mode means tests that
         * never call it will not fail. Tests that DO call it get the right answer.
         */
        when(sink.isHealthy()).thenReturn(true);
        when(sink.sinkType()).thenReturn("MockSink");
        emitter = new AsyncAuditEventEmitter(sink, 100, 10, 1);
    }

    @AfterEach
    void tearDown() {
        // Only shut down if it was not already shut down inside the test
        if (emitter != null) {
            emitter.shutdown();
        }
    }

    @Test
    @DisplayName("emit() returns immediately without blocking")
    void emitNonBlocking() {
        AuditEvent event = sampleEvent();
        long start = System.currentTimeMillis();
        emitter.emit(event);
        long elapsed = System.currentTimeMillis() - start;

        assertThat(elapsed).isLessThan(10);
        assertThat(emitter.getEmittedCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("events are delivered to sink within flush interval")
    void eventDeliveredToSink() throws Exception {
        emitter.emit(sampleEvent());
        emitter.emit(sampleEvent());

        // Flush interval is 1s in test; wait up to 3s for delivery
        await().atMost(3, TimeUnit.SECONDS)
                .until(() -> emitter.getPersistedCount() >= 2);

        verify(sink, atLeastOnce()).writeBatch(anyList());
    }

    @Test
    @DisplayName("flush() synchronously drains remaining events")
    void flushDrainsQueue() throws Exception {
        for (int i = 0; i < 5; i++) emitter.emit(sampleEvent());
        emitter.flush();

        verify(sink, atLeastOnce()).writeBatch(anyList());
    }

    @Test
    @DisplayName("events dropped when queue is full — never blocks")
    void queueFullDropsWithoutBlocking() throws Exception {
        /*
         * FIX 3: The hanging sink previously used Thread.sleep(60_000).
         * When @AfterEach called emitter.shutdown() -> drainQueue() ->
         * sink.writeBatch(), the test hung for 60 seconds.
         *
         * Solution: use an interruptible sink + shut down this emitter
         * explicitly inside the test before @AfterEach fires.
         */
        AtomicBoolean interrupted = new AtomicBoolean(false);
        AuditSink interruptibleSink = new AuditSink() {
            @Override
            public void writeBatch(List<AuditEvent> events) throws Exception {
                try {
                    Thread.sleep(30_000);
                } catch (InterruptedException e) {
                    interrupted.set(true);
                    Thread.currentThread().interrupt();
                }
            }
            @Override public boolean isHealthy() { return true; }
            @Override public String sinkType()   { return "InterruptibleHangingSink"; }
        };

        // Tiny queue (capacity 5) so it fills fast
        AsyncAuditEventEmitter tinyEmitter =
                new AsyncAuditEventEmitter(interruptibleSink, 5, 10, 60);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 20; i++) tinyEmitter.emit(sampleEvent());
        long elapsed = System.currentTimeMillis() - start;

        // emit() must never block — should complete in well under 200ms
        assertThat(elapsed).isLessThan(200);

        // Some events must have been dropped (queue only holds 5)
        assertThat(tinyEmitter.getDroppedCount()).isGreaterThan(0);

        // Shut down explicitly here so tearDown() doesn't double-shutdown
        // The scheduler interrupt will wake the sleeping sink thread
        tinyEmitter.shutdown();
        emitter = null; // prevent tearDown() from calling shutdown again
    }

    @Test
    @DisplayName("sink failure marks emitter unhealthy but does not throw to caller")
    void sinkFailureHandled() throws Exception {
        doThrow(new RuntimeException("Iceberg unavailable"))
                .when(sink).writeBatch(anyList());

        emitter.emit(sampleEvent());
        emitter.flush();

        assertThat(emitter.isHealthy()).isFalse();
        // No exception propagated to the test — emit() is fire-and-forget
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private AuditEvent sampleEvent() {
        return AuditEvent.builder()
                .principalId("svc_airflow")
                .engineName("spark")
                .tableName("silver.cleansed_transactions")
                .rowFilterApplied("jurisdiction = 'UK'")
                .outcome(AuditEvent.Outcome.ALLOWED)
                .build();
    }
}