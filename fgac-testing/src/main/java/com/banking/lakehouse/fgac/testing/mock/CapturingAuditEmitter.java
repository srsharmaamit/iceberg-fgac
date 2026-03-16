package com.banking.lakehouse.fgac.testing.mock;

import com.banking.lakehouse.fgac.api.audit.AuditEvent;
import com.banking.lakehouse.fgac.api.audit.AuditEventEmitter;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Test-only {@link AuditEventEmitter} that captures emitted events
 * in memory for assertion in tests, with zero I/O overhead.
 *
 * <p>Usage:
 * <pre>
 *   CapturingAuditEmitter emitter = new CapturingAuditEmitter();
 *   // ... run code under test ...
 *   assertThat(emitter.captured()).hasSize(1);
 *   assertThat(emitter.captured().get(0).getOutcome())
 *       .isEqualTo(AuditEvent.Outcome.DENIED);
 * </pre>
 *
 * <p>Also serves as a no-op emitter when constructed and never queried:
 * <pre>
 *   new CapturingAuditEmitter()  // silently discards all events
 * </pre>
 */
public class CapturingAuditEmitter implements AuditEventEmitter {

    private final List<AuditEvent> events = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void emit(AuditEvent event) {
        events.add(event);
    }

    @Override
    public void flush() { /* no-op */ }

    @Override
    public int pendingCount() { return 0; }

    @Override
    public boolean isHealthy() { return true; }

    @Override
    public String emitterType() { return "CapturingAuditEmitter[test]"; }

    /** Returns an unmodifiable snapshot of captured events. */
    public List<AuditEvent> captured() {
        return Collections.unmodifiableList(new ArrayList<>(events));
    }

    /** Clears all captured events. Useful between test cases. */
    public void reset() { events.clear(); }

    /** Returns the last captured event, or throws if none. */
    public AuditEvent last() {
        if (events.isEmpty()) throw new AssertionError("No audit events were emitted");
        return events.get(events.size() - 1);
    }
}