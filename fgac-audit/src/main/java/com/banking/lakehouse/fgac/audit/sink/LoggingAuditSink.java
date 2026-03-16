package com.banking.lakehouse.fgac.audit.sink;

import com.banking.lakehouse.fgac.api.audit.AuditEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Fallback audit sink that writes JSON-serialised audit events to SLF4J.
 *
 * <p>Used in:
 * <ul>
 *   <li>Unit tests (no Iceberg catalog available)</li>
 *   <li>Local IDE runs where MinIO may not be started</li>
 *   <li>Failover when the {@code IcebergAuditSink} reports unhealthy</li>
 * </ul>
 *
 * <p>In production, configure a log appender that ships these records to
 * your SIEM or centralised log store (e.g. Splunk, Elastic) as a backup
 * to the Iceberg table.
 */
@Component("loggingAuditSink")
public class LoggingAuditSink implements AuditSink {

    private static final Logger auditLog =
            LoggerFactory.getLogger("com.banking.lakehouse.fgac.AUDIT");

    private final ObjectMapper objectMapper;

    public LoggingAuditSink(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public void writeBatch(List<AuditEvent> events) {
        for (AuditEvent event : events) {
            try {
                String json = objectMapper.writeValueAsString(event);
                auditLog.info("AUDIT_EVENT {}", json);
            } catch (Exception e) {
                auditLog.error("Failed to serialise audit event {}: {}",
                        event.getEventId(), e.getMessage());
            }
        }
    }

    @Override public boolean isHealthy() { return true; }
    @Override public String sinkType()   { return "LoggingAuditSink"; }
}