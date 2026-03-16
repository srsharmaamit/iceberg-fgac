package com.banking.lakehouse.fgac.api.audit;

import com.banking.lakehouse.fgac.api.entitlement.TableEntitlement;
import com.banking.lakehouse.fgac.api.identity.UserIdentity;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Immutable audit record emitted after every policy-enforced query.
 *
 * <p>Written asynchronously (non-blocking) to {@code nessie.audit.query_log}
 * via the {@link AuditEventEmitter}. Each record contains enough information
 * to answer a banking regulator's question: "what did user X see on table Y
 * on date Z, and what restrictions were in effect?"
 *
 * <p>Because audit records are written to an Iceberg table in Nessie, they are:
 * <ul>
 *   <li>Immutable (Iceberg append-only, no in-place updates)</li>
 *   <li>Time-travelable (query what the log contained at any past snapshot)</li>
 *   <li>Queryable (standard Spark SQL, retained for 7 years)</li>
 *   <li>Independently versioned (Nessie commit per batch append)</li>
 * </ul>
 */
public final class AuditEvent {

    public enum Operation { SCAN, WRITE, MERGE, DELETE, POLICY_LOAD, POLICY_CACHE_REFRESH }
    public enum Outcome   { ALLOWED, DENIED, PARTIAL }

    private final String    eventId;
    private final Instant   eventTs;
    private final String    principalId;
    private final String    engineName;        // "spark", "trino", "flink"
    private final String    tableName;
    private final String    nessieRef;         // active branch, null for HMS queries
    private final Operation operation;
    private final Outcome   outcome;
    private final String    rowFilterApplied;  // serialised predicate string
    private final String    columnPolicyApplied; // summary of column restrictions
    private final String    sparkAppId;        // nullable
    private final String    queryHash;         // SHA-256 of the logical plan string
    private final long      planningDurationMs;

    @JsonCreator
    private AuditEvent(
            @JsonProperty("eventId")              String    eventId,
            @JsonProperty("eventTs")              Instant   eventTs,
            @JsonProperty("principalId")          String    principalId,
            @JsonProperty("engineName")           String    engineName,
            @JsonProperty("tableName")            String    tableName,
            @JsonProperty("nessieRef")            String    nessieRef,
            @JsonProperty("operation")            Operation operation,
            @JsonProperty("outcome")              Outcome   outcome,
            @JsonProperty("rowFilterApplied")     String    rowFilterApplied,
            @JsonProperty("columnPolicyApplied")  String    columnPolicyApplied,
            @JsonProperty("sparkAppId")           String    sparkAppId,
            @JsonProperty("queryHash")            String    queryHash,
            @JsonProperty("planningDurationMs")   long      planningDurationMs) {
        this.eventId             = eventId;
        this.eventTs             = eventTs;
        this.principalId         = principalId;
        this.engineName          = engineName;
        this.tableName           = tableName;
        this.nessieRef           = nessieRef;
        this.operation           = operation;
        this.outcome             = outcome;
        this.rowFilterApplied    = rowFilterApplied;
        this.columnPolicyApplied = columnPolicyApplied;
        this.sparkAppId          = sparkAppId;
        this.queryHash           = queryHash;
        this.planningDurationMs  = planningDurationMs;
    }

    // getters
    public String    getEventId()             { return eventId; }
    public Instant   getEventTs()             { return eventTs; }
    public String    getPrincipalId()         { return principalId; }
    public String    getEngineName()          { return engineName; }
    public String    getTableName()           { return tableName; }
    public String    getNessieRef()           { return nessieRef; }
    public Operation getOperation()           { return operation; }
    public Outcome   getOutcome()             { return outcome; }
    public String    getRowFilterApplied()    { return rowFilterApplied; }
    public String    getColumnPolicyApplied() { return columnPolicyApplied; }
    public String    getSparkAppId()          { return sparkAppId; }
    public String    getQueryHash()           { return queryHash; }
    public long      getPlanningDurationMs()  { return planningDurationMs; }

    // ── Builder ──────────────────────────────────────────────────────────

    public static Builder builder() { return new Builder(); }

    /** Convenience factory: build from resolved identity + entitlement. */
    public static Builder fromEntitlement(
            UserIdentity identity,
            TableEntitlement entitlement,
            String engineName) {
        return builder()
                .principalId(identity.getPrincipalId())
                .engineName(engineName)
                .tableName(entitlement.getTableName())
                .rowFilterApplied(entitlement.getRowPolicy().getPredicate())
                .columnPolicyApplied(entitlement.getColumnPolicy().toString())
                .outcome(entitlement.isDefaultDeny() ? Outcome.DENIED : Outcome.ALLOWED);
    }

    public static final class Builder {
        private String    eventId            = UUID.randomUUID().toString();
        private Instant   eventTs            = Instant.now();
        private String    principalId        = "UNKNOWN";
        private String    engineName         = "unknown";
        private String    tableName          = "UNKNOWN";
        private String    nessieRef;
        private Operation operation          = Operation.SCAN;
        private Outcome   outcome            = Outcome.ALLOWED;
        private String    rowFilterApplied   = "ALLOW_ALL";
        private String    columnPolicyApplied = "ALLOW_ALL";
        private String    sparkAppId;
        private String    queryHash;
        private long      planningDurationMs  = 0;

        public Builder eventId(String v)             { this.eventId = v;            return this; }
        public Builder eventTs(Instant v)            { this.eventTs = v;            return this; }
        public Builder principalId(String v)         { this.principalId = v;        return this; }
        public Builder engineName(String v)          { this.engineName = v;         return this; }
        public Builder tableName(String v)           { this.tableName = v;          return this; }
        public Builder nessieRef(String v)           { this.nessieRef = v;          return this; }
        public Builder operation(Operation v)        { this.operation = v;          return this; }
        public Builder outcome(Outcome v)            { this.outcome = v;            return this; }
        public Builder rowFilterApplied(String v)    { this.rowFilterApplied = v;   return this; }
        public Builder columnPolicyApplied(String v) { this.columnPolicyApplied = v;return this; }
        public Builder sparkAppId(String v)          { this.sparkAppId = v;         return this; }
        public Builder queryHash(String v)           { this.queryHash = v;          return this; }
        public Builder planningDurationMs(long v)    { this.planningDurationMs = v; return this; }

        public AuditEvent build() {
            return new AuditEvent(eventId, eventTs, principalId, engineName,
                    tableName, nessieRef, operation, outcome,
                    rowFilterApplied, columnPolicyApplied,
                    sparkAppId, queryHash, planningDurationMs);
        }
    }

    @Override public String toString() {
        return "AuditEvent{id='" + eventId + "', user='" + principalId
                + "', table='" + tableName + "', outcome=" + outcome + '}';
    }
}