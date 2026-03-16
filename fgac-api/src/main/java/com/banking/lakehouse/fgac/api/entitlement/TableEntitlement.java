package com.banking.lakehouse.fgac.api.entitlement;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * Top-level entitlement: the combined row and column policy
 * for a specific (role, tableName) pair.
 *
 * <p>This is what the {@code EntitlementPolicyStore} returns and what
 * the {@code IcebergExpressionBuilder} and {@code ColumnPolicyEngine}
 * consume. Immutable after construction.
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>An operator writes a {@code TableEntitlement} row into the
 *       {@code nessie.entitlements.rules} Iceberg table.</li>
 *   <li>The {@code PolicyCacheManager} loads it at startup (and every 5 min TTL).</li>
 *   <li>At query planning time the Catalyst rule calls
 *       {@code policyStore.find(role, tableName)} → {@code TableEntitlement}.</li>
 *   <li>The row policy predicate feeds {@code IcebergExpressionBuilder.build()}.</li>
 *   <li>The column policy feeds {@code ColumnPolicyEngine.project()} and
 *       {@code SparkUdfRegistrar.wrapMaskedColumns()}.</li>
 *   <li>An {@code AuditEvent} is emitted recording the applied entitlement.</li>
 * </ol>
 */
public final class TableEntitlement {

    /** Sentinel returned when no matching policy row exists. Denies everything. */
    public static final TableEntitlement DEFAULT_DENY = new TableEntitlement(
            "UNKNOWN", "UNKNOWN", RowPolicy.DENY_ALL, ColumnPolicy.DENY_ALL,
            false, Instant.EPOCH, null);

    private final String roleName;
    private final String tableName;       // fully qualified: namespace.table
    private final RowPolicy rowPolicy;
    private final ColumnPolicy columnPolicy;
    private final boolean auditEnabled;
    private final Instant effectiveFrom;
    private final Instant effectiveTo;    // null = no expiry

    @JsonCreator
    public TableEntitlement(
            @JsonProperty("roleName")      String roleName,
            @JsonProperty("tableName")     String tableName,
            @JsonProperty("rowPolicy")     RowPolicy rowPolicy,
            @JsonProperty("columnPolicy")  ColumnPolicy columnPolicy,
            @JsonProperty("auditEnabled")  boolean auditEnabled,
            @JsonProperty("effectiveFrom") Instant effectiveFrom,
            @JsonProperty("effectiveTo")   Instant effectiveTo) {
        this.roleName      = Objects.requireNonNull(roleName,     "roleName");
        this.tableName     = Objects.requireNonNull(tableName,    "tableName");
        this.rowPolicy     = Objects.requireNonNull(rowPolicy,    "rowPolicy");
        this.columnPolicy  = Objects.requireNonNull(columnPolicy, "columnPolicy");
        this.auditEnabled  = auditEnabled;
        this.effectiveFrom = effectiveFrom != null ? effectiveFrom : Instant.EPOCH;
        this.effectiveTo   = effectiveTo;
    }

    public String getRoleName()          { return roleName; }
    public String getTableName()         { return tableName; }
    public RowPolicy getRowPolicy()      { return rowPolicy; }
    public ColumnPolicy getColumnPolicy(){ return columnPolicy; }
    public boolean isAuditEnabled()      { return auditEnabled; }
    public Instant getEffectiveFrom()    { return effectiveFrom; }
    public Instant getEffectiveTo()      { return effectiveTo; }

    public boolean isDefaultDeny()  {
        return rowPolicy.isDenyAll() && columnPolicy.isDenyAll();
    }

    /**
     * Whether this entitlement is currently active (not expired).
     */
    public boolean isActive() {
        Instant now = Instant.now();
        return !now.isBefore(effectiveFrom)
               && (effectiveTo == null || now.isBefore(effectiveTo));
    }

    // ── Builder ──────────────────────────────────────────────────────────

    public static Builder builder(String roleName, String tableName) {
        return new Builder(roleName, tableName);
    }

    public static final class Builder {
        private final String roleName;
        private final String tableName;
        private RowPolicy rowPolicy     = RowPolicy.DENY_ALL;
        private ColumnPolicy columnPolicy = ColumnPolicy.DENY_ALL;
        private boolean auditEnabled    = true;
        private Instant effectiveFrom   = Instant.now();
        private Instant effectiveTo     = null;

        private Builder(String roleName, String tableName) {
            this.roleName  = Objects.requireNonNull(roleName);
            this.tableName = Objects.requireNonNull(tableName);
        }

        public Builder rowPolicy(RowPolicy rowPolicy) {
            this.rowPolicy = Objects.requireNonNull(rowPolicy);
            return this;
        }

        public Builder columnPolicy(ColumnPolicy columnPolicy) {
            this.columnPolicy = Objects.requireNonNull(columnPolicy);
            return this;
        }

        public Builder auditEnabled(boolean enabled) {
            this.auditEnabled = enabled;
            return this;
        }

        public Builder effectiveFrom(Instant from) {
            this.effectiveFrom = from;
            return this;
        }

        public Builder effectiveTo(Instant to) {
            this.effectiveTo = to;
            return this;
        }

        public TableEntitlement build() {
            return new TableEntitlement(
                    roleName, tableName, rowPolicy, columnPolicy,
                    auditEnabled, effectiveFrom, effectiveTo);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TableEntitlement that)) return false;
        return roleName.equals(that.roleName) && tableName.equals(that.tableName);
    }

    @Override public int hashCode() { return Objects.hash(roleName, tableName); }

    @Override public String toString() {
        return "TableEntitlement{role='" + roleName
               + "', table='" + tableName
               + "', row=" + rowPolicy
               + ", col=" + columnPolicy + '}';
    }
}