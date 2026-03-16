package com.banking.lakehouse.fgac.api.entitlement;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Immutable row-level access policy for a (role, table) combination.
 *
 * <p>A {@code RowPolicy} carries a predicate expression string that the
 * {@code IcebergExpressionBuilder} translates into an
 * {@code org.apache.iceberg.expressions.Expression} for scan planning.
 *
 * <h3>Expression syntax</h3>
 * The {@code predicate} field uses a simple domain-specific syntax that
 * the expression builder parses:
 * <pre>
 *   "jurisdiction = 'UK'"
 *   "jurisdiction IN ('DE','FR','NL')"
 *   "jurisdiction = 'UK' AND data_classification != 'PII_RESTRICTED'"
 *   "ALLOW_ALL"    — no row restriction (data_engineer, compliance_officer)
 *   "DENY_ALL"     — no rows visible (default when no policy matches)
 * </pre>
 *
 * <p>The expression is intentionally kept as a string (not a pre-built Iceberg
 * {@code Expression}) so that the API module has zero iceberg-core dependency.
 * Parsing happens in {@code fgac-core}.
 */
public final class RowPolicy {

    /** Sentinel: no row restriction. */
    public static final RowPolicy ALLOW_ALL =
            new RowPolicy("ALLOW_ALL", "No row restriction — full table visible");

    /** Sentinel: deny all rows. Used when no policy matches a (role, table) pair. */
    public static final RowPolicy DENY_ALL =
            new RowPolicy("DENY_ALL", "Default deny — no matching policy found");

    private final String predicate;
    private final String description;

    @JsonCreator
    public RowPolicy(
            @JsonProperty("predicate") String predicate,
            @JsonProperty("description") String description) {
        this.predicate   = Objects.requireNonNull(predicate, "predicate");
        this.description = description != null ? description : "";
    }

    public String getPredicate()   { return predicate; }
    public String getDescription() { return description; }

    public boolean isAllowAll() { return "ALLOW_ALL".equals(predicate); }
    public boolean isDenyAll()  { return "DENY_ALL".equals(predicate); }

    public static RowPolicy of(String predicate) {
        return new RowPolicy(predicate, "");
    }

    public static RowPolicy of(String predicate, String description) {
        return new RowPolicy(predicate, description);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RowPolicy that)) return false;
        return predicate.equals(that.predicate);
    }

    @Override public int hashCode() { return Objects.hash(predicate); }

    @Override
    public String toString() {
        return "RowPolicy{predicate='" + predicate + "'}";
    }
}