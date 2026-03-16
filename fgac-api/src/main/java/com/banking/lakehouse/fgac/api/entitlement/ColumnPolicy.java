package com.banking.lakehouse.fgac.api.entitlement;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Immutable column-level access policy for a (role, table) combination.
 *
 * <p>Carries three separate concerns deliberately kept distinct:
 * <ol>
 *   <li><b>allowedColumns</b> – the exhaustive list of columns this role may see.
 *       An empty list means "all columns allowed" (not "no columns allowed" — use
 *       the {@link #DENY_ALL} sentinel for that). The Catalyst plan rewriter
 *       replaces the query's SELECT list with this list at optimisation time.</li>
 *   <li><b>maskedColumns</b> – columns that are in the allowed set but whose
 *       values must be transformed by a registered masking function before being
 *       returned to the caller. Key = column name, value = masking function id
 *       registered in {@code MaskingFunctionLibrary}.</li>
 *   <li><b>redactedColumns</b> – columns that are projected out entirely (not in
 *       the allowed set). Kept as an explicit field for audit readability — the
 *       audit record logs which columns were redacted, not just which were allowed.</li>
 * </ol>
 */
public final class ColumnPolicy {

    /** Sentinel: all columns allowed, no masking. */
    public static final ColumnPolicy ALLOW_ALL = new ColumnPolicy(
            Collections.emptyList(),
            Collections.emptyMap(),
            Collections.emptyList(),
            "All columns allowed, no masking");

    /** Sentinel: no columns allowed. */
    public static final ColumnPolicy DENY_ALL = new ColumnPolicy(
            Collections.emptyList(),
            Collections.emptyMap(),
            Collections.emptyList(),
            "DENY_ALL");

    private final List<String> allowedColumns;   // empty = all allowed
    private final Map<String, String> maskedColumns; // col → maskFunctionId
    private final List<String> redactedColumns;  // projected out entirely
    private final String description;

    @JsonCreator
    public ColumnPolicy(
            @JsonProperty("allowedColumns")  List<String> allowedColumns,
            @JsonProperty("maskedColumns")   Map<String, String> maskedColumns,
            @JsonProperty("redactedColumns") List<String> redactedColumns,
            @JsonProperty("description")     String description) {
        this.allowedColumns  = Collections.unmodifiableList(
                allowedColumns  != null ? allowedColumns  : Collections.emptyList());
        this.maskedColumns   = Collections.unmodifiableMap(
                maskedColumns   != null ? maskedColumns   : Collections.emptyMap());
        this.redactedColumns = Collections.unmodifiableList(
                redactedColumns != null ? redactedColumns : Collections.emptyList());
        this.description     = description != null ? description : "";
    }

    public List<String> getAllowedColumns()          { return allowedColumns; }
    public Map<String, String> getMaskedColumns()    { return maskedColumns; }
    public List<String> getRedactedColumns()         { return redactedColumns; }
    public String getDescription()                   { return description; }

    /** True when all columns are visible with no restrictions. */
    public boolean isAllowAll() {
        return allowedColumns.isEmpty()
                && maskedColumns.isEmpty()
                && redactedColumns.isEmpty()
                && !"DENY_ALL".equals(description);
    }

    public boolean isDenyAll() { return "DENY_ALL".equals(description); }

    public boolean isColumnMasked(String columnName) {
        return maskedColumns.containsKey(columnName);
    }

    public String getMaskFunctionId(String columnName) {
        return maskedColumns.get(columnName);
    }

    // ── Builder ──────────────────────────────────────────────────────────

    public static Builder builder() { return new Builder(); }

    public static final class Builder {
        private final List<String> allowedColumns  = new java.util.ArrayList<>();
        private final Map<String, String> maskedColumns = new java.util.LinkedHashMap<>();
        private final List<String> redactedColumns = new java.util.ArrayList<>();
        private String description = "";

        public Builder allowColumn(String col) {
            allowedColumns.add(Objects.requireNonNull(col));
            return this;
        }

        public Builder allowColumns(java.util.Collection<String> cols) {
            allowedColumns.addAll(cols);
            return this;
        }

        public Builder maskColumn(String col, String maskFunctionId) {
            allowedColumns.add(Objects.requireNonNull(col));
            maskedColumns.put(col, Objects.requireNonNull(maskFunctionId));
            return this;
        }

        public Builder redactColumn(String col) {
            redactedColumns.add(Objects.requireNonNull(col));
            return this;
        }

        public Builder description(String description) {
            this.description = description;
            return this;
        }

        public ColumnPolicy build() {
            return new ColumnPolicy(allowedColumns, maskedColumns, redactedColumns, description);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnPolicy that)) return false;
        return allowedColumns.equals(that.allowedColumns)
                && maskedColumns.equals(that.maskedColumns)
                && redactedColumns.equals(that.redactedColumns);
    }

    @Override public int hashCode() {
        return Objects.hash(allowedColumns, maskedColumns, redactedColumns);
    }

    @Override public String toString() {
        return "ColumnPolicy{allowed=" + allowedColumns.size()
                + ", masked=" + maskedColumns.size()
                + ", redacted=" + redactedColumns.size() + '}';
    }
}