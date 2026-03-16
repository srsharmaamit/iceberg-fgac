package com.banking.lakehouse.fgac.core.policy;

import com.banking.lakehouse.fgac.api.entitlement.ColumnPolicy;
import com.banking.lakehouse.fgac.api.entitlement.TableEntitlement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Resolves which columns a user may see and which must be masked.
 *
 * <p>Called by the Catalyst rule after the row filter has been applied.
 * Returns a {@link ProjectionResult} that the rule uses to:
 * <ol>
 *   <li>Replace the query's SELECT list with only the allowed columns.</li>
 *   <li>Wrap masked column references with the registered masking UDF call.</li>
 * </ol>
 *
 * <p>The engine adapter calls this once per (user, table) combination during
 * plan optimisation. Results are not independently cached here — they derive
 * from the already-cached {@link TableEntitlement} in {@link PolicyCacheManager}.
 */
@Component
public class ColumnPolicyEngine {

    private static final Logger log = LoggerFactory.getLogger(ColumnPolicyEngine.class);

    /**
     * Projects the schema columns against the column policy.
     *
     * @param schemaColumns all columns in the table schema (from Iceberg schema)
     * @param entitlement   the resolved entitlement for this (user, table) pair
     * @return a {@link ProjectionResult} describing allowed, masked, and
     *         redacted columns
     */
    public ProjectionResult project(List<String> schemaColumns, TableEntitlement entitlement) {
        ColumnPolicy policy = entitlement.getColumnPolicy();

        if (policy.isAllowAll()) {
            log.debug("Column policy ALLOW_ALL for role='{}' table='{}'",
                    entitlement.getRoleName(), entitlement.getTableName());
            return ProjectionResult.createAllowAll(schemaColumns);
        }

        if (policy.isDenyAll()) {
            log.warn("Column policy DENY_ALL for role='{}' table='{}'",
                    entitlement.getRoleName(), entitlement.getTableName());
            return ProjectionResult.createDenyAll();
        }

        // Intersection: only columns that are both in the schema AND in the allowed list
        Set<String> redacted = policy.getRedactedColumns().stream()
                .collect(Collectors.toUnmodifiableSet());

        List<String> allowed;
        if (policy.getAllowedColumns().isEmpty()) {
            // Empty allowed list means all non-redacted schema columns
            allowed = schemaColumns.stream()
                    .filter(c -> !redacted.contains(c))
                    .collect(Collectors.toList());
        } else {
            Set<String> schemaSet = Set.copyOf(schemaColumns);
            allowed = policy.getAllowedColumns().stream()
                    .filter(schemaSet::contains)
                    .filter(c -> !redacted.contains(c))
                    .collect(Collectors.toList());
        }

        // Masked columns: subset of allowed that have a mask function assigned
        Map<String, String> masked = policy.getMaskedColumns().entrySet().stream()
                .filter(e -> allowed.contains(e.getKey()))
                .collect(Collectors.toUnmodifiableMap(
                        Map.Entry::getKey, Map.Entry::getValue));

        List<String> redactedActual = schemaColumns.stream()
                .filter(c -> !allowed.contains(c))
                .collect(Collectors.toList());

        log.debug("Column projection: total={} allowed={} masked={} redacted={} "
                        + "role='{}' table='{}'",
                schemaColumns.size(), allowed.size(), masked.size(),
                redactedActual.size(), entitlement.getRoleName(),
                entitlement.getTableName());

        return new ProjectionResult(allowed, masked, redactedActual, false, false);
    }

    // ── Projection result ─────────────────────────────────────────────────

    /**
     * Result of column policy projection.
     * Consumed by engine adapters to rewrite the query plan.
     */
    public record ProjectionResult(
            List<String> allowedColumns,
            Map<String, String> maskedColumns,   // col -> maskFunctionId
            List<String> redactedColumns,
            boolean allowAll,
            boolean denyAll) {

        public static ProjectionResult createAllowAll(List<String> allColumns) {
            return new ProjectionResult(allColumns,
                    Map.of(), List.of(), true, false);
        }

        public static ProjectionResult createDenyAll() {
            return new ProjectionResult(List.of(),
                    Map.of(), List.of(), false, true);
        }

        public boolean isColumnMasked(String col) {
            return maskedColumns.containsKey(col);
        }

        public String getMaskFunctionId(String col) {
            return maskedColumns.get(col);
        }
    }
}
