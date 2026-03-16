package com.banking.lakehouse.fgac.store.json;

import com.banking.lakehouse.fgac.api.entitlement.ColumnPolicy;
import com.banking.lakehouse.fgac.api.entitlement.RowPolicy;
import com.banking.lakehouse.fgac.api.entitlement.TableEntitlement;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads {@link TableEntitlement} objects from the seed JSON policy file.
 *
 * <p>Expected JSON structure:
 * <pre>
 * {
 *   "version": "1.0.0",
 *   "entitlements": [
 *     {
 *       "roleName":        "analyst_uk",
 *       "tableName":       "silver.cleansed_transactions",
 *       "rowPredicate":    "jurisdiction = 'UK'",
 *       "allowedColumns":  ["transaction_id", "account_number", ...],
 *       "maskedColumns":   {"account_number": "fgac_mask_account_number"},
 *       "redactedColumns": ["data_classification"],
 *       "auditEnabled":    true,
 *       "description":     "UK analyst scope"
 *     }
 *   ]
 * }
 * </pre>
 *
 * <p>Fields {@code allowedColumns}, {@code maskedColumns}, and
 * {@code redactedColumns} are optional — absence means "all allowed,
 * none masked, none redacted".
 *
 * <p>The {@code comment} field anywhere in the JSON is silently ignored,
 * allowing operators to annotate the file for readability.
 */
@Component
public class JsonPolicyReader {

    private static final Logger log = LoggerFactory.getLogger(JsonPolicyReader.class);

    private final ObjectMapper objectMapper;

    public JsonPolicyReader(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Reads all entitlements from a classpath or filesystem {@link InputStream}.
     */
    public List<TableEntitlement> readAll(InputStream inputStream) throws IOException {
        JsonNode root = objectMapper.readTree(inputStream);
        return parseEntitlements(root);
    }

    /**
     * Reads all entitlements from a filesystem {@link Path}.
     */
    public List<TableEntitlement> readAll(Path path) throws IOException {
        try (InputStream is = Files.newInputStream(path)) {
            return readAll(is);
        }
    }

    // ── Parsing ────────────────────────────────────────────────────────────

    private List<TableEntitlement> parseEntitlements(JsonNode root) {
        List<TableEntitlement> result = new ArrayList<>();

        String version = root.path("version").asText("unknown");
        log.info("Reading seed policy file version '{}'", version);

        JsonNode entitlementsNode = root.path("entitlements");
        if (!entitlementsNode.isArray()) {
            log.warn("Seed policy file has no 'entitlements' array — returning empty");
            return result;
        }

        for (JsonNode node : entitlementsNode) {
            try {
                TableEntitlement entitlement = parseOne(node);
                result.add(entitlement);
                log.debug("Parsed entitlement: role='{}' table='{}'",
                        entitlement.getRoleName(), entitlement.getTableName());
            } catch (Exception e) {
                log.warn("Skipping malformed entitlement node: {} — {}",
                        node.toString().substring(0, Math.min(80, node.toString().length())),
                        e.getMessage());
            }
        }

        log.info("Parsed {} entitlements from seed file", result.size());
        return result;
    }

    private TableEntitlement parseOne(JsonNode node) {
        String roleName  = requireText(node, "roleName");
        String tableName = requireText(node, "tableName");
        String rowPred   = node.path("rowPredicate").asText("DENY_ALL");
        String desc      = node.path("description").asText("");
        boolean audit    = node.path("auditEnabled").asBoolean(true);

        RowPolicy rowPolicy = RowPolicy.of(rowPred, desc);

        // Allowed columns — empty array = all allowed
        List<String> allowedCols = readStringList(node, "allowedColumns");

        // Masked columns — object: { "colName": "maskFunctionId" }
        Map<String, String> maskedCols = readStringMap(node, "maskedColumns");

        // Redacted columns — array of column names
        List<String> redactedCols = readStringList(node, "redactedColumns");

        ColumnPolicy columnPolicy = new ColumnPolicy(
                allowedCols, maskedCols, redactedCols, desc);

        // Optional temporal scope
        Instant effectiveFrom = parseInstant(node, "effectiveFrom", Instant.EPOCH);
        Instant effectiveTo   = parseInstant(node, "effectiveTo", null);

        return TableEntitlement.builder(roleName, tableName)
                .rowPolicy(rowPolicy)
                .columnPolicy(columnPolicy)
                .auditEnabled(audit)
                .effectiveFrom(effectiveFrom)
                .effectiveTo(effectiveTo)
                .build();
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private String requireText(JsonNode node, String field) {
        JsonNode n = node.path(field);
        if (n.isMissingNode() || n.isNull() || n.asText().isBlank()) {
            throw new IllegalArgumentException("Required field missing or blank: " + field);
        }
        return n.asText().trim();
    }

    private List<String> readStringList(JsonNode node, String field) {
        List<String> result = new ArrayList<>();
        JsonNode arr = node.path(field);
        if (arr.isArray()) {
            for (JsonNode item : arr) {
                String val = item.asText("").trim();
                if (!val.isEmpty()) result.add(val);
            }
        }
        return result;
    }

    private Map<String, String> readStringMap(JsonNode node, String field) {
        Map<String, String> result = new HashMap<>();
        JsonNode obj = node.path(field);
        if (obj.isObject()) {
            obj.properties().forEach(entry ->
                    result.put(entry.getKey(), entry.getValue().asText("")));
        }
        return result;
    }

    private Instant parseInstant(JsonNode node, String field, Instant defaultValue) {
        JsonNode n = node.path(field);
        if (n.isMissingNode() || n.isNull()) return defaultValue;
        try {
            if (n.isNumber()) return Instant.ofEpochMilli(n.longValue());
            return Instant.parse(n.asText());
        } catch (Exception e) {
            log.warn("Could not parse instant field '{}' value '{}' — using default",
                    field, n.asText());
            return defaultValue;
        }
    }
}