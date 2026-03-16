package com.banking.lakehouse.fgac.store.json;

import com.banking.lakehouse.fgac.api.entitlement.TableEntitlement;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("JsonPolicyReader")
class JsonPolicyReaderTest {

    private JsonPolicyReader reader;

    @BeforeEach
    void setUp() {
        ObjectMapper mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        reader = new JsonPolicyReader(mapper);
    }

    @Test
    @DisplayName("reads all entitlements from the banking seed file")
    void readsSeedFile() throws Exception {
        InputStream is = getClass().getResourceAsStream("/test-policies.json");
        assertThat(is).isNotNull();

        List<TableEntitlement> entitlements = reader.readAll(is);

        assertThat(entitlements).isNotEmpty();
    }

    @Test
    @DisplayName("analyst_uk has jurisdiction row predicate")
    void analystUkHasJurisdictionFilter() throws Exception {
        List<TableEntitlement> all = readTestFile();

        TableEntitlement ukEntry = all.stream()
                .filter(e -> e.getRoleName().equals("analyst_uk"))
                .filter(e -> e.getTableName().equals("silver.cleansed_transactions"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("analyst_uk entry not found"));

        assertThat(ukEntry.getRowPolicy().getPredicate())
                .contains("UK");
        assertThat(ukEntry.getRowPolicy().isAllowAll()).isFalse();
        assertThat(ukEntry.getRowPolicy().isDenyAll()).isFalse();
    }

    @Test
    @DisplayName("account_number is in maskedColumns for analyst roles")
    void accountNumberMaskedForAnalysts() throws Exception {
        List<TableEntitlement> all = readTestFile();

        TableEntitlement ukEntry = all.stream()
                .filter(e -> e.getRoleName().equals("analyst_uk"))
                .filter(e -> e.getTableName().equals("silver.cleansed_transactions"))
                .findFirst()
                .orElseThrow();

        assertThat(ukEntry.getColumnPolicy().getMaskedColumns())
                .containsKey("account_number");
        assertThat(ukEntry.getColumnPolicy().getMaskedColumns().get("account_number"))
                .isEqualTo("fgac_mask_account_number");
    }

    @Test
    @DisplayName("data_engineer has ALLOW_ALL row policy")
    void dataEngineerAllowAll() throws Exception {
        List<TableEntitlement> all = readTestFile();

        TableEntitlement entry = all.stream()
                .filter(e -> e.getRoleName().equals("data_engineer"))
                .filter(e -> e.getTableName().equals("silver.cleansed_transactions"))
                .findFirst()
                .orElseThrow(() -> new AssertionError("data_engineer entry not found"));

        assertThat(entry.getRowPolicy().isAllowAll()).isTrue();
    }

    @Test
    @DisplayName("audit_enabled defaults to true when absent")
    void auditEnabledDefault() throws Exception {
        List<TableEntitlement> all = readTestFile();
        assertThat(all).allMatch(TableEntitlement::isAuditEnabled);
    }

    @Test
    @DisplayName("malformed entries are skipped, valid entries still parsed")
    void malformedEntriesSkipped() throws Exception {
        String json = """
            {
              "version": "1.0.0",
              "entitlements": [
                {
                  "roleName": "analyst_uk",
                  "tableName": "silver.cleansed_transactions",
                  "rowPredicate": "jurisdiction = 'UK'"
                },
                {
                  "comment": "missing required roleName — should be skipped"
                }
              ]
            }
            """;
        try (InputStream is = new java.io.ByteArrayInputStream(json.getBytes())) {
            List<TableEntitlement> result = reader.readAll(is);
            // Only the valid entry should be returned
            assertThat(result).hasSize(1);
            assertThat(result.get(0).getRoleName()).isEqualTo("analyst_uk");
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private List<TableEntitlement> readTestFile() throws Exception {
        try (InputStream is = getClass().getResourceAsStream("/test-policies.json")) {
            assertThat(is).as("test-policies.json must be on test classpath").isNotNull();
            return reader.readAll(is);
        }
    }
}