package com.banking.lakehouse.fgac.core.policy;

import com.banking.lakehouse.fgac.api.entitlement.ColumnPolicy;
import com.banking.lakehouse.fgac.api.entitlement.RowPolicy;
import com.banking.lakehouse.fgac.api.entitlement.TableEntitlement;
import com.banking.lakehouse.fgac.testing.fixtures.PolicyFixtures;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("ColumnPolicyEngine")
class ColumnPolicyEngineTest {

    private static final List<String> SCHEMA_COLUMNS = List.of(
            "transaction_id", "account_number", "transaction_amount",
            "transaction_ts", "jurisdiction", "merchant_category",
            "transaction_type", "currency_code", "data_classification",
            "_ingested_at", "_nessie_ref");

    private ColumnPolicyEngine engine;

    @BeforeEach
    void setUp() {
        engine = new ColumnPolicyEngine();
    }

    @Nested
    @DisplayName("ALLOW_ALL policy")
    class AllowAll {

        @Test
        @DisplayName("all schema columns are in allowed list")
        void allColumnsAllowed() {
            TableEntitlement entitlement = TableEntitlement.builder(
                            PolicyFixtures.ROLE_DATA_ENGINEER,
                            PolicyFixtures.SILVER_TRANSACTIONS)
                    .rowPolicy(RowPolicy.ALLOW_ALL)
                    .columnPolicy(ColumnPolicy.ALLOW_ALL)
                    .build();

            ColumnPolicyEngine.ProjectionResult result =
                    engine.project(SCHEMA_COLUMNS, entitlement);

            assertThat(result.allowAll()).isTrue();
            assertThat(result.allowedColumns()).containsExactlyInAnyOrderElementsOf(SCHEMA_COLUMNS);
            assertThat(result.maskedColumns()).isEmpty();
            assertThat(result.redactedColumns()).isEmpty();
        }
    }

    @Nested
    @DisplayName("DENY_ALL policy")
    class DenyAll {

        @Test
        @DisplayName("denyAll flag is set and no columns allowed")
        void noColumnsAllowed() {
            TableEntitlement entitlement = TableEntitlement.DEFAULT_DENY;
            ColumnPolicyEngine.ProjectionResult result =
                    engine.project(SCHEMA_COLUMNS, entitlement);

            assertThat(result.denyAll()).isTrue();
            assertThat(result.allowedColumns()).isEmpty();
        }
    }

    @Nested
    @DisplayName("Analyst UK policy")
    class AnalystUk {

        @Test
        @DisplayName("allowed columns exclude redacted columns")
        void allowedColumnsExcludeRedacted() {
            TableEntitlement entitlement = PolicyFixtures.analystUkTransactions();
            ColumnPolicyEngine.ProjectionResult result =
                    engine.project(SCHEMA_COLUMNS, entitlement);

            assertThat(result.allowAll()).isFalse();
            assertThat(result.denyAll()).isFalse();
            assertThat(result.allowedColumns()).contains("transaction_id", "jurisdiction");
            assertThat(result.allowedColumns())
                    .doesNotContain("data_classification", "_ingested_at", "_nessie_ref");
        }

        @Test
        @DisplayName("account_number is in allowed but marked for masking")
        void accountNumberMasked() {
            TableEntitlement entitlement = PolicyFixtures.analystUkTransactions();
            ColumnPolicyEngine.ProjectionResult result =
                    engine.project(SCHEMA_COLUMNS, entitlement);

            assertThat(result.isColumnMasked("account_number")).isTrue();
            assertThat(result.getMaskFunctionId("account_number"))
                    .isEqualTo("fgac_mask_account_number");
        }

        @Test
        @DisplayName("unmasked allowed columns are not in maskedColumns map")
        void unmaskedColumnsNotInMaskMap() {
            TableEntitlement entitlement = PolicyFixtures.analystUkTransactions();
            ColumnPolicyEngine.ProjectionResult result =
                    engine.project(SCHEMA_COLUMNS, entitlement);

            assertThat(result.isColumnMasked("transaction_id")).isFalse();
            assertThat(result.isColumnMasked("jurisdiction")).isFalse();
        }
    }

    @Nested
    @DisplayName("Schema intersection")
    class SchemaIntersection {

        @Test
        @DisplayName("policy columns not in schema are silently dropped")
        void policyColumnsNotInSchemaDropped() {
            // Policy requests a column that doesn't exist in the table
            ColumnPolicy policy = ColumnPolicy.builder()
                    .allowColumn("transaction_id")
                    .allowColumn("nonexistent_column")
                    .build();

            TableEntitlement entitlement = TableEntitlement.builder(
                            "test_role", PolicyFixtures.SILVER_TRANSACTIONS)
                    .rowPolicy(RowPolicy.ALLOW_ALL)
                    .columnPolicy(policy)
                    .build();

            ColumnPolicyEngine.ProjectionResult result =
                    engine.project(SCHEMA_COLUMNS, entitlement);

            assertThat(result.allowedColumns()).containsExactly("transaction_id");
            assertThat(result.allowedColumns()).doesNotContain("nonexistent_column");
        }
    }
}