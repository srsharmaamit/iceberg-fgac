package com.banking.lakehouse.fgac.testing.fixtures;

import com.banking.lakehouse.fgac.api.entitlement.ColumnPolicy;
import com.banking.lakehouse.fgac.api.entitlement.RowPolicy;
import com.banking.lakehouse.fgac.api.entitlement.TableEntitlement;
import com.banking.lakehouse.fgac.api.identity.UserIdentity;
import com.banking.lakehouse.fgac.testing.mock.MockPolicyStore;

import java.util.List;
import java.util.Map;

/**
 * Shared test fixtures for banking entitlement scenarios.
 *
 * <p>Centralises the "standard" entitlement rules so that every test
 * that needs a realistic policy store can call
 * {@link #standardBankingPolicyStore()} without duplicating the
 * same setup across test classes.
 *
 * <p>All fixtures are immutable — callers may add extra entitlements
 * on top via {@code MockPolicyStore.withEntitlement()}.
 */
public final class PolicyFixtures {

    private PolicyFixtures() {}

    // ── Table names ────────────────────────────────────────────────────────

    public static final String SILVER_TRANSACTIONS  = "silver.cleansed_transactions";
    public static final String SILVER_CUSTOMERS     = "silver.cleansed_customers";
    public static final String GOLD_ACCOUNT_SUMMARY = "gold.account_summary";
    public static final String GOLD_RISK_REPORT     = "gold.jurisdiction_risk_report";
    public static final String AUDIT_LOG            = "audit.query_log";
    public static final String ENTITLEMENTS_RULES   = "entitlements.rules";

    // ── Role names ─────────────────────────────────────────────────────────

    public static final String ROLE_DATA_ENGINEER    = "data_engineer";
    public static final String ROLE_COMPLIANCE       = "compliance_officer";
    public static final String ROLE_ANALYST_UK       = "analyst_uk";
    public static final String ROLE_ANALYST_EU       = "analyst_eu";

    // ── Identity fixtures ──────────────────────────────────────────────────

    public static UserIdentity svcAirflow() {
        return UserIdentity.builder("svc_airflow")
                .role(ROLE_DATA_ENGINEER)
                .attribute("engine", "spark")
                .build();
    }

    public static UserIdentity svcSpark() {
        return UserIdentity.builder("svc_spark")
                .role(ROLE_DATA_ENGINEER)
                .attribute("engine", "spark")
                .build();
    }

    public static UserIdentity analystUk() {
        return UserIdentity.builder("alice")
                .role(ROLE_ANALYST_UK)
                .attribute("jurisdiction", "UK")
                .build();
    }

    public static UserIdentity analystEu() {
        return UserIdentity.builder("bob")
                .role(ROLE_ANALYST_EU)
                .attribute("jurisdiction", "EU")
                .build();
    }

    public static UserIdentity complianceOfficer() {
        return UserIdentity.builder("carol")
                .role(ROLE_COMPLIANCE)
                .attribute("jurisdiction", "ALL")
                .build();
    }

    public static UserIdentity unknownUser() {
        return UserIdentity.UNKNOWN;
    }

    // ── Entitlement fixtures ───────────────────────────────────────────────

    public static TableEntitlement dataEngineerFullAccess(String tableName) {
        return TableEntitlement.builder(ROLE_DATA_ENGINEER, tableName)
                .rowPolicy(RowPolicy.ALLOW_ALL)
                .columnPolicy(ColumnPolicy.ALLOW_ALL)
                .auditEnabled(true)
                .build();
    }

    public static TableEntitlement analystUkTransactions() {
        ColumnPolicy cols = ColumnPolicy.builder()
                .allowColumns(List.of(
                        "transaction_id", "account_number", "transaction_amount",
                        "transaction_ts", "jurisdiction", "merchant_category",
                        "transaction_type", "currency_code"))
                .maskColumn("account_number", "fgac_mask_account_number")
                .redactColumn("data_classification")
                .redactColumn("_ingested_at")
                .redactColumn("_nessie_ref")
                .build();

        return TableEntitlement.builder(ROLE_ANALYST_UK, SILVER_TRANSACTIONS)
                .rowPolicy(RowPolicy.of("jurisdiction = 'UK'", "UK analyst scope"))
                .columnPolicy(cols)
                .auditEnabled(true)
                .build();
    }

    public static TableEntitlement analystEuTransactions() {
        ColumnPolicy cols = ColumnPolicy.builder()
                .allowColumns(List.of(
                        "transaction_id", "account_number", "transaction_amount",
                        "transaction_ts", "jurisdiction", "merchant_category",
                        "transaction_type", "currency_code"))
                .maskColumn("account_number", "fgac_mask_account_number")
                .redactColumn("data_classification")
                .build();

        return TableEntitlement.builder(ROLE_ANALYST_EU, SILVER_TRANSACTIONS)
                .rowPolicy(RowPolicy.of(
                        "jurisdiction IN ('DE','FR','NL','ES','IT','BE','PL')",
                        "EU analyst scope"))
                .columnPolicy(cols)
                .auditEnabled(true)
                .build();
    }

    public static TableEntitlement complianceOfficerTransactions() {
        ColumnPolicy cols = ColumnPolicy.builder()
                .allowColumns(List.of(
                        "transaction_id", "account_number", "transaction_amount",
                        "transaction_ts", "jurisdiction", "merchant_category",
                        "transaction_type", "currency_code", "data_classification",
                        "_ingested_at", "_nessie_ref"))
                .maskColumn("account_number", "fgac_mask_account_number")
                .build();

        return TableEntitlement.builder(ROLE_COMPLIANCE, SILVER_TRANSACTIONS)
                .rowPolicy(RowPolicy.ALLOW_ALL)
                .columnPolicy(cols)
                .auditEnabled(true)
                .build();
    }

    // ── Pre-built MockPolicyStore ──────────────────────────────────────────

    /**
     * Returns a {@link MockPolicyStore} pre-loaded with the standard
     * banking PoC entitlement rules for all roles and tables.
     */
    public static MockPolicyStore standardBankingPolicyStore() {
        MockPolicyStore store = new MockPolicyStore();

        // Data engineer: full access to all tables
        for (String table : List.of(
                SILVER_TRANSACTIONS, SILVER_CUSTOMERS,
                GOLD_ACCOUNT_SUMMARY, GOLD_RISK_REPORT,
                AUDIT_LOG, ENTITLEMENTS_RULES)) {
            store.withEntitlement(dataEngineerFullAccess(table));
        }

        // Analyst UK
        store.withEntitlement(analystUkTransactions());
        store.withEntitlement(TableEntitlement.builder(ROLE_ANALYST_UK, GOLD_ACCOUNT_SUMMARY)
                .rowPolicy(RowPolicy.of("jurisdiction = 'UK'"))
                .columnPolicy(ColumnPolicy.builder()
                        .allowColumns(List.of("masked_account_number", "jurisdiction",
                                "report_date", "total_credits", "total_debits",
                                "net_balance", "transaction_count"))
                        .build())
                .build());

        // Analyst EU
        store.withEntitlement(analystEuTransactions());

        // Compliance officer
        store.withEntitlement(complianceOfficerTransactions());
        store.withEntitlement(TableEntitlement.builder(ROLE_COMPLIANCE, AUDIT_LOG)
                .rowPolicy(RowPolicy.ALLOW_ALL)
                .columnPolicy(ColumnPolicy.ALLOW_ALL)
                .build());

        return store;
    }
}