package com.banking.lakehouse.fgac.testing.mock;

import com.banking.lakehouse.fgac.api.entitlement.ColumnPolicy;
import com.banking.lakehouse.fgac.api.entitlement.RowPolicy;
import com.banking.lakehouse.fgac.api.entitlement.TableEntitlement;
import com.banking.lakehouse.fgac.api.identity.UserIdentity;
import com.banking.lakehouse.fgac.api.policy.EntitlementPolicyStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * In-memory policy store for unit tests.
 *
 * <p>Pre-populated with realistic banking entitlements via the fluent
 * builder methods. Tests that need specific policies add them directly:
 * <pre>
 *   MockPolicyStore store = new MockPolicyStore()
 *       .withEntitlement(
 *           TableEntitlement.builder("analyst_uk", "silver.transactions")
 *               .rowPolicy(RowPolicy.of("jurisdiction = 'UK'"))
 *               .columnPolicy(ColumnPolicy.ALLOW_ALL)
 *               .build());
 * </pre>
 */
public class MockPolicyStore implements EntitlementPolicyStore {

    private final Map<String, TableEntitlement> store = new HashMap<>();
    private boolean simulateUnhealthy = false;

    public MockPolicyStore() {}

    /** Fluent method to add a pre-built entitlement. */
    public MockPolicyStore withEntitlement(TableEntitlement entitlement) {
        store.put(key(entitlement.getRoleName(), entitlement.getTableName()), entitlement);
        return this;
    }

    /** Convenience: add full-access entitlement for a service account. */
    public MockPolicyStore withFullAccess(String role, String table) {
        return withEntitlement(TableEntitlement.builder(role, table)
                .rowPolicy(RowPolicy.ALLOW_ALL)
                .columnPolicy(ColumnPolicy.ALLOW_ALL)
                .build());
    }

    /** Convenience: add jurisdiction-scoped analyst entitlement. */
    public MockPolicyStore withJurisdictionScope(
            String role, String table, String jurisdiction) {
        return withEntitlement(TableEntitlement.builder(role, table)
                .rowPolicy(RowPolicy.of("jurisdiction = '" + jurisdiction + "'",
                        role + " scoped to " + jurisdiction))
                .columnPolicy(ColumnPolicy.ALLOW_ALL)
                .build());
    }

    public MockPolicyStore simulateUnhealthy() {
        this.simulateUnhealthy = true;
        return this;
    }

    // ── EntitlementPolicyStore ────────────────────────────────────────────

    @Override
    public TableEntitlement find(String roleName, String tableName) {
        return store.getOrDefault(key(roleName, tableName), TableEntitlement.DEFAULT_DENY);
    }

    @Override
    public TableEntitlement findMostPermissive(UserIdentity identity, String tableName) {
        return identity.getRoles().stream()
                .map(role -> find(role, tableName))
                .filter(e -> !e.isDefaultDeny())
                .findFirst()
                .orElse(TableEntitlement.DEFAULT_DENY);
    }

    @Override
    public List<TableEntitlement> findAll() {
        return new ArrayList<>(store.values());
    }

    @Override
    public List<TableEntitlement> findByRole(String roleName) {
        return store.entrySet().stream()
                .filter(e -> e.getKey().startsWith(roleName + "::"))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    @Override
    public List<TableEntitlement> findByTable(String tableName) {
        return store.entrySet().stream()
                .filter(e -> e.getKey().endsWith("::" + tableName))
                .map(Map.Entry::getValue)
                .collect(Collectors.toList());
    }

    @Override
    public boolean isHealthy() { return !simulateUnhealthy; }

    @Override
    public String storeType() { return "MockPolicyStore"; }

    private String key(String role, String table) { return role + "::" + table; }
}