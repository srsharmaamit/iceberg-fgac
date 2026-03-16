package com.banking.lakehouse.fgac.api.policy;

import com.banking.lakehouse.fgac.api.entitlement.TableEntitlement;
import com.banking.lakehouse.fgac.api.identity.UserIdentity;

import java.util.List;
import java.util.Optional;

/**
 * Central SPI for entitlement policy lookup.
 *
 * <p>This interface is the only dependency the engine adapters and the
 * {@code IcebergExpressionBuilder} have on the policy subsystem. Swap
 * the backing store (JSON file → Iceberg table → external IdP) by
 * providing a different implementation — no adapter code changes.
 *
 * <h3>Lookup semantics</h3>
 * <ol>
 *   <li>The store is keyed by (role, tableName). A single user may have
 *       multiple roles; the caller resolves which entitlement applies by
 *       calling {@link #findMostPermissive(UserIdentity, String)} which
 *       merges applicable policies with a most-permissive-wins strategy.</li>
 *   <li>If no policy matches, implementations must return
 *       {@link TableEntitlement#DEFAULT_DENY} — never {@code null} and
 *       never throw.</li>
 *   <li>Lookup is expected to be O(1) from an in-memory cache managed by
 *       the {@code PolicyCacheManager}. The backing store is read on
 *       initialisation and refreshed on TTL expiry only.</li>
 * </ol>
 *
 * <h3>Thread safety</h3>
 * Implementations must be thread-safe. The Catalyst optimizer rule is
 * called from multiple threads concurrently during parallel query planning.
 */
public interface EntitlementPolicyStore {

    /**
     * Find the entitlement for a specific (role, table) pair.
     *
     * @param roleName  the role name (e.g. "analyst_uk", "svc_airflow")
     * @param tableName fully-qualified table name (e.g. "silver.cleansed_transactions")
     * @return the matching entitlement, or {@link TableEntitlement#DEFAULT_DENY}
     */
    TableEntitlement find(String roleName, String tableName);

    /**
     * Find the most permissive entitlement applicable to a user across
     * all their roles for the given table.
     *
     * <p>Merging strategy: if any role returns ALLOW_ALL row policy, the
     * merged result is ALLOW_ALL. Column policies are unioned — the merged
     * allowed column set is the union of all role column sets. This follows
     * the principle of least surprise for multi-role users.
     *
     * @param identity  the resolved user identity (may carry multiple roles)
     * @param tableName fully-qualified table name
     * @return merged entitlement across all applicable roles
     */
    TableEntitlement findMostPermissive(UserIdentity identity, String tableName);

    /**
     * Returns all entitlements currently in the store.
     * Used by the future admin REST API and CLI.
     */
    List<TableEntitlement> findAll();

    /**
     * Returns all entitlements for a specific role.
     */
    List<TableEntitlement> findByRole(String roleName);

    /**
     * Returns all entitlements for a specific table.
     */
    List<TableEntitlement> findByTable(String tableName);

    /**
     * Checks whether the store is healthy and the cache is populated.
     * Used by health-check endpoints and startup validation.
     */
    boolean isHealthy();

    /**
     * Returns a human-readable description of the backing store type.
     * Used for logging and metrics labels.
     */
    String storeType();
}