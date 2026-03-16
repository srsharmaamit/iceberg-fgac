package com.banking.lakehouse.fgac.core.policy;

import com.banking.lakehouse.fgac.api.entitlement.TableEntitlement;
import com.banking.lakehouse.fgac.api.identity.UserIdentity;
import com.banking.lakehouse.fgac.api.policy.EntitlementPolicyStore;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Caching decorator over an {@link EntitlementPolicyStore}.
 *
 * <p>This is the class that the engine adapters actually talk to.
 * It wraps the backing store (Iceberg table, JSON file) with a
 * Guava {@code LoadingCache} keyed on {@code "role::tableName"}.
 *
 * <h3>Cache design</h3>
 * <ul>
 *   <li>TTL: 5 minutes (configurable via {@code fgac.policy.cache.ttl-minutes})</li>
 *   <li>Max size: 10,000 entries (role × table combinations)</li>
 *   <li>On cache miss: blocking load from backing store. Expected only at
 *       startup and after TTL expiry.</li>
 *   <li>On backing store failure: returns {@link TableEntitlement#DEFAULT_DENY}
 *       and logs an error. Never throws to the query path.</li>
 * </ul>
 *
 * <p>The cache key is {@code "roleName::tableName"}. Each unique combination
 * is cached independently so a single role with access to 50 tables creates
 * 50 cache entries, each evictable independently.
 */
@Component
public class PolicyCacheManager implements EntitlementPolicyStore {

    private static final Logger log = LoggerFactory.getLogger(PolicyCacheManager.class);

    private final EntitlementPolicyStore delegate;
    private final LoadingCache<String, TableEntitlement> cache;

    public PolicyCacheManager(
            EntitlementPolicyStore delegate,
            @Value("${fgac.policy.cache.ttl-minutes:5}") int ttlMinutes,
            @Value("${fgac.policy.cache.max-size:10000}") long maxSize) {

        this.delegate = delegate;
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(maxSize)
                .expireAfterWrite(ttlMinutes, TimeUnit.MINUTES)
                .recordStats()
                .build(CacheLoader.from(this::loadFromDelegate));

        log.info("PolicyCacheManager initialised: ttl={}min, maxSize={}",
                ttlMinutes, maxSize);
    }

    @Override
    public TableEntitlement find(String roleName, String tableName) {
        String key = cacheKey(roleName, tableName);
        try {
            return cache.get(key);
        } catch (ExecutionException e) {
            log.error("Cache load failed for key '{}' — applying default deny: {}",
                    key, e.getMessage());
            return TableEntitlement.DEFAULT_DENY;
        }
    }

    @Override
    public TableEntitlement findMostPermissive(UserIdentity identity, String tableName) {
        // Try each role, collect applicable entitlements, merge most permissive
        return identity.getRoles().stream()
                .map(role -> find(role, tableName))
                .filter(e -> !e.isDefaultDeny())
                .filter(TableEntitlement::isActive)
                .reduce(this::mergePermissive)
                .orElse(TableEntitlement.DEFAULT_DENY);
    }

    @Override
    public List<TableEntitlement> findAll() { return delegate.findAll(); }

    @Override
    public List<TableEntitlement> findByRole(String roleName) {
        return delegate.findByRole(roleName);
    }

    @Override
    public List<TableEntitlement> findByTable(String tableName) {
        return delegate.findByTable(tableName);
    }

    @Override
    public boolean isHealthy() {
        return delegate.isHealthy();
    }

    @Override
    public String storeType() {
        return "CachingDecorator[" + delegate.storeType() + "]";
    }

    /**
     * Forces immediate invalidation of all cached entries.
     * Called by the admin CLI and REST API when a policy is updated.
     */
    public void invalidateAll() {
        cache.invalidateAll();
        log.info("Policy cache invalidated — next lookup will reload from backing store");
    }

    /** Exposes Guava cache stats for metrics/monitoring. */
    public com.google.common.cache.CacheStats stats() {
        return cache.stats();
    }

    // ── Private helpers ────────────────────────────────────────────────────

    private TableEntitlement loadFromDelegate(String key) {
        String[] parts = key.split("::", 2);
        if (parts.length != 2) return TableEntitlement.DEFAULT_DENY;
        TableEntitlement result = delegate.find(parts[0], parts[1]);
        log.debug("Cache miss for key '{}' — loaded from backing store: {}", key, result);
        return result;
    }

    private String cacheKey(String roleName, String tableName) {
        return roleName + "::" + tableName;
    }

    /**
     * Most-permissive merge: ALLOW_ALL row wins; column sets are unioned.
     */
    private TableEntitlement mergePermissive(TableEntitlement a, TableEntitlement b) {
        // Row policy: most permissive wins
        var rowPolicy = a.getRowPolicy().isAllowAll() || b.getRowPolicy().isAllowAll()
                ? com.banking.lakehouse.fgac.api.entitlement.RowPolicy.ALLOW_ALL
                : a.getRowPolicy(); // first match wins for non-trivial predicates

        // Column policy: union allowed columns, intersection of masked columns
        var colBuilder = com.banking.lakehouse.fgac.api.entitlement.ColumnPolicy.builder();
        colBuilder.allowColumns(a.getColumnPolicy().getAllowedColumns());
        colBuilder.allowColumns(b.getColumnPolicy().getAllowedColumns());
        // Only mask a column if BOTH policies require masking
        a.getColumnPolicy().getMaskedColumns().forEach((col, fn) -> {
            if (b.getColumnPolicy().isColumnMasked(col)) {
                colBuilder.maskColumn(col, fn);
            }
        });

        return TableEntitlement.builder(a.getRoleName(), a.getTableName())
                .rowPolicy(rowPolicy)
                .columnPolicy(colBuilder.build())
                .auditEnabled(a.isAuditEnabled() || b.isAuditEnabled())
                .build();
    }
}