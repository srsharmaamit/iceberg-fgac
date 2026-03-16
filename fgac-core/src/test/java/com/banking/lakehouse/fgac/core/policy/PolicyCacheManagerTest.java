package com.banking.lakehouse.fgac.core.policy;

import com.banking.lakehouse.fgac.api.entitlement.RowPolicy;
import com.banking.lakehouse.fgac.api.entitlement.TableEntitlement;
import com.banking.lakehouse.fgac.api.identity.UserIdentity;
import com.banking.lakehouse.fgac.testing.fixtures.PolicyFixtures;
import com.banking.lakehouse.fgac.testing.mock.MockPolicyStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("PolicyCacheManager")
class PolicyCacheManagerTest {

    private MockPolicyStore delegate;
    private PolicyCacheManager cache;

    @BeforeEach
    void setUp() {
        delegate = PolicyFixtures.standardBankingPolicyStore();
        // Short TTL for tests — 1 min (won't expire during test run)
        cache = new PolicyCacheManager(delegate, 1, 1000);
    }

    @Nested
    @DisplayName("find by role and table")
    class FindByRoleAndTable {

        @Test
        @DisplayName("returns entitlement for known role + table")
        void knownRoleTable() {
            TableEntitlement result = cache.find(
                    PolicyFixtures.ROLE_ANALYST_UK,
                    PolicyFixtures.SILVER_TRANSACTIONS);

            assertThat(result).isNotNull();
            assertThat(result.isDefaultDeny()).isFalse();
            assertThat(result.getRowPolicy().getPredicate())
                    .isEqualTo("jurisdiction = 'UK'");
        }

        @Test
        @DisplayName("returns DEFAULT_DENY for unknown role + table combination")
        void unknownReturnsDefaultDeny() {
            TableEntitlement result = cache.find("unknown_role", "nonexistent.table");

            assertThat(result.isDefaultDeny()).isTrue();
        }

        @Test
        @DisplayName("data engineer gets ALLOW_ALL row policy")
        void dataEngineerAllowAll() {
            TableEntitlement result = cache.find(
                    PolicyFixtures.ROLE_DATA_ENGINEER,
                    PolicyFixtures.SILVER_TRANSACTIONS);

            assertThat(result.getRowPolicy().isAllowAll()).isTrue();
            assertThat(result.getColumnPolicy().isAllowAll()).isTrue();
        }
    }

    @Nested
    @DisplayName("findMostPermissive for multi-role user")
    class FindMostPermissive {

        @Test
        @DisplayName("single role returns matching entitlement")
        void singleRole() {
            UserIdentity user = PolicyFixtures.analystUk();
            TableEntitlement result = cache.findMostPermissive(
                    user, PolicyFixtures.SILVER_TRANSACTIONS);

            assertThat(result.isDefaultDeny()).isFalse();
            assertThat(result.getRowPolicy().getPredicate()).contains("UK");
        }

        @Test
        @DisplayName("UNKNOWN identity returns DEFAULT_DENY")
        void unknownIdentityDenied() {
            TableEntitlement result = cache.findMostPermissive(
                    UserIdentity.UNKNOWN,
                    PolicyFixtures.SILVER_TRANSACTIONS);

            assertThat(result.isDefaultDeny()).isTrue();
        }

        @Test
        @DisplayName("user with no matching role gets DEFAULT_DENY")
        void noMatchingRole() {
            UserIdentity user = UserIdentity.builder("dave")
                    .role("undefined_role")
                    .build();

            TableEntitlement result = cache.findMostPermissive(
                    user, PolicyFixtures.SILVER_TRANSACTIONS);

            assertThat(result.isDefaultDeny()).isTrue();
        }

        @Test
        @DisplayName("multi-role user: ALLOW_ALL wins over restricted role")
        void multiRoleAllowAllWins() {
            // Add both analyst_uk (restricted) and data_engineer (allow all) to same user
            UserIdentity multiRole = UserIdentity.builder("power_user")
                    .role(PolicyFixtures.ROLE_ANALYST_UK)
                    .role(PolicyFixtures.ROLE_DATA_ENGINEER)
                    .build();

            // data_engineer has ALLOW_ALL — should win
            TableEntitlement result = cache.findMostPermissive(
                    multiRole, PolicyFixtures.SILVER_TRANSACTIONS);

            assertThat(result.isDefaultDeny()).isFalse();
        }
    }

    @Nested
    @DisplayName("cache statistics")
    class CacheStats {

        @Test
        @DisplayName("cache records hits after repeated lookups")
        void cacheHits() {
            // First call: miss
            cache.find(PolicyFixtures.ROLE_ANALYST_UK, PolicyFixtures.SILVER_TRANSACTIONS);
            // Second call: hit
            cache.find(PolicyFixtures.ROLE_ANALYST_UK, PolicyFixtures.SILVER_TRANSACTIONS);

            assertThat(cache.stats().hitCount()).isGreaterThanOrEqualTo(1);
        }

        @Test
        @DisplayName("isHealthy delegates to backing store")
        void healthDelegates() {
            assertThat(cache.isHealthy()).isTrue();

            MockPolicyStore unhealthy = new MockPolicyStore().simulateUnhealthy();
            PolicyCacheManager unhealthyCache = new PolicyCacheManager(unhealthy, 1, 100);
            assertThat(unhealthyCache.isHealthy()).isFalse();
        }
    }

    @Nested
    @DisplayName("findAll and findByRole and findByTable")
    class QueryMethods {

        @Test
        @DisplayName("findAll returns all entitlements from backing store")
        void findAll() {
            assertThat(cache.findAll()).isNotEmpty();
        }

        @Test
        @DisplayName("findByRole returns only entries for that role")
        void findByRole() {
            var results = cache.findByRole(PolicyFixtures.ROLE_ANALYST_UK);
            assertThat(results).isNotEmpty();
            assertThat(results).allMatch(e ->
                    e.getRoleName().equals(PolicyFixtures.ROLE_ANALYST_UK));
        }

        @Test
        @DisplayName("findByTable returns entries across all roles for that table")
        void findByTable() {
            var results = cache.findByTable(PolicyFixtures.SILVER_TRANSACTIONS);
            assertThat(results).isNotEmpty();
            assertThat(results).allMatch(e ->
                    e.getTableName().equals(PolicyFixtures.SILVER_TRANSACTIONS));
        }
    }
}