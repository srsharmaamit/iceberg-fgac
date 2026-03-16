package com.banking.lakehouse.fgac.spark.identity;

import com.banking.lakehouse.fgac.api.identity.IdentityResolver;
import com.banking.lakehouse.fgac.api.identity.UserIdentity;
import com.banking.lakehouse.fgac.core.policy.PolicyCacheManager;
import org.apache.spark.SparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Resolves {@link UserIdentity} from the active {@link SparkContext}.
 *
 * <p>For the PoC, service accounts are the only identity type. The
 * principal ID is read from Spark conf key {@code spark.app.current_user},
 * which is set by the Airflow DAG when submitting the Spark job:
 * <pre>
 *   SparkConf conf = new SparkConf()
 *       .set("spark.app.current_user", "svc_airflow");
 * </pre>
 *
 * <p>The roles for the resolved principal are loaded from the
 * {@link PolicyCacheManager}'s underlying policy store, where each
 * service account has a pre-defined role set.
 *
 * <h3>Extension point</h3>
 * For future interactive use (real users in Jupyter or Trino), replace
 * this with a Kerberos or JWT-backed resolver that reads the principal
 * from the Kerberos ticket or OIDC token rather than a Spark conf key.
 */
public class SparkIdentityResolver implements IdentityResolver<SparkContext> {

    private static final Logger log = LoggerFactory.getLogger(SparkIdentityResolver.class);
    private static final String USER_CONF_KEY = "spark.app.current_user";

    /** Fallback: use the Spark application user (OS user or Kerberos principal). */
    private static final String FALLBACK_CONF_KEY = "spark.app.name";

    private final PolicyCacheManager policyCache;

    // Defined service accounts for PoC — in production load from policy store
    private static final java.util.Map<String, List<String>> SERVICE_ACCOUNT_ROLES =
            java.util.Map.of(
                    "svc_airflow",    List.of("data_engineer"),
                    "svc_spark",      List.of("data_engineer"),
                    "svc_compliance", List.of("compliance_officer")
            );

    public SparkIdentityResolver(PolicyCacheManager policyCache) {
        this.policyCache = policyCache;
    }

    @Override
    public UserIdentity resolve(SparkContext context) {
        String principalId = context.conf().get(USER_CONF_KEY, null);

        if (principalId == null || principalId.isBlank()) {
            // Try Spark's native user
            principalId = context.sparkUser();
        }

        if (principalId == null || principalId.isBlank()) {
            log.warn("Could not resolve user identity from SparkContext — "
                    + "applying UNKNOWN (deny-all). "
                    + "Set '{}' in SparkConf.", USER_CONF_KEY);
            return UserIdentity.UNKNOWN;
        }

        List<String> roles = SERVICE_ACCOUNT_ROLES.getOrDefault(
                principalId, List.of());

        if (roles.isEmpty()) {
            log.warn("No roles defined for principal '{}' — "
                            + "user will see only tables with ALLOW_ALL entitlement",
                    principalId);
        }

        UserIdentity identity = UserIdentity.builder(principalId)
                .roles(roles)
                .attribute("engine", "spark")
                .attribute("appId", context.applicationId())
                .build();

        log.debug("Resolved identity: {}", identity);
        return identity;
    }

    @Override
    public String engineName() { return "spark"; }
}