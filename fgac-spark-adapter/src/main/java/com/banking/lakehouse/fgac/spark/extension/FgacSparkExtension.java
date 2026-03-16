package com.banking.lakehouse.fgac.spark.extension;

import com.banking.lakehouse.fgac.core.context.FgacCoreContext;
import com.banking.lakehouse.fgac.spark.config.SparkFgacConfiguration;
import com.banking.lakehouse.fgac.spark.rule.CatalystEntitlementRule;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSessionExtensions;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

/**
 * Entry point for the FGAC Spark adapter.
 *
 * Registered via:
 *   spark.sql.extensions=com.banking.lakehouse.fgac.spark.extension.FgacSparkExtension
 *
 * Spark calls apply(SparkSessionExtensions) once during session creation.
 *
 * FIX: injectOptimizerRule takes a Scala Function1[SparkSession, Rule[LogicalPlan]].
 * Java lambdas cannot satisfy Scala function types directly. Use AbstractFunction1
 * which is the standard Java bridge for scala.Function1.
 */
public class FgacSparkExtension
        extends AbstractFunction1<SparkSessionExtensions, BoxedUnit> {

    private static final Logger log = LoggerFactory.getLogger(FgacSparkExtension.class);

    @Override
    public BoxedUnit apply(SparkSessionExtensions extensions) {
        log.info("Registering FGAC Spark extension");

        try {
            // Initialise Spring context on driver — idempotent
            FgacCoreContext ctx = FgacCoreContext.initialise(SparkFgacConfiguration.class);

            // Build the rule once — it is stateless and thread-safe
            CatalystEntitlementRule rule = new CatalystEntitlementRule(
                    ctx.getPolicyCacheManager(),
                    ctx.getExpressionBuilder(),
                    ctx.getColumnPolicyEngine(),
                    ctx.getAuditEmitter());

            // FIX: injectOptimizerRule requires scala.Function1, not a Java lambda.
            // AbstractFunction1 is the correct Java bridge class.
            extensions.injectOptimizerRule(
                    new AbstractFunction1<SparkSession, Rule<LogicalPlan>>() {
                        @Override
                        public Rule<LogicalPlan> apply(SparkSession session) {
                            // UDFs are registered lazily inside the rule on first plan evaluation.
                            // SparkRuleAdapter is no longer needed — the rule IS the Rule<LogicalPlan>.
                            return rule;
                        }
                    });

            log.info("FGAC Spark extension registered. Policy store: {}",
                    ctx.getPolicyCacheManager().storeType());

        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to initialise FGAC Spark extension. "
                            + "Check fgac.nessie.uri and policy store connectivity.", e);
        }

        return BoxedUnit.UNIT;
    }
}
