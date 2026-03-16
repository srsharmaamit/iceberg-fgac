package com.banking.lakehouse.fgac.spark.rule;

import com.banking.lakehouse.fgac.api.audit.AuditEvent;
import com.banking.lakehouse.fgac.api.audit.AuditEventEmitter;
import com.banking.lakehouse.fgac.api.entitlement.TableEntitlement;
import com.banking.lakehouse.fgac.api.identity.UserIdentity;
import com.banking.lakehouse.fgac.core.expression.IcebergExpressionBuilder;
import com.banking.lakehouse.fgac.core.policy.ColumnPolicyEngine;
import com.banking.lakehouse.fgac.core.policy.PolicyCacheManager;
import com.banking.lakehouse.fgac.spark.identity.SparkIdentityResolver;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Attribute;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.Literal$;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.Filter;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.catalyst.plans.logical.Project;
import org.apache.spark.sql.catalyst.rules.Rule;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConverters;
import scala.runtime.AbstractPartialFunction;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Catalyst optimizer rule that injects FGAC entitlements into Iceberg table
 * scans in the logical plan.
 *
 * Spark 3.5.x Java interop notes:
 *
 * 1. transformUp requires a Scala PartialFunction — use AbstractPartialFunction,
 *    not a Java lambda (Scala PartialFunction is not a @FunctionalInterface).
 *
 * 2. Filter and Project are Scala case classes. Use their constructors directly
 *    (new Filter(cond, child), new Project(list, child)) — the companion object
 *    apply() methods are not accessible from Java.
 *
 * 3. Literal.FalseLiteral is a Scala val on the companion object. Access it
 *    via Literal$.MODULE$.FalseLiteral().
 *
 * 4. UDFs are registered lazily on the first plan evaluation via
 *    SparkSession.active().udf().register() — avoids the injectFunction
 *    Scala interop complexity entirely.
 */
public class CatalystEntitlementRule extends Rule<LogicalPlan> {

    private static final Logger log = LoggerFactory.getLogger(CatalystEntitlementRule.class);

    private final PolicyCacheManager policyCache;
    private final IcebergExpressionBuilder expressionBuilder;
    private final ColumnPolicyEngine columnPolicyEngine;
    private final AuditEventEmitter auditEmitter;
    private final SparkIdentityResolver identityResolver;
    private final AtomicBoolean udfsRegistered = new AtomicBoolean(false);

    public CatalystEntitlementRule(
            PolicyCacheManager policyCache,
            IcebergExpressionBuilder expressionBuilder,
            ColumnPolicyEngine columnPolicyEngine,
            AuditEventEmitter auditEmitter) {
        this.policyCache        = policyCache;
        this.expressionBuilder  = expressionBuilder;
        this.columnPolicyEngine = columnPolicyEngine;
        this.auditEmitter       = auditEmitter;
        this.identityResolver   = new SparkIdentityResolver(policyCache);
    }

    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        // Register UDFs once per session on first plan evaluation
        ensureUdfsRegistered();

        // FIX: transformUp requires a Scala PartialFunction, not a Java lambda.
        // AbstractPartialFunction is the standard Java bridge for PartialFunction.
        return plan.transformUp(
                new AbstractPartialFunction<LogicalPlan, LogicalPlan>() {
                    @Override
                    public boolean isDefinedAt(LogicalPlan x) { return true; }

                    @Override
                    public LogicalPlan apply(LogicalPlan node) {
                        return rewriteNode(node);
                    }
                });
    }

    // ── Node rewriter ──────────────────────────────────────────────────────

    private LogicalPlan rewriteNode(LogicalPlan node) {
        if (!(node instanceof DataSourceV2Relation relation)) {
            return node;
        }

        String tableIdentifier = extractTableIdentifier(relation);
        if (tableIdentifier == null) return node;

        long startMs = System.currentTimeMillis();
        SparkSession session = SparkSession.active();
        UserIdentity identity = identityResolver.resolve(session.sparkContext());
        TableEntitlement entitlement = policyCache.findMostPermissive(identity, tableIdentifier);

        if (entitlement.isDefaultDeny()) {
            log.warn("FGAC DENY: user='{}' table='{}'", identity.getPrincipalId(), tableIdentifier);
            emitAudit(identity, entitlement, System.currentTimeMillis() - startMs, AuditEvent.Outcome.DENIED);
            // FIX: use new Filter(cond, child) — Filter.apply() is not callable from Java
            // FIX: Literal.FalseLiteral() -> Literal$.MODULE$.FalseLiteral()
            return new Filter(Literal$.MODULE$.FalseLiteral(), relation);
        }

        LogicalPlan result = relation;

        // Row filter
        if (!entitlement.getRowPolicy().isAllowAll()) {
            result = applyRowFilter(result, entitlement, session);
        }

        // Column projection + masking
        List<String> schemaColumns = extractColumnNames(relation);
        ColumnPolicyEngine.ProjectionResult projection =
                columnPolicyEngine.project(schemaColumns, entitlement);

        if (!projection.allowAll()) {
            result = applyColumnProjection(result, projection, relation, session);
        }

        emitAudit(identity, entitlement, System.currentTimeMillis() - startMs, AuditEvent.Outcome.ALLOWED);
        return result;
    }

    // ── Row filter ─────────────────────────────────────────────────────────

    private LogicalPlan applyRowFilter(LogicalPlan plan, TableEntitlement entitlement,
                                       SparkSession session) {
        String predicate = entitlement.getRowPolicy().getPredicate();
        try {
            Expression filterExpr = session.sessionState().sqlParser()
                    .parseExpression(predicate);
            // FIX: new Filter(cond, child) not Filter.apply(cond, child)
            return new Filter(filterExpr, plan);
        } catch (Exception e) {
            log.error("Failed to apply row filter '{}': {} — applying deny-all", predicate, e.getMessage());
            return new Filter(Literal$.MODULE$.FalseLiteral(), plan);
        }
    }

    // ── Column projection ──────────────────────────────────────────────────

    private LogicalPlan applyColumnProjection(LogicalPlan plan,
                                              ColumnPolicyEngine.ProjectionResult projection,
                                              DataSourceV2Relation relation,
                                              SparkSession session) {
        if (projection.denyAll()) {
            return new Filter(Literal$.MODULE$.FalseLiteral(), plan);
        }

        List<NamedExpression> projectList = projection.allowedColumns().stream()
                .map(colName -> {
                    Attribute attr = findAttribute(relation, colName);
                    if (attr == null) return null;
                    if (projection.isColumnMasked(colName)) {
                        return buildMaskExpression(attr, projection.getMaskFunctionId(colName), session);
                    }
                    return (NamedExpression) attr;
                })
                .filter(e -> e != null)
                .collect(Collectors.toList());

        if (projectList.isEmpty()) return plan;

        // FIX: new Project(scalaList, child) not Project.apply(...)
        return new Project(
                JavaConverters.asScalaBuffer(projectList).toList(),
                plan);
    }

    // ── Masking expression ─────────────────────────────────────────────────

    private NamedExpression buildMaskExpression(Attribute attr, String udfName,
                                                SparkSession session) {
        try {
            String sql = udfName + "(`" + attr.name() + "`)";
            Expression udfExpr = session.sessionState().sqlParser().parseExpression(sql);
            return (NamedExpression) session.sessionState().analyzer()
                    .execute(
                            new Project(
                                    JavaConverters.asScalaBuffer(
                                            List.<NamedExpression>of(
                                                    new org.apache.spark.sql.catalyst.expressions.Alias(
                                                            udfExpr, attr.name(),
                                                            org.apache.spark.sql.catalyst.expressions.NamedExpression.newExprId(),
                                                            JavaConverters.asScalaBuffer(List.<String>of()).toSeq(),
                                                            Option.empty(),
                                                            JavaConverters.asScalaBuffer(List.<String>of()).toSeq()
                                                    )
                                            )
                                    ).toList(),
                                    org.apache.spark.sql.catalyst.plans.logical.LocalRelation$.MODULE$.apply(
                                            JavaConverters.asScalaBuffer(List.of(attr)).toSeq()
                                    )
                            )
                    ).output().head();
        } catch (Exception e) {
            log.warn("Could not build mask expression for col='{}' udf='{}' — returning raw attribute: {}",
                    attr.name(), udfName, e.getMessage());
            return attr;
        }
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private String extractTableIdentifier(DataSourceV2Relation relation) {
        try {
            Option<org.apache.spark.sql.connector.catalog.Identifier> idOpt = relation.identifier();
            if (idOpt.isEmpty()) return null;
            var id = idOpt.get();
            String[] ns = id.namespace();
            return (ns.length > 0 ? String.join(".", ns) + "." : "") + id.name();
        } catch (Exception e) {
            return null;
        }
    }

    private List<String> extractColumnNames(DataSourceV2Relation relation) {
        return JavaConverters.seqAsJavaList(relation.output())
                .stream().map(Attribute::name).collect(Collectors.toList());
    }

    private Attribute findAttribute(DataSourceV2Relation relation, String colName) {
        return JavaConverters.seqAsJavaList(relation.output())
                .stream().filter(a -> a.name().equalsIgnoreCase(colName))
                .findFirst().orElse(null);
    }

    private void emitAudit(UserIdentity identity, TableEntitlement entitlement,
                           long durationMs, AuditEvent.Outcome outcome) {
        try {
            auditEmitter.emit(
                    AuditEvent.fromEntitlement(identity, entitlement, "spark")
                            .outcome(outcome)
                            .planningDurationMs(durationMs)
                            .build());
        } catch (Exception e) {
            log.warn("Audit emit failed: {}", e.getMessage());
        }
    }

    // FIX: Register UDFs lazily using SparkSession.udf().register() —
    // avoids the injectFunction Scala interop complexity entirely.
    private void ensureUdfsRegistered() {
        if (udfsRegistered.compareAndSet(false, true)) {
            try {
                SparkSession session = SparkSession.active();
                com.banking.lakehouse.fgac.spark.udf.SparkUdfRegistrar.registerAll(session);
            } catch (Exception e) {
                log.error("Failed to register FGAC masking UDFs: {}", e.getMessage());
                udfsRegistered.set(false); // retry on next plan
            }
        }
    }
}