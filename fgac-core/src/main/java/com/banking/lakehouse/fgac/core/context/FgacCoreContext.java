package com.banking.lakehouse.fgac.core.context;

import com.banking.lakehouse.fgac.api.audit.AuditEventEmitter;
import com.banking.lakehouse.fgac.api.policy.EntitlementPolicyStore;
import com.banking.lakehouse.fgac.core.expression.IcebergExpressionBuilder;
import com.banking.lakehouse.fgac.core.policy.PolicyCacheManager;
import com.banking.lakehouse.fgac.core.policy.ColumnPolicyEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages the FGAC Spring {@link ApplicationContext} lifecycle.
 *
 * <h3>Driver vs Executor</h3>
 * The Spring context is ONLY created on the Spark <em>driver</em> (or in the
 * engine-equivalent query coordinator). It is never serialised to or started
 * on Spark executors. Engine adapters retrieve the pre-built, immutable
 * {@link PolicyCacheManager} and {@link IcebergExpressionBuilder} from this
 * context on the driver, then pass their outputs (Expression objects, column
 * lists) to the executor via the Catalyst plan — which is serialisation-safe.
 *
 * <h3>Lifecycle</h3>
 * <ol>
 *   <li>The engine adapter calls {@link #initialise(Class)} once when the
 *       Spark session (or Trino plugin) starts.</li>
 *   <li>{@link #get()} provides access to the singleton context.</li>
 *   <li>{@link #close()} is called on session shutdown to flush the
 *       audit emitter and release resources.</li>
 * </ol>
 *
 * <p>Thread-safe: uses an {@link AtomicReference} for the context instance.
 */
public final class FgacCoreContext {

    private static final Logger log = LoggerFactory.getLogger(FgacCoreContext.class);
    private static final AtomicReference<FgacCoreContext> INSTANCE = new AtomicReference<>();

    private final ApplicationContext springContext;

    private FgacCoreContext(ApplicationContext springContext) {
        this.springContext = springContext;
    }

    /**
     * Initialises the FGAC context from a Spring {@code @Configuration} class.
     * Idempotent — subsequent calls return the existing context.
     *
     * @param configClass a {@code @Configuration}-annotated class that defines
     *                    the beans for the backing policy store, audit emitter,
     *                    and masking registry appropriate for the deployment.
     */
    public static FgacCoreContext initialise(Class<?>... configClass) {
        FgacCoreContext existing = INSTANCE.get();
        if (existing != null) {
            log.debug("FgacCoreContext already initialised — reusing");
            return existing;
        }

        log.info("Initialising FGAC core context with config: {}",
                java.util.Arrays.toString(configClass));

        var ctx = new AnnotationConfigApplicationContext(configClass);
        ctx.registerShutdownHook();
        var fgacCtx = new FgacCoreContext(ctx);

        if (!INSTANCE.compareAndSet(null, fgacCtx)) {
            // Another thread initialised concurrently — close ours and return theirs
            ctx.close();
            return INSTANCE.get();
        }

        log.info("FGAC core context initialised. Policy store: {}, Audit emitter: {}",
                fgacCtx.getPolicyCacheManager().storeType(),
                fgacCtx.getAuditEmitter().emitterType());

        return fgacCtx;
    }

    /** Returns the singleton context. Throws if not yet initialised. */
    public static FgacCoreContext get() {
        FgacCoreContext ctx = INSTANCE.get();
        if (ctx == null) {
            throw new IllegalStateException(
                    "FgacCoreContext has not been initialised. "
                            + "Call FgacCoreContext.initialise() at engine startup.");
        }
        return ctx;
    }

    /** Returns true if the context has been initialised. */
    public static boolean isInitialised() { return INSTANCE.get() != null; }

    // ── Bean accessors ────────────────────────────────────────────────────

    public PolicyCacheManager getPolicyCacheManager() {
        return springContext.getBean(PolicyCacheManager.class);
    }

    public IcebergExpressionBuilder getExpressionBuilder() {
        return springContext.getBean(IcebergExpressionBuilder.class);
    }

    public ColumnPolicyEngine getColumnPolicyEngine() {
        return springContext.getBean(ColumnPolicyEngine.class);
    }

    public AuditEventEmitter getAuditEmitter() {
        return springContext.getBean(AuditEventEmitter.class);
    }

    public EntitlementPolicyStore getPolicyStore() {
        return springContext.getBean(EntitlementPolicyStore.class);
    }

    /** Generic bean accessor for extension points. */
    public <T> T getBean(Class<T> type) {
        return springContext.getBean(type);
    }

    /** Flush audit queue and close the Spring context. */
    public void close() {
        try {
            getAuditEmitter().flush();
        } catch (Exception e) {
            log.warn("Error flushing audit emitter on shutdown: {}", e.getMessage());
        }
        if (springContext instanceof AnnotationConfigApplicationContext ctx) {
            ctx.close();
        }
        INSTANCE.compareAndSet(this, null);
        log.info("FGAC core context closed");
    }
}