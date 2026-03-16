package com.banking.lakehouse.fgac.api.identity;

/**
 * SPI: resolves a {@link UserIdentity} from an engine-specific context object.
 *
 * <p>Each engine adapter provides its own implementation:
 * <ul>
 *   <li>{@code SparkIdentityResolver} reads {@code spark.app.current_user}
 *       from the active {@code SparkConf}.</li>
 *   <li>{@code TrinoIdentityResolver} reads the authenticated {@code Identity}
 *       from Trino's {@code SystemAccessControl} context.</li>
 *   <li>{@code StaticIdentityResolver} is used in tests and CLI tools.</li>
 * </ul>
 *
 * <p>Implementations must be thread-safe. The {@code resolve} method is called
 * once per query plan optimisation — it should be fast (sub-millisecond).
 * Role and attribute resolution from a backing store should be cached inside
 * the implementation, not performed on every call.
 *
 * @param <C> engine context type (e.g. {@code SparkContext}, {@code ConnectorSession})
 */
public interface IdentityResolver<C> {

    /**
     * Resolves the {@link UserIdentity} for the given engine context.
     *
     * <p>Must never return {@code null}. Return {@link UserIdentity#UNKNOWN}
     * if the identity cannot be determined — the framework will apply deny-all.
     *
     * @param context the engine-specific context carrying identity information
     * @return resolved identity, never null
     */
    UserIdentity resolve(C context);

    /**
     * Returns the engine type name this resolver handles.
     * Used for logging and metrics only.
     */
    String engineName();
}