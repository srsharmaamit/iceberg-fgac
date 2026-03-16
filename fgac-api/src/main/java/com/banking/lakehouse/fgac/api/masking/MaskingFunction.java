package com.banking.lakehouse.fgac.api.masking;

/**
 * SPI for a pluggable column masking function.
 *
 * <p>Implementations live in {@code fgac-masking} and are registered in
 * the {@code MaskingFunctionRegistry}. Engine adapters register each
 * implementation as a native UDF in their engine's UDF system:
 * <ul>
 *   <li>Spark: registered via {@code SparkSessionExtensions.injectFunction()}</li>
 *   <li>Trino: registered as a {@code @ScalarFunction}-annotated plugin class
 *       whose {@code evaluate()} delegates to {@link #apply(String)}</li>
 *   <li>Flink: registered as a {@code ScalarFunction} subclass</li>
 * </ul>
 *
 * <p>The masking logic is written <em>once</em> in {@code fgac-masking}
 * as a pure Java method. Engine wrappers are trivial boilerplate.
 *
 * <h3>Contract</h3>
 * <ul>
 *   <li>Must be pure (no side effects, no I/O)</li>
 *   <li>Must be null-safe — a null input must return null</li>
 *   <li>Must be deterministic — same input always produces same output</li>
 *   <li>Must be thread-safe — instances are shared across threads</li>
 * </ul>
 */
public interface MaskingFunction {

    /**
     * Applies the masking transformation to a single column value.
     *
     * @param value the raw column value; may be null
     * @return the masked value, or null if input was null
     */
    String apply(String value);

    /**
     * Unique identifier used to reference this function in {@code ColumnPolicy}.
     * Must be stable across releases — policy rows in Nessie reference this id.
     * Convention: lowercase with underscores, e.g. "mask_account_number".
     */
    String functionId();

    /**
     * Human-readable description of what this function does.
     * Shown in admin UI and audit records.
     */
    String description();

    /**
     * The data type of the column this function is designed for.
     * Advisory only — the function may be applied to any STRING column.
     */
    default String targetDataType() { return "STRING"; }
}