package com.banking.lakehouse.fgac.spark.udf;

import com.banking.lakehouse.fgac.masking.library.MaskingFunctionLibrary;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Registers all FGAC masking functions as named Spark SQL UDFs.
 *
 * Uses {@code SparkSession.udf().register()} — the simple, stable public API.
 * The original approach using {@code SparkSessionExtensions.injectFunction()}
 * required constructing Scala internals (FunctionRegistryBase.FunctionBuilder,
 * ScalaUDF, Nil$) that are fragile across Spark minor versions.
 *
 * Called once per SparkSession from {@code CatalystEntitlementRule} on first
 * plan evaluation, guarded by an AtomicBoolean.
 */
public final class SparkUdfRegistrar {

    private static final Logger log = LoggerFactory.getLogger(SparkUdfRegistrar.class);

    // UDF names — must match values used in ColumnPolicy.maskedColumns map
    public static final String MASK_ACCOUNT_NUMBER = "fgac_mask_account_number";
    public static final String MASK_EMAIL          = "fgac_mask_email";
    public static final String REDACT_NATIONAL_ID  = "fgac_redact_national_id";
    public static final String MASK_PHONE          = "fgac_mask_phone";
    public static final String MASK_DATE_OF_BIRTH  = "fgac_mask_date_of_birth";
    public static final String MASK_SORT_CODE      = "fgac_mask_sort_code";

    private SparkUdfRegistrar() {}

    /**
     * Registers all masking UDFs on the given SparkSession.
     * Called once per session — idempotent (Spark silently overwrites on re-register).
     */
    public static void registerAll(SparkSession session) {
        session.udf().register(MASK_ACCOUNT_NUMBER,
                (UDF1<String, String>) MaskingFunctionLibrary::maskAccountNumber,
                DataTypes.StringType);

        session.udf().register(MASK_EMAIL,
                (UDF1<String, String>) MaskingFunctionLibrary::maskEmail,
                DataTypes.StringType);

        session.udf().register(REDACT_NATIONAL_ID,
                (UDF1<String, String>) MaskingFunctionLibrary::redactNationalId,
                DataTypes.StringType);

        session.udf().register(MASK_PHONE,
                (UDF1<String, String>) MaskingFunctionLibrary::maskPhone,
                DataTypes.StringType);

        session.udf().register(MASK_DATE_OF_BIRTH,
                (UDF1<String, String>) MaskingFunctionLibrary::maskDateOfBirth,
                DataTypes.StringType);

        session.udf().register(MASK_SORT_CODE,
                (UDF1<String, String>) MaskingFunctionLibrary::maskSortCode,
                DataTypes.StringType);

        log.info("Registered 6 FGAC masking UDFs on SparkSession");
    }
}
