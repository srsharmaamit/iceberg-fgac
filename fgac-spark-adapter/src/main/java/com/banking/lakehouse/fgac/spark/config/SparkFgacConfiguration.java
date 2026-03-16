package com.banking.lakehouse.fgac.spark.config;

import com.banking.lakehouse.fgac.api.audit.AuditEventEmitter;
import com.banking.lakehouse.fgac.api.policy.EntitlementPolicyStore;
import com.banking.lakehouse.fgac.audit.emitter.AsyncAuditEventEmitter;
import com.banking.lakehouse.fgac.audit.sink.AuditSink;
import com.banking.lakehouse.fgac.audit.sink.LoggingAuditSink;
import com.banking.lakehouse.fgac.core.expression.IcebergExpressionBuilder;
import com.banking.lakehouse.fgac.core.policy.ColumnPolicyEngine;
import com.banking.lakehouse.fgac.core.policy.PolicyCacheManager;
import com.banking.lakehouse.fgac.store.iceberg.IcebergPolicyStore;
import com.banking.lakehouse.fgac.store.seed.SeedPolicyLoader;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.nessie.NessieCatalog;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;

import java.util.HashMap;
import java.util.Map;

/**
 * Spring {@code @Configuration} for the Spark adapter.
 *
 * <p>Wires together all FGAC framework beans for a Spark deployment.
 * The Spring context is created <em>once</em> on the driver by
 * {@link com.banking.lakehouse.fgac.core.context.FgacCoreContext}.
 *
 * <h3>Bean graph</h3>
 * <pre>
 *   ObjectMapper
 *   NessieCatalog (iceberg-nessie)
 *     → IcebergPolicyStore
 *       → SeedPolicyLoader (runs @PostConstruct once)
 *       → PolicyCacheManager (wraps IcebergPolicyStore)
 *   IcebergExpressionBuilder
 *   ColumnPolicyEngine
 *   AuditSink (LoggingAuditSink for PoC, IcebergAuditSink for production)
 *   AsyncAuditEventEmitter (wraps AuditSink)
 * </pre>
 */
@org.springframework.context.annotation.Configuration
@ComponentScan(basePackages = {
        "com.banking.lakehouse.fgac.core",
        "com.banking.lakehouse.fgac.store",
        "com.banking.lakehouse.fgac.audit",
        "com.banking.lakehouse.fgac.masking"
})
@PropertySource(value = {
        "classpath:fgac-defaults.properties",
        "file:${fgac.config.path:config/fgac.properties}",
}, ignoreResourceNotFound = true)
public class SparkFgacConfiguration {

    @Value("${fgac.nessie.uri:http://nessie:19120/api/v1}")
    private String nessieUri;

    @Value("${fgac.nessie.ref:main}")
    private String nessieRef;

    @Value("${fgac.nessie.warehouse:s3a://warehouse/}")
    private String warehouse;

    @Value("${fgac.s3a.endpoint:http://minio:9000}")
    private String s3aEndpoint;

    @Value("${fgac.s3a.access.key:minioadmin}")
    private String s3aAccessKey;

    @Value("${fgac.s3a.secret.key:minioadmin}")
    private String s3aSecretKey;

    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    }

    @Bean
    public Catalog nessieCatalog() {
        NessieCatalog catalog = new NessieCatalog();
        Configuration hadoopConf = new Configuration();

        // S3A / MinIO configuration
        hadoopConf.set("fs.s3a.endpoint",           s3aEndpoint);
        hadoopConf.set("fs.s3a.access.key",          s3aAccessKey);
        hadoopConf.set("fs.s3a.secret.key",          s3aSecretKey);
        hadoopConf.set("fs.s3a.path.style.access",   "true");
        hadoopConf.set("fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem");
        hadoopConf.set("fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("uri",        nessieUri);
        catalogProps.put("ref",        nessieRef);
        catalogProps.put("warehouse",  warehouse);
        catalogProps.put("authentication.type", "NONE");

        catalog.setConf(hadoopConf);
        catalog.initialize("nessie", catalogProps);
        return catalog;
    }

    @Bean
    public AuditSink auditSink(ObjectMapper objectMapper) {
        // PoC: use logging sink. Production: swap for IcebergAuditSink.
        return new LoggingAuditSink(objectMapper);
    }

    @Bean
    public AuditEventEmitter auditEventEmitter(AuditSink sink) {
        return new AsyncAuditEventEmitter(sink, 10000, 200, 10);
    }
}