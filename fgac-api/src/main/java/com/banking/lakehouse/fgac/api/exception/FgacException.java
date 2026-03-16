package com.banking.lakehouse.fgac.api.exception;

import java.io.Serial;

/**
 * Root exception for all FGAC framework errors.
 *
 * <p>The exception hierarchy separates operational failures
 * (policy store unavailable) from authorisation failures
 * (no policy matched → access denied) to allow callers
 * to handle them differently.
 *
 * <pre>
 * FgacException
 *  ├── FgacAccessDeniedException   — policy evaluated, access not granted
 *  ├── FgacPolicyStoreException    — policy store read/write failure
 *  ├── FgacExpressionException     — predicate string could not be parsed
 *  ├── FgacIdentityException       — identity could not be resolved
 *  └── FgacConfigurationException  — framework misconfigured at startup
 * </pre>
 */
public class FgacException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;
    public FgacException(String message) {
        super(message);
    }

    public FgacException(String message, Throwable cause) {
        super(message, cause);
    }

    // ── Subtypes ──────────────────────────────────────────────────────────

    /**
     * Thrown when a policy was evaluated and resulted in deny.
     * The engine adapter translates this to the engine's native
     * access-denied exception.
     */
    public static final class FgacAccessDeniedException extends FgacException {
        @Serial
        private static final long serialVersionUID = 1L;
        private final String principalId;
        private final String tableName;

        public FgacAccessDeniedException(String principalId, String tableName) {
            super("Access denied: principal='" + principalId
                    + "' has no entitlement for table='" + tableName + "'");
            this.principalId = principalId;
            this.tableName   = tableName;
        }

        public String getPrincipalId() { return principalId; }
        public String getTableName()   { return tableName; }
    }

    /**
     * Thrown when the policy store cannot be read or written.
     * The framework falls back to deny-all if this is thrown
     * after the initial warm-up period.
     */
    public static final class FgacPolicyStoreException extends FgacException {
        @Serial
        private static final long serialVersionUID = 1L;
        public FgacPolicyStoreException(String message) { super(message); }
        public FgacPolicyStoreException(String message, Throwable cause) { super(message, cause); }
    }

    /**
     * Thrown when a row policy predicate string cannot be parsed
     * into an Iceberg Expression.
     */
    public static final class FgacExpressionException extends FgacException {
        @Serial
        private static final long serialVersionUID = 1L;
        private final String predicate;

        public FgacExpressionException(String predicate, Throwable cause) {
            //super("Failed to parse row policy predicate: '" + predicate + "'", cause);
            super("Failed to parse row policy predicate: '" + predicate + "': " + cause.getMessage(), cause);
            this.predicate = predicate;
        }

        public String getPredicate() { return predicate; }
    }

    /**
     * Thrown when the IdentityResolver cannot determine the current user.
     */
    public static final class FgacIdentityException extends FgacException {
        @Serial
        private static final long serialVersionUID = 1L;
        public FgacIdentityException(String message) { super(message); }
        public FgacIdentityException(String message, Throwable cause) { super(message, cause); }
    }

    /**
     * Thrown at framework startup when required configuration is missing
     * or invalid.
     */
    public static final class FgacConfigurationException extends FgacException {
        @Serial
        private static final long serialVersionUID = 1L;
        public FgacConfigurationException(String message) { super(message); }
        public FgacConfigurationException(String message, Throwable cause) { super(message, cause); }
    }
}