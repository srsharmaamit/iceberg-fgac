package com.banking.lakehouse.fgac.masking.library;

/**
 * Library of pure static masking functions.
 *
 * <p>These are the actual masking implementations. They are deliberately
 * written as static methods with no framework dependency so that any engine
 * adapter can wrap them in its native UDF system without any classpath coupling.
 *
 * <h3>Contract (all methods)</h3>
 * <ul>
 *   <li>Null-safe: null input → null output</li>
 *   <li>Deterministic: same input always produces same output</li>
 *   <li>Pure: no I/O, no side effects</li>
 *   <li>Thread-safe: trivially, as static methods with no shared state</li>
 * </ul>
 *
 * <p>Engine adapters call these methods from UDF wrappers:
 * <pre>
 *   // Spark UDF wrapper (in fgac-spark-adapter):
 *   spark.udf().register("mask_account_number",
 *       (String v) -> MaskingFunctionLibrary.maskAccountNumber(v),
 *       DataTypes.StringType);
 * </pre>
 */
public final class MaskingFunctionLibrary {

    private MaskingFunctionLibrary() { /* static utility */ }

    // ── Account number masking ─────────────────────────────────────────────

    /**
     * Format-preserving partial masking: show only the last {@code visibleDigits}
     * characters, replace the rest with '*'.
     *
     * <pre>
     *   "1234567890123456" → "************3456"  (visibleDigits=4)
     *   "GB29NWBK60161331"  → "************1331"
     * </pre>
     */
    public static String maskAccountNumber(String value) {
        return maskAccountNumber(value, 4);
    }

    public static String maskAccountNumber(String value, int visibleDigits) {
        if (value == null) return null;
        if (value.length() <= visibleDigits) return value;
        return "*".repeat(value.length() - visibleDigits)
                + value.substring(value.length() - visibleDigits);
    }

    // ── Email masking ──────────────────────────────────────────────────────

    /**
     * Masks the local part of an email address while preserving domain.
     *
     * <pre>
     *   "john.doe@bank.com"   → "j***@bank.com"
     *   "alice@example.co.uk" → "a***@example.co.uk"
     *   "ab@bank.com"         → "a*@bank.com"
     * </pre>
     */
    public static String maskEmail(String value) {
        if (value == null) return null;
        int at = value.indexOf('@');
        if (at <= 0) return "***";
        String local  = value.substring(0, at);
        String domain = value.substring(at);
        if (local.length() == 1) return local + domain;
        return local.charAt(0) + "*".repeat(local.length() - 1) + domain;
    }

    // ── National ID / SSN / passport ──────────────────────────────────────

    /**
     * Full redaction — returns the literal string {@code "REDACTED"}.
     * Used for national ID numbers, passports, SSNs.
     * Full redaction (not partial) because any visible fragment of a
     * national ID number is a regulatory violation in most jurisdictions.
     */
    public static String redactNationalId(String value) {
        return value == null ? null : "REDACTED";
    }

    // ── Phone number masking ───────────────────────────────────────────────

    /**
     * Keeps only the last 2 digits visible, replaces the rest with '*'.
     * Country code digits (if present) are also masked.
     *
     * <pre>
     *   "+44 7911 123456" → "**************56"
     *   "07911123456"     → "*********56"
     * </pre>
     */
    public static String maskPhone(String value) {
        if (value == null) return null;
        // Strip non-digit characters for length calculation, then re-apply
        String stripped = value.replaceAll("[^\\d]", "");
        if (stripped.length() <= 2) return value;
        int visibleFrom = stripped.length() - 2;
        StringBuilder result = new StringBuilder();
        int digitsSeen = 0;
        for (char c : value.toCharArray()) {
            if (Character.isDigit(c)) {
                result.append(digitsSeen >= visibleFrom ? c : '*');
                digitsSeen++;
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }

    // ── Date of birth ─────────────────────────────────────────────────────

    /**
     * Retains birth year only; masks month and day.
     *
     *
            */
    public static String maskDateOfBirth(String value) {
        if (value == null) return null;
        // ISO format: YYYY-MM-DD
        if (value.matches("\\d{4}-\\d{2}-\\d{2}")) {
            return value.substring(0, 4) + "-**-**";
        }
        // DD/MM/YYYY format
        if (value.matches("\\d{2}/\\d{2}/\\d{4}")) {
            return "**/**/" + value.substring(6);
        }
        // Unknown format — fully redact
        return "REDACTED";
    }

    // ── Sort code (UK banking) ─────────────────────────────────────────────

    /**
     * Masks the first two segments of a UK sort code.
     *
     * <pre>
     *   "20-00-00" → "**-**-00"
     *   "200000"   → "****00"
     * </pre>
     */
    public static String maskSortCode(String value) {
        if (value == null) return null;
        if (value.matches("\\d{2}-\\d{2}-\\d{2}")) {
            return "**-**-" + value.substring(6);
        }
        if (value.matches("\\d{6}")) {
            return "****" + value.substring(4);
        }
        return "REDACTED";
    }

    // ── Generic partial masking ────────────────────────────────────────────

    /**
     * Generic format-preserving mask: show first {@code prefixLen} chars,
     * mask the middle, show last {@code suffixLen} chars.
     *
     * <pre>
     *   genericMask("ABCDEFGHIJ", 2, 2) → "AB******IJ"
     * </pre>
     */
    public static String genericMask(String value, int prefixLen, int suffixLen) {
        if (value == null) return null;
        int len = value.length();
        if (len <= prefixLen + suffixLen) return value;
        return value.substring(0, prefixLen)
                + "*".repeat(len - prefixLen - suffixLen)
                + value.substring(len - suffixLen);
    }
}