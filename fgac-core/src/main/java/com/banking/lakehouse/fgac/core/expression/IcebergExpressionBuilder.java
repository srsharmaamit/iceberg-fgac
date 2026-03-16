package com.banking.lakehouse.fgac.core.expression;

import com.banking.lakehouse.fgac.api.entitlement.RowPolicy;
import com.banking.lakehouse.fgac.api.exception.FgacException.FgacExpressionException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Translates a {@link RowPolicy} predicate string into a native
 * {@code org.apache.iceberg.expressions.Expression} for use in
 * {@code TableScan.filter()}.
 *
 * <p>This is the most performance-sensitive class in the framework.
 * Expressions are cached by the {@code PolicyCacheManager} — this
 * builder is only called on cache miss (startup + every 5min TTL).
 * It is never called in the per-query hot path.
 *
 * <h3>Supported predicate syntax</h3>
 * <pre>
 *   ALLOW_ALL
 *   DENY_ALL
 *   column = 'value'
 *   column != 'value'
 *   column IN ('v1','v2','v3')
 *   column NOT IN ('v1','v2')
 *   column > value       (numeric)
 *   column >= value
 *   column < value
 *   column <= value
 *   column IS NULL
 *   column IS NOT NULL
 *   expr1 AND expr2      (recursive, arbitrary depth)
 *   expr1 OR expr2
 *   NOT expr
 * </pre>
 *
 * <p>String values must be single-quoted. Column names are case-sensitive
 * and must match the Iceberg schema field names exactly.
 */
@Component
public class IcebergExpressionBuilder {

    private static final Logger log = LoggerFactory.getLogger(IcebergExpressionBuilder.class);

    // Regex patterns for each clause type
    private static final Pattern EQ_PATTERN     = Pattern.compile("^(\\w+)\\s*=\\s*'([^']*)'$");
    private static final Pattern NEQ_PATTERN    = Pattern.compile("^(\\w+)\\s*!=\\s*'([^']*)'$");
    private static final Pattern IN_PATTERN     = Pattern.compile("^(\\w+)\\s+IN\\s*\\(([^)]+)\\)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern NOT_IN_PATTERN = Pattern.compile("^(\\w+)\\s+NOT\\s+IN\\s*\\(([^)]+)\\)$", Pattern.CASE_INSENSITIVE);
    private static final Pattern GT_PATTERN     = Pattern.compile("^(\\w+)\\s*>\\s*(-?[\\d.]+)$");
    private static final Pattern GTE_PATTERN    = Pattern.compile("^(\\w+)\\s*>=\\s*(-?[\\d.]+)$");
    private static final Pattern LT_PATTERN     = Pattern.compile("^(\\w+)\\s*<\\s*(-?[\\d.]+)$");
    private static final Pattern LTE_PATTERN    = Pattern.compile("^(\\w+)\\s*<=\\s*(-?[\\d.]+)$");
    private static final Pattern NULL_PATTERN   = Pattern.compile("^(\\w+)\\s+IS\\s+NULL$", Pattern.CASE_INSENSITIVE);
    private static final Pattern NOT_NULL_PAT   = Pattern.compile("^(\\w+)\\s+IS\\s+NOT\\s+NULL$", Pattern.CASE_INSENSITIVE);

    /**
     * Builds an Iceberg {@code Expression} from a {@link RowPolicy}.
     *
     * @param rowPolicy the row policy whose predicate to translate
     * @return a native Iceberg Expression for scan planning
     * @throws FgacExpressionException if the predicate cannot be parsed
     */
    public Expression build(RowPolicy rowPolicy) {
        if (rowPolicy.isAllowAll()) {
            return Expressions.alwaysTrue();
        }
        if (rowPolicy.isDenyAll()) {
            return Expressions.alwaysFalse();
        }
        try {
            Expression expr = parse(rowPolicy.getPredicate().trim());
            log.debug("Built expression for predicate '{}': {}",
                    rowPolicy.getPredicate(), expr);
            return expr;
        } catch (FgacExpressionException e) {
            throw e;
        } catch (Exception e) {
            throw new FgacExpressionException(rowPolicy.getPredicate(), e);
        }
    }

    // ── Parser ────────────────────────────────────────────────────────────

    private Expression parse(String predicate) {
        predicate = predicate.trim();

        // AND (split on top-level AND, not inside parentheses)
        List<String> andParts = splitTopLevel(predicate, " AND ");
        if (andParts.size() > 1) {
            return andParts.stream()
                    .map(this::parse)
                    .reduce(Expressions::and)
                    .orElse(Expressions.alwaysTrue());
        }

        // OR
        List<String> orParts = splitTopLevel(predicate, " OR ");
        if (orParts.size() > 1) {
            return orParts.stream()
                    .map(this::parse)
                    .reduce(Expressions::or)
                    .orElse(Expressions.alwaysFalse());
        }

        // NOT
        if (predicate.toUpperCase().startsWith("NOT ")) {
            return Expressions.not(parse(predicate.substring(4).trim()));
        }

        // Strip outer parentheses
        if (predicate.startsWith("(") && predicate.endsWith(")")) {
            return parse(predicate.substring(1, predicate.length() - 1));
        }

        return parseSingleClause(predicate);
    }

    private Expression parseSingleClause(String clause) {
        Matcher m;

        // IS NULL
        m = NULL_PATTERN.matcher(clause);
        if (m.matches()) return Expressions.isNull(m.group(1));

        // IS NOT NULL
        m = NOT_NULL_PAT.matcher(clause);
        if (m.matches()) return Expressions.notNull(m.group(1));

        // column = 'value'
        m = EQ_PATTERN.matcher(clause);
        if (m.matches()) return Expressions.equal(m.group(1), m.group(2));

        // column != 'value'
        m = NEQ_PATTERN.matcher(clause);
        if (m.matches()) return Expressions.notEqual(m.group(1), m.group(2));

        // column IN ('v1','v2')
        m = IN_PATTERN.matcher(clause);
        if (m.matches()) {
            String col    = m.group(1);
            String[] vals = parseInValues(m.group(2));
            if (vals.length == 1) return Expressions.equal(col, vals[0]);
            return Expressions.in(col, (Object[]) vals);
        }

        // column NOT IN ('v1','v2')
        m = NOT_IN_PATTERN.matcher(clause);
        if (m.matches()) {
            String col    = m.group(1);
            String[] vals = parseInValues(m.group(2));
            if (vals.length == 1) return Expressions.notEqual(col, vals[0]);
            return Expressions.notIn(col, (Object[]) vals);
        }

        // Numeric comparisons
        m = GT_PATTERN.matcher(clause);
        if (m.matches()) return Expressions.greaterThan(m.group(1), parseLong(m.group(2)));

        m = GTE_PATTERN.matcher(clause);
        if (m.matches()) return Expressions.greaterThanOrEqual(m.group(1), parseLong(m.group(2)));

        m = LT_PATTERN.matcher(clause);
        if (m.matches()) return Expressions.lessThan(m.group(1), parseLong(m.group(2)));

        m = LTE_PATTERN.matcher(clause);
        if (m.matches()) return Expressions.lessThanOrEqual(m.group(1), parseLong(m.group(2)));

        throw new FgacExpressionException(clause,
                new IllegalArgumentException("Unsupported predicate syntax: '" + clause + "'"));
    }

    /**
     * Splits a predicate string on a delimiter, but only at the top level
     * (not inside nested parentheses).
     */
    private List<String> splitTopLevel(String expr, String delimiter) {
        List<String> parts = new ArrayList<>();
        int depth = 0, start = 0;
        String upper = expr.toUpperCase();
        String delUpper = delimiter.toUpperCase();
        for (int i = 0; i <= expr.length() - delimiter.length(); i++) {
            if (expr.charAt(i) == '(') { depth++; continue; }
            if (expr.charAt(i) == ')') { depth--; continue; }
            if (depth == 0 && upper.startsWith(delUpper, i)) {
                parts.add(expr.substring(start, i).trim());
                start = i + delimiter.length();
                i = start - 1;
            }
        }
        if (parts.isEmpty()) return List.of(expr);
        parts.add(expr.substring(start).trim());
        return parts;
    }

    private String[] parseInValues(String csv) {
        return Arrays.stream(csv.split(","))
                .map(String::trim)
                .map(v -> v.startsWith("'") && v.endsWith("'")
                        ? v.substring(1, v.length() - 1) : v)
                .toArray(String[]::new);
    }

    private long parseLong(String s) {
        try { return Long.parseLong(s.trim()); }
        catch (NumberFormatException e) {
            return (long) Double.parseDouble(s.trim());
        }
    }
}