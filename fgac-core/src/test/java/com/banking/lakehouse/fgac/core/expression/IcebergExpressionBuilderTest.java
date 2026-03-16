package com.banking.lakehouse.fgac.core.expression;

import com.banking.lakehouse.fgac.api.entitlement.RowPolicy;
import com.banking.lakehouse.fgac.api.exception.FgacException.FgacExpressionException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("IcebergExpressionBuilder")
class IcebergExpressionBuilderTest {

    private IcebergExpressionBuilder builder;

    @BeforeEach
    void setUp() {
        builder = new IcebergExpressionBuilder();
    }

    @Nested
    @DisplayName("sentinel policies")
    class Sentinels {

        @Test
        @DisplayName("ALLOW_ALL returns alwaysTrue()")
        void allowAllReturnsTrue() {
            Expression expr = builder.build(RowPolicy.ALLOW_ALL);
            assertThat(expr).isEqualTo(Expressions.alwaysTrue());
        }

        @Test
        @DisplayName("DENY_ALL returns alwaysFalse()")
        void denyAllReturnsFalse() {
            Expression expr = builder.build(RowPolicy.DENY_ALL);
            assertThat(expr).isEqualTo(Expressions.alwaysFalse());
        }
    }

    @Nested
    @DisplayName("equality predicates")
    class EqualityPredicates {

        @Test
        @DisplayName("single equality: jurisdiction = 'UK'")
        void singleEquality() {
            Expression expr = builder.build(RowPolicy.of("jurisdiction = 'UK'"));
            assertThat(expr.toString()).contains("jurisdiction");
            assertThat(expr.toString()).contains("UK");
        }

        @Test
        @DisplayName("single not-equal: status != 'CANCELLED'")
        void notEqual() {
            Expression expr = builder.build(RowPolicy.of("status != 'CANCELLED'"));
            assertThat(expr.toString()).contains("status");
        }
    }

    @Nested
    @DisplayName("IN predicates")
    class InPredicates {

        @Test
        @DisplayName("IN with multiple values")
        void inMultipleValues() {
            Expression expr = builder.build(
                    RowPolicy.of("jurisdiction IN ('DE','FR','NL','ES')"));
            assertThat(expr.toString()).containsAnyOf("DE", "FR");
        }

        @Test
        @DisplayName("IN with single value collapses to equality")
        void inSingleValue() {
            Expression expr = builder.build(RowPolicy.of("jurisdiction IN ('UK')"));
            assertThat(expr.toString()).contains("jurisdiction");
            assertThat(expr.toString()).contains("UK");
        }

        @Test
        @DisplayName("NOT IN predicate")
        void notIn() {
            Expression expr = builder.build(
                    RowPolicy.of("jurisdiction NOT IN ('UNKNOWN','TEST')"));
            assertThat(expr).isNotNull();
        }
    }

    @Nested
    @DisplayName("compound predicates")
    class CompoundPredicates {

        @Test
        @DisplayName("AND combines two predicates")
        void andPredicate() {
            Expression expr = builder.build(
                    RowPolicy.of("jurisdiction = 'UK' AND data_classification != 'PII_RESTRICTED'"));
            assertThat(expr.toString()).contains("and");
        }

        @Test
        @DisplayName("OR combines two predicates")
        void orPredicate() {
            Expression expr = builder.build(
                    RowPolicy.of("jurisdiction = 'UK' OR jurisdiction = 'IE'"));
            assertThat(expr.toString()).contains("or");
        }

        @Test
        @DisplayName("nested AND/OR")
        void nestedAndOr() {
            Expression expr = builder.build(RowPolicy.of(
                    "(jurisdiction = 'UK' OR jurisdiction = 'IE') AND data_classification = 'INTERNAL'"));
            assertThat(expr).isNotNull();
        }
    }

    @Nested
    @DisplayName("null predicates")
    class NullPredicates {

        @Test
        @DisplayName("IS NULL")
        void isNull() {
            Expression expr = builder.build(RowPolicy.of("deleted_at IS NULL"));
            assertThat(expr.toString()).contains("deleted_at");
        }

        @Test
        @DisplayName("IS NOT NULL")
        void isNotNull() {
            Expression expr = builder.build(RowPolicy.of("effective_from IS NOT NULL"));
            assertThat(expr.toString()).contains("effective_from");
        }
    }

    @Nested
    @DisplayName("numeric predicates")
    class NumericPredicates {

        @Test
        @DisplayName("greater than numeric")
        void greaterThan() {
            Expression expr = builder.build(RowPolicy.of("transaction_amount > 1000"));
            assertThat(expr).isNotNull();
        }

        @Test
        @DisplayName("less than or equal numeric")
        void lessThanOrEqual() {
            Expression expr = builder.build(RowPolicy.of("risk_score <= 7"));
            assertThat(expr).isNotNull();
        }
    }

    @Nested
    @DisplayName("error cases")
    class ErrorCases {

        @Test
        @DisplayName("unsupported syntax throws FgacExpressionException")
        void unsupportedSyntaxThrows() {
            assertThatThrownBy(() -> builder.build(RowPolicy.of("CALL some_function()")))
                    .isInstanceOf(FgacExpressionException.class)
                    .hasMessageContaining("Unsupported predicate syntax");
        }
    }
}