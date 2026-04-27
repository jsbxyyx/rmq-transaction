package com.github.jsbxyyx.transaction.rmq.dialect;

/**
 * Default dialect using {@code LIMIT ?} — covers MySQL, PostgreSQL, SQLite, H2, etc.
 * Registered as a catch-all fallback with the lowest possible order.
 *
 * @author jsbxyyx
 * @since 1.0.0
 */
public class LimitMqMsgDialect implements MqMsgDialect {

    @Override
    public boolean supports(String databaseProductName) {
        return true;
    }

    @Override
    public String applyLimit(String sql) {
        return sql + " LIMIT ?";
    }

    @Override
    public int getOrder() {
        return Integer.MIN_VALUE;
    }

}
