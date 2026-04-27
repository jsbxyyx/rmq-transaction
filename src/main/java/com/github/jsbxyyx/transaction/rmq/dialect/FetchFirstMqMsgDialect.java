package com.github.jsbxyyx.transaction.rmq.dialect;

/**
 * Dialect using {@code FETCH FIRST ? ROWS ONLY} — covers Oracle and DB2.
 *
 * @author jsbxyyx
 * @since 1.0.0
 */
public class FetchFirstMqMsgDialect implements MqMsgDialect {

    @Override
    public boolean supports(String databaseProductName) {
        String name = databaseProductName.toLowerCase();
        return name.contains("oracle") || name.contains("db2");
    }

    @Override
    public String applyLimit(String sql) {
        return sql + " FETCH FIRST ? ROWS ONLY";
    }

}
