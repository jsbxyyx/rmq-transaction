package com.github.jsbxyyx.transaction.rmq.dialect;

/**
 * SPI for pagination dialect. Implement this interface and register it in
 * META-INF/services/com.github.jsbxyyx.transaction.rmq.dialect.MqMsgDialect
 * to support additional databases.
 *
 * @author jsbxyyx
 * @since 1.0.0
 */
public interface MqMsgDialect {

    /**
     * Returns true if this dialect handles the given database product name
     * (as returned by {@code DatabaseMetaData.getDatabaseProductName()}).
     */
    boolean supports(String databaseProductName);

    /**
     * Appends the pagination clause to the base query.
     * The returned SQL must accept one additional {@code ?} bind parameter for the row limit.
     */
    String applyLimit(String sql);

    /**
     * Higher order wins when multiple dialects support the same database.
     * Built-in fallback uses {@code Integer.MIN_VALUE}; custom implementations
     * should use 0 or higher to take precedence.
     */
    default int getOrder() {
        return 0;
    }

}
