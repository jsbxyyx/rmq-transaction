package com.github.jsbxyyx.transaction.rmq;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.Message;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * @author jsbxyyx
 * @since 1.0.0
 */
public class RMQHelper {

    public static void syncSend(final RocketMQTemplate rocketMQTemplate, final String destination,
            final Message message) {
        syncSend(rocketMQTemplate, destination, message, null);
    }

    public static void syncSend(final RocketMQTemplate rocketMQTemplate, final String destination,
            final Message message, final String messageDelay) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                    new RMQTransactionSynchronization(rocketMQTemplate, destination, message, messageDelay));
        } else {
            throw new RuntimeException("MQ Transaction synchronization is not active");
        }
    }

}
