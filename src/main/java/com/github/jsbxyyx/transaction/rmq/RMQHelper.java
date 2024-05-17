package com.github.jsbxyyx.transaction.rmq;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.Message;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jsbxyyx
 * @since 1.0.0
 */
public class RMQHelper {

    private static final Log log = LogFactory.getLog(RMQHelper.class);

    private static final ThreadPoolExecutor POOL = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().availableProcessors() * 2,
            60000L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(10000),
            new ThreadFactory() {
                final AtomicInteger ai = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setName(String.format("rmq-transaction-without-%d-%d", ai.getAndIncrement(), r.hashCode()));
                    return t;
                }
            }, new ThreadPoolExecutor.CallerRunsPolicy());

    public static void syncSend(final RocketMQTemplate rocketMQTemplate, final String destination,
                                final Message<Object> message) {
        syncSend(rocketMQTemplate, destination, message, null);
    }

    public static void syncSend(final RocketMQTemplate rocketMQTemplate, final String destination,
                                final Message<Object> message, final String messageDelay) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(
                    new RMQTransactionSynchronization(rocketMQTemplate, destination, message, messageDelay));
        } else {
            if (log.isInfoEnabled()) {
                log.info("MQ Transaction synchronization is not active");
            }
            RMQWithoutTransactionSynchronization synchronization = new RMQWithoutTransactionSynchronization(
                    rocketMQTemplate, destination, message, messageDelay
            );
            POOL.submit(new Runnable() {
                @Override
                public void run() {
                    int status = TransactionSynchronization.STATUS_UNKNOWN;
                    try {
                        synchronization.beforeCommit(false);
                        synchronization.afterCommit();
                        status = TransactionSynchronization.STATUS_COMMITTED;
                    } finally {
                        synchronization.afterCompletion(status);
                    }
                }
            });
            if (log.isDebugEnabled()) {
                log.debug("submit task in pool");
            }
        }
    }

}
