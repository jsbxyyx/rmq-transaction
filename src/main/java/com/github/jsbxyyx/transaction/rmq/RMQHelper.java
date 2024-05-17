package com.github.jsbxyyx.transaction.rmq;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.Message;
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
                final AtomicInteger ai = new AtomicInteger();

                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r);
                    t.setName(String.format("rmq-transaction-%d-%d", ai.getAndIncrement(), t.hashCode()));
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
            log.info("MQ Transaction synchronization is not active");
            POOL.submit(new Runnable() {
                @Override
                public void run() {
                    SendResult sendResult = rocketMQTemplate.syncSend(destination, message, rocketMQTemplate.getProducer().getSendMsgTimeout(),
                            messageDelay == null ? 0 : Integer.parseInt(messageDelay));
                    log.info("sendResult => msgId:" + sendResult.getMsgId() + " sendStatus:" + sendResult.getSendStatus());
                }
            });
        }
    }

}
