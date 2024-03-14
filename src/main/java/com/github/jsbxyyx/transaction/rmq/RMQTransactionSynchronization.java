package com.github.jsbxyyx.transaction.rmq;

import com.github.jsbxyyx.transaction.rmq.dao.MqMsgDao;
import com.github.jsbxyyx.transaction.rmq.util.MqId;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.messaging.Message;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.util.Map;

/**
 * @author jsbxyyx
 * @since 1.0.0
 */
public class RMQTransactionSynchronization implements TransactionSynchronization {

    private static final Logger log = LoggerFactory.getLogger(RMQTransactionSynchronization.class);

    private DataSource dataSource;
    private ConnectionHolder connectionHolder;
    private Long id;
    private RocketMQTemplate rocketMQTemplate;
    private String destination;
    private Message<Object> message;
    private String messageDelay;

    public RMQTransactionSynchronization(RocketMQTemplate rocketMQTemplate, String destination, //
            Message<Object> message, String messageDelay) {
        this.rocketMQTemplate = rocketMQTemplate;
        this.destination = destination;
        this.message = message;
        this.messageDelay = messageDelay;
    }

    @Override
    public void beforeCompletion() {
    }

    @Override
    public void beforeCommit(boolean readOnly) {
        Map<Object, Object> resourceMap = TransactionSynchronizationManager.getResourceMap();
        for (Map.Entry<Object, Object> entry : resourceMap.entrySet()) {
            Object key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof ConnectionHolder) {
                this.dataSource = (DataSource) key;
                this.connectionHolder = (ConnectionHolder) value;
                break;
            }
        }
        if (connectionHolder == null) {
            log.warn("connectionHolder is null");
            return;
        }
        this.id = MqId.nextId();
        final String mqTemplateName = SpringContextUtils.findBeanName(rocketMQTemplate.getClass(), rocketMQTemplate);
        MqMsgDao.insertMsg(connectionHolder, id, mqTemplateName, destination, message, messageDelay);
    }

    @Override
    public void afterCommit() {
        log.debug("afterCommit {}", TransactionSynchronizationManager.getCurrentTransactionName());
        try {
            SendResult sendResult = rocketMQTemplate.syncSend(destination, message, rocketMQTemplate.getProducer().getSendMsgTimeout(),
                    messageDelay == null ? 0 : Integer.parseInt(messageDelay));
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                log.debug("send message ok. [{}] [{}]", destination, sendResult.getMsgId());
                MqMsgDao.updateStatusById(dataSource, MqMsgDao.STATUS_PUBLISHED, this.id);
            } else {
                log.error("mq send message failed. sendStatus:[{}]", sendResult.getSendStatus());
            }
        } catch (Exception e) {
            log.error("mq send message failed. topic:[{}], message:[{}]", destination, message, e);
        }
    }

    @Override
    public void afterCompletion(int status) {
        log.debug("afterCompletion {} : {}", TransactionSynchronizationManager.getCurrentTransactionName(), status);
        dataSource = null;
        connectionHolder = null;
        id = null;
        rocketMQTemplate = null;
        destination = null;
        message = null;
        messageDelay = null;
    }

    @Override
    public void suspend() {
    }

    @Override
    public void resume() {
    }

    @Override
    public void flush() {
    }

}
