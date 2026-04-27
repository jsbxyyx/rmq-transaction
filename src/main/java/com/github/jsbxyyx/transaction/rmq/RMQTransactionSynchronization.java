package com.github.jsbxyyx.transaction.rmq;

import com.github.jsbxyyx.transaction.rmq.dao.MqMsgDao;
import com.github.jsbxyyx.transaction.rmq.util.MqId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
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

    protected final Log log = LogFactory.getLog(getClass());

    private DataSource dataSource;
    private ConnectionHolder connectionHolder;
    private String id;
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
            if (log.isWarnEnabled()) {
                log.warn("connectionHolder is null");
            }
            return;
        }
        this.id = MqId.nextId();
        final String mqTemplateName = SpringContextUtils.findBeanName(rocketMQTemplate.getClass(), rocketMQTemplate);
        if (mqTemplateName == null) {
            throw new IllegalStateException("Cannot find bean name for RocketMQTemplate instance, bean may not be managed by Spring");
        }
        MqMsgDao.insertMsg(connectionHolder.getConnection(), id, mqTemplateName, destination, message, messageDelay);
    }

    @Override
    public void afterCommit() {
        if (this.id == null) {
            return;
        }
        if (log.isDebugEnabled()) {
            log.debug("afterCommit " + TransactionSynchronizationManager.getCurrentTransactionName());
        }
        try {
            SendResult sendResult = rocketMQTemplate.syncSend(destination, message, rocketMQTemplate.getProducer().getSendMsgTimeout(),
                    messageDelay == null ? 0 : Integer.parseInt(messageDelay));
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                if (log.isDebugEnabled()) {
                    log.debug("send message ok. destination:[" + destination + "], msgId:[" + sendResult.getMsgId() + "]");
                }
                MqMsgDao.updateStatusById(dataSource, MqMsgDao.STATUS_PUBLISHED, this.id);
            } else {
                if (log.isErrorEnabled()) {
                    log.error("mq send message failed. sendStatus:[" + sendResult.getSendStatus() + "]");
                }
            }
        } catch (Exception e) {
            if (log.isErrorEnabled()) {
                log.error("mq send message failed. topic:[" + destination + "], message:[" + message + "]", e);
            }
        }
    }

    @Override
    public void afterCompletion(int status) {
        if (log.isDebugEnabled()) {
            log.debug("afterCompletion " + TransactionSynchronizationManager.getCurrentTransactionName() + " : " + status);
        }
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
