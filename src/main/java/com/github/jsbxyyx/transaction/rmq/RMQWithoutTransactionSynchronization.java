package com.github.jsbxyyx.transaction.rmq;

import com.github.jsbxyyx.transaction.rmq.dao.MqMsgDao;
import com.github.jsbxyyx.transaction.rmq.util.MqId;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.messaging.Message;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * @author jsbxyyx
 * @since 1.0.0
 */
public class RMQWithoutTransactionSynchronization implements TransactionSynchronization {

    protected final Log log = LogFactory.getLog(getClass());

    private DataSource dataSource;
    private Long id;
    private RocketMQTemplate rocketMQTemplate;
    private String destination;
    private Message<Object> message;
    private String messageDelay;

    public RMQWithoutTransactionSynchronization(RocketMQTemplate rocketMQTemplate, String destination, //
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
        this.dataSource = SpringContextUtils.getBean(DataSource.class);
        this.id = MqId.nextId();
        final String mqTemplateName = SpringContextUtils.findBeanName(rocketMQTemplate.getClass(), rocketMQTemplate);
        try (Connection connection = dataSource.getConnection()) {
            MqMsgDao.insertMsg(connection, id, mqTemplateName, destination, message, messageDelay);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void afterCommit() {
        if (log.isDebugEnabled()) {
            log.debug("afterCommit " + TransactionSynchronizationManager.getCurrentTransactionName());
        }
        try {
            SendResult sendResult = rocketMQTemplate.syncSend(destination, message,
                    rocketMQTemplate.getProducer().getSendMsgTimeout(),
                    messageDelay == null ? 0 : Integer.parseInt(messageDelay));
            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                if (log.isDebugEnabled()) {
                    log.debug("send message ok. destination:[" + destination + "] msgId:[" + sendResult.getMsgId() + "]");
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
