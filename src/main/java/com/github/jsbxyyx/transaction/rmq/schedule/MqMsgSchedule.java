package com.github.jsbxyyx.transaction.rmq.schedule;

import com.github.jsbxyyx.transaction.rmq.SpringContextUtils;
import com.github.jsbxyyx.transaction.rmq.dao.MqMsgDao;
import com.github.jsbxyyx.transaction.rmq.domain.MqMsg;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.env.Environment;

import javax.sql.DataSource;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author jsbxyyx
 * @since 1.0.0
 */
public class MqMsgSchedule implements InitializingBean, DisposableBean {

    private static final Log log = LogFactory.getLog(MqMsgSchedule.class);

    private static final ScheduledThreadPoolExecutor EXECUTOR_RETRY = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        final AtomicInteger threadCount = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "rmq-transaction-retry-" + threadCount.getAndIncrement() + "-" + r.hashCode());
        }
    }, new ThreadPoolExecutor.DiscardPolicy());

    private static final ScheduledThreadPoolExecutor EXECUTOR_DELETE = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        final AtomicInteger threadCount = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "rmq-transaction-delete-" + threadCount.getAndIncrement() + "-" + r.hashCode());
        }
    }, new ThreadPoolExecutor.DiscardPolicy());

    @Override
    public void afterPropertiesSet() throws Exception {
        Environment env = SpringContextUtils.getApplicationContext().getEnvironment();

        int retryDelay = Integer.parseInt(env.getProperty("rmq.transaction.retry.delay", "5000"));
        EXECUTOR_RETRY.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                retrySendTask();
            }
        }, 0, retryDelay, TimeUnit.MILLISECONDS);

        int deleteDelay = Integer.parseInt(env.getProperty("rmq.transaction.delete.delay", "600000"));
        int deleteInterval = Integer.parseInt(env.getProperty("rmq.transaction.delete.interval", "600000"));
        EXECUTOR_DELETE.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                deletePublishedRecord(System.currentTimeMillis() - deleteInterval);
            }
        }, 0, deleteDelay, TimeUnit.MILLISECONDS);
    }

    @Override
    public void destroy() throws Exception {
        try {
            EXECUTOR_RETRY.shutdown();
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
        try {
            EXECUTOR_DELETE.shutdown();
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
        }
    }

    public void deletePublishedRecord(long gmtCreateBefore) {
        try {
            Map<String, DataSource> beans = SpringContextUtils.getApplicationContext().getBeansOfType(DataSource.class);
            for (Map.Entry<String, DataSource> entry : beans.entrySet()) {
                MqMsgDao.deletePublishedMsg(entry.getValue(), new Date(gmtCreateBefore));
            }
        } catch (Exception e) {
            log.error("delete published task error.", e);
        }
    }

    public void retrySendTask() {
        try {
            Map<String, DataSource> beans = SpringContextUtils.getApplicationContext().getBeansOfType(DataSource.class);
            for (Map.Entry<String, DataSource> entry : beans.entrySet()) {
                List<MqMsg> mqMsgList = MqMsgDao.listMsg(entry.getValue());
                for (MqMsg mqMsg : mqMsgList) {
                    if (mqMsg.getRetryTimes() >= MqMsgDao.MAX_RETRY_TIMES) {
                        if (log.isErrorEnabled()) {
                            log.error("mqMsg retry times reach " + MqMsgDao.MAX_RETRY_TIMES + ", id:[" + mqMsg.getId() + "]");
                        }
                    } else {
                        RocketMQTemplate template = (RocketMQTemplate) SpringContextUtils
                                .getBean(mqMsg.getMqTemplateName());
                        try {
                            SendResult sendResult = template.syncSend(
                                    mqMsg.getMqDestination(),
                                    MqMsgDao.json2Message(mqMsg.getPayload()),
                                    template.getProducer().getSendMsgTimeout(),
                                    mqMsg.getMqDelay() == null ? 0 : Integer.parseInt(mqMsg.getMqDelay())
                            );
                            if (sendResult.getSendStatus() == SendStatus.SEND_OK) {
                                MqMsgDao.updateStatusById(entry.getValue(), MqMsgDao.STATUS_PUBLISHED, mqMsg.getId());
                            } else {
                                MqMsgDao.updateMsgRetryTimes(entry.getValue(), mqMsg.getId());
                                if (log.isErrorEnabled()) {
                                    log.error("[task] mq send message failed. sendStatus:[" + sendResult.getSendStatus() + "]");
                                }
                            }
                        } catch (Exception e) {
                            MqMsgDao.updateMsgRetryTimes(entry.getValue(), mqMsg.getId());
                            if (log.isErrorEnabled()) {
                                log.error("[task] mq send failed. mqMsg:[" + mqMsg + "]", e);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("retry task error.", e);
        }
    }

}
