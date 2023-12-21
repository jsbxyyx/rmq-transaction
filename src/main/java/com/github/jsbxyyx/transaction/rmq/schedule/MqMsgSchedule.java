package com.github.jsbxyyx.transaction.rmq.schedule;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.github.jsbxyyx.transaction.rmq.SpringContextUtils;
import com.github.jsbxyyx.transaction.rmq.dao.MqMsgDao;
import com.github.jsbxyyx.transaction.rmq.domain.MqMsg;

/**
 * @author jsbxyyx
 * @since 1.0.0
 */
public class MqMsgSchedule implements InitializingBean, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(MqMsgSchedule.class);

    private static final ScheduledThreadPoolExecutor EXECUTOR = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
        AtomicInteger threadCount = new AtomicInteger(0);

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "mq-transaction-" + threadCount.getAndIncrement() + "-" + r.hashCode());
        }
    }, new ThreadPoolExecutor.DiscardPolicy());

    @Override
    public void afterPropertiesSet() throws Exception {
        EXECUTOR.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                retrySendTask();
            }
        }, 0, 5000, TimeUnit.MILLISECONDS);
    }

    @Override
    public void destroy() throws Exception {
        EXECUTOR.shutdown();
    }

    public void retrySendTask() {
        try {
            Map<String, DataSource> beans = SpringContextUtils.getApplicationContext().getBeansOfType(DataSource.class);
            for (Map.Entry<String, DataSource> entry : beans.entrySet()) {
                List<MqMsg> mqMsgList = MqMsgDao.listMsg(entry.getValue());
                for (MqMsg mqMsg : mqMsgList) {
                    if (mqMsg.getRetryTimes() >= MqMsgDao.MAX_RETRY_TIMES) {
                        log.error("mqMsg retry times reach {}, id:[{}]", MqMsgDao.MAX_RETRY_TIMES, mqMsg.getId());
                    } else {
                        RocketMQTemplate template = (RocketMQTemplate) SpringContextUtils
                                .getBean(mqMsg.getMqTemplateName());
                        try {
                            template.syncSend( //
                                    mqMsg.getMqDestination(), //
                                    MqMsgDao.json2Message(mqMsg.getPayload()), //
                                    template.getProducer().getSendMsgTimeout(), //
                                    mqMsg.getMqDelay() == null ? 0 : Integer.valueOf(mqMsg.getMqDelay())//
                            );
                            MqMsgDao.deleteMsgById(entry.getValue(), mqMsg.getId());
                        } catch (Exception e) {
                            MqMsgDao.updateMsgRetryTimes(entry.getValue(), mqMsg.getId());
                            log.error("[task] mq send failed. mqMsg:[{}]", mqMsg, e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("task error.", e);
        }
    }

}
