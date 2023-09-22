package com.github.jsbxyyx.transaction.rmq;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import com.github.jsbxyyx.transaction.rmq.schedule.MqMsgSchedule;

/**
 * @author jsbxyyx
 * @since 1.0.0
 */
@Configuration
public class RMQTransactionConfiguration {
    
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    @ConditionalOnMissingBean
    public SpringContextUtils springContextUtils() {
        return new SpringContextUtils();
    }
    
    @Bean
    @ConditionalOnMissingBean
    public MqMsgSchedule mqMsgSchedule() {
        return new MqMsgSchedule();
    }

}
