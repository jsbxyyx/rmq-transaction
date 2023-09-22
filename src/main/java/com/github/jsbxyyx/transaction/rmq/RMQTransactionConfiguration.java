package com.github.jsbxyyx.transaction.rmq;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import com.github.jsbxyyx.transaction.rmq.schedule.MqMsgSchedule;

@Configuration
public class RMQTransactionConfiguration {
    
    @Bean
    @Order(Ordered.HIGHEST_PRECEDENCE)
    public SpringContextUtils springContextUtils() {
        return new SpringContextUtils();
    }
    
    @Bean
    public MqMsgSchedule mqMsgSchedule() {
        return new MqMsgSchedule();
    }

}
