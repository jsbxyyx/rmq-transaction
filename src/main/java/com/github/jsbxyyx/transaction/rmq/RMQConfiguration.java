package com.github.jsbxyyx.transaction.rmq;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.github.jsbxyyx.transaction.rmq.schedule.MqMsgSchedule;

@Configuration
public class RMQConfiguration {
    
    @Bean
    public MqMsgSchedule mqMsgSchedule() {
        return new MqMsgSchedule();
    }
    
    @Bean
    public SpringContextUtils springContextUtils() {
        return new SpringContextUtils();
    }

}
