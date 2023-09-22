package com.github.jsbxyyx.transaction.rmq;

import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;

/**
 * @author jsbxyyx
 * @since 1.0.0
 */
public class SpringContextUtils implements ApplicationContextAware {
    
    private static final Logger log = LoggerFactory.getLogger(SpringContextUtils.class);
    
    private static ApplicationContext context;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
        log.info("=== rmq transaction ApplicationContextUtils init ===");
    }
    
    public static ApplicationContext getApplicationContext() {
        return Objects.requireNonNull(context);
    }


    public static Object getBean(String name) {
        return getApplicationContext().getBean(name);
    }

    public static <T> T getBean(Class<T> clazz) {
        return getApplicationContext().getBean(clazz);
    }

    public static <T> T getBean(String name, Class<T> clazz) {
        return getApplicationContext().getBean(name, clazz);
    }

    public static String findBeanName(Class<?> clazz, Object obj) {
        Map<String, ?> beans = getApplicationContext().getBeansOfType(clazz);
        for (Map.Entry<String, ?> entry : beans.entrySet()) {
            Object value = entry.getValue();
            if (value == obj) {
                return entry.getKey();
            }
        }
        return null;
    }


}
