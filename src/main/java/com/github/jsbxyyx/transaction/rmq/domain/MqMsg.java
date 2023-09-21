package com.github.jsbxyyx.transaction.rmq.domain;

import java.util.Date;

import org.springframework.messaging.support.GenericMessage;

/**
 * @author jsbxyyx
 * @since 1.0.0
 */
public class MqMsg {

    private Long id;
    private String status;
    private String mqTemplateName;
    private String mqDestination;
    private Long mqTimeout;
    // rocketmq-v5 messageDelayTime
    private String mqDelay;
    private String payload;
    private Integer retryTimes;
    private Date gmtCreate;
    private Date gmtModified;

    private GenericMessage message;

    public Long getId() {
        return id;
    }

    public MqMsg setId(Long id) {
        this.id = id;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public MqMsg setStatus(String status) {
        this.status = status;
        return this;
    }

    public String getMqTemplateName() {
        return mqTemplateName;
    }

    public MqMsg setMqTemplateName(String mqTemplateName) {
        this.mqTemplateName = mqTemplateName;
        return this;
    }

    public String getMqDestination() {
        return mqDestination;
    }

    public MqMsg setMqDestination(String mqDestination) {
        this.mqDestination = mqDestination;
        return this;
    }

    public Long getMqTimeout() {
        return mqTimeout;
    }

    public void setMqTimeout(Long mqTimeout) {
        this.mqTimeout = mqTimeout;
    }

    public String getMqDelay() {
        return mqDelay;
    }

    public MqMsg setMqDelay(String mqDelay) {
        this.mqDelay = mqDelay;
        return this;
    }

    public String getPayload() {
        return payload;
    }

    public MqMsg setPayload(String payload) {
        this.payload = payload;
        return this;
    }

    public Integer getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(Integer retryTimes) {
        this.retryTimes = retryTimes;
    }

    public Date getGmtCreate() {
        return gmtCreate;
    }

    public MqMsg setGmtCreate(Date gmtCreate) {
        this.gmtCreate = gmtCreate;
        return this;
    }

    public Date getGmtModified() {
        return gmtModified;
    }

    public MqMsg setGmtModified(Date gmtModified) {
        this.gmtModified = gmtModified;
        return this;
    }

    public GenericMessage getMessage() {
        return message;
    }

    public MqMsg setMessage(GenericMessage message) {
        this.message = message;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("MqMsg{");
        builder.append("id:").append(id).append(", ");
        builder.append("status:").append(status).append(", ");
        builder.append("mqTemplateName:").append(mqTemplateName).append(", ");
        builder.append("mqDestination:").append(mqDestination).append(", ");
        builder.append("mqTimeout:").append(mqTimeout).append(", ");
        builder.append("mqDelay:").append(mqDelay).append(", ");
        builder.append("payload:").append(payload).append(", ");
        builder.append("retryTimes:").append(retryTimes).append(", ");
        builder.append("gmtCreate:").append(gmtCreate).append(", ");
        builder.append("gmtModified:").append(gmtModified);
        builder.append("}");
        return builder.toString();
    }

}
