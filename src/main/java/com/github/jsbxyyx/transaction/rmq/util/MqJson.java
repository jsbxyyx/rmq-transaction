package com.github.jsbxyyx.transaction.rmq.util;

import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * @author jsbxyyx
 * @since 1.0.0
 */
public class MqJson {

    private static final JsonMapper json = JsonMapper.builder() //
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES) //
            .disable(SerializationFeature.FAIL_ON_EMPTY_BEANS) //
            .enable(MapperFeature.PROPAGATE_TRANSIENT_MARKER) //
            .build();

    static {
        json.registerModule(new JavaTimeModule());
    }

    public static JsonMapper getJson() {
        return json;
    }

    public static String toJson(Object payload) {
        try {
            return json.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> fromJson(String payload) {
        return fromJson(payload, new TypeReference<Map<String, Object>>() {
        });
    }

    public static <T> T fromJson(String content, TypeReference<T> valueTypeRef) {
        try {
            return json.readValue(content, valueTypeRef);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
