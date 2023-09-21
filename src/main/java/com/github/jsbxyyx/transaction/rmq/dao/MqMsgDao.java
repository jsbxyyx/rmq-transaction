package com.github.jsbxyyx.transaction.rmq.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.ConnectionHolder;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.GenericMessage;

import com.github.jsbxyyx.transaction.rmq.domain.MqMsg;
import com.github.jsbxyyx.transaction.rmq.util.MqJson;

/**
 * @author jsbxyyx
 * @since 1.0.0
 */
public class MqMsgDao {

    private static final Logger log = LoggerFactory.getLogger(MqMsgDao.class);

    public static final String STATUS_NEW = "NEW";
    public static final Integer MAX_RETRY_TIMES = 5;

    private static final String COMMA = ",";
    private static final String ID = "id";
    private static final String STATUS = "status";
    private static final String MQ_TEMPLATE_NAME = "mq_template_name";
    private static final String MQ_DESTINATION = "mq_destination";
    private static final String MQ_TIMEOUT = "mq_timeout";
    private static final String MQ_DELAY = "mq_delay";
    private static final String PAYLOAD = "payload";
    private static final String RETRY_TIMES = "retry_times";
    private static final String GMT_CREATE = "gmt_create";
    private static final String GMT_MODIFIED = "gmt_modified";

    private static final String ALL_FIELD = ID + COMMA + STATUS + COMMA + //
            MQ_TEMPLATE_NAME + COMMA + MQ_DESTINATION + COMMA + //
            MQ_TIMEOUT + COMMA + MQ_DELAY + COMMA + //
            PAYLOAD + COMMA + RETRY_TIMES + COMMA + //
            GMT_CREATE + COMMA + GMT_MODIFIED;
    private static final String ALL_FIELD_PLACEHOLDER = "?,?,?,?,?,?,?,?,?,?";
    private static final String TABLE = "tb_mq_msg";

    private static final String SQL_LIST_MSG = "SELECT #{all_field} FROM #{table} WHERE #{status} = ? AND #{retry_times} < ? LIMIT ?"//
            .replace("#{all_field}", ALL_FIELD)//
            .replace("#{table}", TABLE)//
            .replace("#{status}", STATUS)//
            .replace("#{retry_times}", RETRY_TIMES);

    private static final String SQL_INSERT_MSG = "INSERT INTO #{table}(#{all_field}) VALUES (#{all_field_placeholder})"//
            .replace("#{table}", TABLE)//
            .replace("#{all_field}", ALL_FIELD)//
            .replace("#{all_field_placeholder}", ALL_FIELD_PLACEHOLDER);

    private static final String SQL_UPDATE_RETRY_TIMES = "update #{table} set #{retry_times} = #{retry_times} + 1, #{update_time} = ? where #{id} = ?"//
            .replace("#{table}", TABLE)//
            .replace("#{retry_times}", RETRY_TIMES)//
            .replace("#{id}", ID);
    
    private static final String SQL_DELETE_MSG = "DELETE FROM #{table} where #{id} = ?" //
            .replace("#{table}", TABLE) //
            .replace("#{id}", ID);

    public static List<MqMsg> listMsg(DataSource dataSource) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(SQL_LIST_MSG);
            int i = 0;
            ps.setObject(++i, STATUS_NEW);
            ps.setObject(++i, MAX_RETRY_TIMES);
            ps.setObject(++i, 100);
            rs = ps.executeQuery();
            List<MqMsg> list = new ArrayList<>(100);
            while (rs.next()) {
                MqMsg mqMsg = new MqMsg();
                mqMsg.setId(rs.getLong(ID));
                mqMsg.setStatus(rs.getString(STATUS));
                mqMsg.setMqTemplateName(rs.getString(MQ_TEMPLATE_NAME));
                mqMsg.setMqDestination(rs.getString(MQ_DESTINATION));
                mqMsg.setMqTimeout(rs.getLong(MQ_TIMEOUT));
                mqMsg.setMqDelay(rs.getString(MQ_DELAY));
                Map<String, Object> map = MqJson.fromJson(rs.getString(PAYLOAD));
                GenericMessage<Object> message = new GenericMessage<>(map.get("payload"),
                        (Map<String, Object>) map.get("headers"));
                mqMsg.setMessage(message);
                mqMsg.setRetryTimes(rs.getInt(RETRY_TIMES));
                mqMsg.setGmtCreate(rs.getTimestamp(GMT_CREATE));
                mqMsg.setGmtModified(rs.getTimestamp(GMT_MODIFIED));
                list.add(mqMsg);
            }
            return list;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            close(rs, ps, conn);
        }
    }

    public static void insertMsg(ConnectionHolder connectionHolder, long id, String mqTemplateName,
            String mqDestination, Message message, String messageDelay) {
        Connection connection = connectionHolder.getConnection();
        PreparedStatement ps = null;
        Map<String, Object> payload = new HashMap<>();
        payload.put("payload", message.getPayload());
        payload.put("headers", message.getHeaders());

        try {
            ps = connection.prepareStatement(SQL_INSERT_MSG);
            Date now = new Date();
            int i = 0;
            ps.setObject(++i, id);
            ps.setObject(++i, STATUS_NEW);
            ps.setObject(++i, mqTemplateName);
            ps.setObject(++i, mqDestination);
            ps.setObject(++i, 0);
            ps.setObject(++i, messageDelay == null ? 0 : messageDelay);
            ps.setObject(++i, MqJson.toJson(payload));
            ps.setObject(++i, 0);
            ps.setObject(++i, now);
            ps.setObject(++i, now);
            int affect = ps.executeUpdate();
            if (affect <= 0) {
                throw new RuntimeException("insert mq msg affect : " + affect);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            close(ps);
        }
    }

    public static void updateMsgRetryTimes(DataSource dataSource, Long id) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(SQL_UPDATE_RETRY_TIMES);
            int i = 0;
            ps.setObject(++i, new Date());
            ps.setObject(++i, id);
            int affect = ps.executeUpdate();
            if (affect <= 0) {
                log.error("update mq msg retry_times failed. id:[{}]", id);
                throw new RuntimeException("update mq msg retry_times failed. id:" + id);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            close(ps, conn);
        }
    }

    public static void deleteMsgById(DataSource dataSource, Long id) {
        Connection conn = null;
        PreparedStatement ps = null;
        try {
            conn = dataSource.getConnection();
            ps = conn.prepareStatement(SQL_DELETE_MSG);
            int i = 0;
            ps.setObject(++i, id);
            int affect = ps.executeUpdate();
            if (affect <= 0) {
                log.error("delete mq msg failed. id:[{}]", id);
                throw new RuntimeException("delete mq msg failed. id:" + id);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            close(ps, conn);
        }
    }

    private static void close(AutoCloseable... closeables) {
        if (closeables != null && closeables.length > 0) {
            for (AutoCloseable closeable : closeables) {
                if (closeable != null) {
                    try {
                        closeable.close();
                    } catch (Exception ignore) {
                    }
                }
            }
        }
    }

}