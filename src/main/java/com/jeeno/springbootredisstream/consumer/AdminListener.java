package com.jeeno.springbootredisstream.consumer;

import com.jeeno.springbootredisstream.pojo.AdminDO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

/**
 * @author JEENO
 * @version 0.0.1
 * @date 2021/4/2 14:16
 */
@Slf4j
@Component
public class AdminListener implements StreamListener<String, ObjectRecord<String, AdminDO>> {

    @Override
    public void onMessage(ObjectRecord<String, AdminDO> message) {
        // 消息ID
        RecordId messageId = message.getId();

        AdminDO value = message.getValue();
        log.info("# consumer # messageId={}, stream={}, value = {}", messageId, message.getStream(), value);
    }
}