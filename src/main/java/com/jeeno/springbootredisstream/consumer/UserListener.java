package com.jeeno.springbootredisstream.consumer;

import com.jeeno.springbootredisstream.pojo.UserDO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

/**
 * @author JEENO
 * @version 0.0.1
 * @date 2021/3/31 14:39
 */
@Slf4j
@Component
public class UserListener implements StreamListener<String, ObjectRecord<String, UserDO>> {

    @Override
    public void onMessage(ObjectRecord<String, UserDO> message) {
        // 消息ID
        RecordId messageId = message.getId();

        // 消息的key和value
        UserDO value = message.getValue();
        log.info("# consumer1 # messageId={}, stream={}, value = {}", messageId, message.getStream(), value);
//        // 通过RedisTemplate手动确认消息
//        this.stringRedisTemplate.opsForStream().acknowledge(CONSUMER_GROUP, message);
    }
}
