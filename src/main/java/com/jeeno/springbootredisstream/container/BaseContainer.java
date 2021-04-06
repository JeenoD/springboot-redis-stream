package com.jeeno.springbootredisstream.container;

import io.lettuce.core.RedisBusyException;
import io.lettuce.core.RedisCommandExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collections;

/**
 * @author JEENO
 * @version 0.0.1
 * @date 2021/4/2 16:26
 */
@Slf4j
@Component
public abstract class BaseContainer {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 生成实际的
     * @param factory 工厂
     * @return StreamMessageListenerContainer
     */
    StreamMessageListenerContainer<String, ?> generate(RedisConnectionFactory factory) {
        // 初始化redis stream 的消费组
        initializeGroup();
        return generateContainer(factory);
    }

    /**
     * 初始化redis stream 的消费组
     */
    protected void initializeGroup() {}

    /**
     * generateContainer
     * @param factory 工厂
     * @return StreamMessageListenerContainer
     */
    protected abstract StreamMessageListenerContainer<String, ?> generateContainer(RedisConnectionFactory factory);

    /**
     * 初始化stream key 的消费组
     * @param key key
     * @param group 消费组
     */
    void createConsumerGroup(String key, String group) {
        try {
            stringRedisTemplate.opsForStream().createGroup(key, group);
        } catch (RedisSystemException e) {
            Class errorClass = e.getRootCause()==null?null:e.getRootCause().getClass();
            if (RedisBusyException.class.equals(errorClass)) {
                log.info("STREAM - Redis group already exists, skipping Redis group creation");
            } else if (RedisCommandExecutionException.class.equals(errorClass)) {
                log.info("STREAM - Stream does not yet exist, creating empty stream");
                stringRedisTemplate.opsForStream().add(key, Collections.singletonMap("", ""));
                stringRedisTemplate.opsForStream().createGroup(key, group);
            } else {
                throw e;
            }
        }
    }
}
