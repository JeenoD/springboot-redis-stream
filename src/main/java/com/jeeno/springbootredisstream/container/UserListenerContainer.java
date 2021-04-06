package com.jeeno.springbootredisstream.container;

import com.jeeno.springbootredisstream.consumer.UserListener;
import com.jeeno.springbootredisstream.pojo.UserDO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.Duration;

/**
 * @author JEENO
 * @version 0.0.1
 * @date 2021/4/2 16:00
 */
@Slf4j
@Component
@SuppressWarnings("unchecked")
public class UserListenerContainer extends BaseContainer{

    @Resource
    private UserListener streamListener;

    /**
     * 对应的stream key
     */
    public static final String STREAM_USER_KEY = "user_stream";

    /**
     * 消费组名称
     */
    private static final String CONSUMER_GROUP = "user_consumer_group";

    /**
     * 消费者名称
     */
    private static final String CONSUMER_NAME = "user-consumer";

    @Override
    protected void initializeGroup() {
        createConsumerGroup(STREAM_USER_KEY, CONSUMER_GROUP);
    }

    @Bean(name = "userContainer")
    public StreamMessageListenerContainer<String, ObjectRecord<String, UserDO>> container(RedisConnectionFactory factory) {
        return (StreamMessageListenerContainer<String, ObjectRecord<String, UserDO>>) generate(factory);
    }

    @Override
    public StreamMessageListenerContainer<String, ObjectRecord<String, UserDO>> generateContainer(RedisConnectionFactory factory) {
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, UserDO>> options = StreamMessageListenerContainer
                .StreamMessageListenerContainerOptions
                .builder()
                // 一直不过期
                .pollTimeout(Duration.ofSeconds(0))
                // 目标类型
                .targetType(UserDO.class)
                // 每次最多取的条数
                .batchSize(10)
                .build();

        StreamMessageListenerContainer<String, ObjectRecord<String, UserDO>> listenerContainer = StreamMessageListenerContainer.create(factory, options);

        // 消费监听配置1
        listenerContainer.receiveAutoAck(Consumer.from(CONSUMER_GROUP, CONSUMER_NAME),
                StreamOffset.create(STREAM_USER_KEY, ReadOffset.lastConsumed()), streamListener);
        return listenerContainer;
    }
}
