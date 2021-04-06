package com.jeeno.springbootredisstream.container;

import com.jeeno.springbootredisstream.consumer.AdminListener;
import com.jeeno.springbootredisstream.pojo.AdminDO;
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
 * @date 2021/4/2 16:10
 */
@Slf4j
@Component
@SuppressWarnings("unchecked")
public class AdminListenerContainer extends BaseContainer{

    /**
     * 对应的stream key
     */
    public static final String STREAM_ADMIN_KEY = "admin_stream";

    /**
     * 消费组名称
     */
    private static final String CONSUMER_GROUP = "admin_consumer_group";

    /**
     * 消费者名称
     */
    private static final String CONSUMER_NAME = "admin-consumer";

    @Resource
    private AdminListener adminListener;

    @Override
    protected void initializeGroup() {
        createConsumerGroup(STREAM_ADMIN_KEY, CONSUMER_GROUP);
    }

    @Bean(name = "adminContainer")
    public StreamMessageListenerContainer<String, ObjectRecord<String, AdminDO>> adminListenerContainer(RedisConnectionFactory factory){
        return (StreamMessageListenerContainer<String, ObjectRecord<String, AdminDO>>) generate(factory);
    }

    @Override
    public StreamMessageListenerContainer<String, ObjectRecord<String, AdminDO>> generateContainer(RedisConnectionFactory factory) {
        // 管理员监听配置
        StreamMessageListenerContainer.StreamMessageListenerContainerOptions<String, ObjectRecord<String, AdminDO>> adminOptions = StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                .builder()
                // 一直不过期
                .pollTimeout(Duration.ofSeconds(0))
                // 目标类型
                .targetType(AdminDO.class)
                // 每次最多取的条数
                .batchSize(10)
                .build();
        StreamMessageListenerContainer<String, ObjectRecord<String, AdminDO>> adminContainer = StreamMessageListenerContainer.create(factory, adminOptions);
        adminContainer.receiveAutoAck(Consumer.from(CONSUMER_GROUP, CONSUMER_NAME),
                StreamOffset.create(STREAM_ADMIN_KEY, ReadOffset.lastConsumed()), adminListener);
        return adminContainer;
    }

}
