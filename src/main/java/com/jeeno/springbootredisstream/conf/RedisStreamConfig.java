package com.jeeno.springbootredisstream.conf;

import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;
import org.springframework.data.redis.stream.Subscription;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author JEENO
 * @version 0.0.1
 * @date 2021/3/31 14:37
 */
@Slf4j
@Component
public class RedisStreamConfig {

    @Resource
    private Map<String, StreamMessageListenerContainer<String, ?>> listenerContainerMap;

    /**
     * 初始化redis stream 的消费组
     */
    private void initializeGroup() {
    }

    /**
     * 监听容器列表
     */
    private List<StreamMessageListenerContainer<String, ?>> containerList = new ArrayList<>();

    @Bean
    public Subscription subscription(RedisConnectionFactory factory){

        // 初始化各个stream的消费组
        initializeGroup();

        // 注册监听容器
        registerContainer();

        // 启动所有的容器
        containerList.forEach(StreamMessageListenerContainer::start);

        return null;
    }

    private void registerContainer() {
        containerList.addAll(listenerContainerMap.values());
    }

}
