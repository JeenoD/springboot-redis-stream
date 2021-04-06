package com.jeeno.springbootredisstream.controller;

import com.jeeno.springbootredisstream.pojo.AdminDO;
import com.jeeno.springbootredisstream.pojo.UserDO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.connection.stream.Record;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

import static com.jeeno.springbootredisstream.container.AdminListenerContainer.STREAM_ADMIN_KEY;
import static com.jeeno.springbootredisstream.container.UserListenerContainer.STREAM_USER_KEY;

/**
 * 生产端-外部接口
 * @author JEENO
 * @version 0.0.1
 * @date 2021/3/31 15:51
 */
@Slf4j
@RequestMapping("/redis")
@RestController
public class ProduceController {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @GetMapping("/user/{name}")
    public String test(@PathVariable String name) {
        UserDO userDO = UserDO.builder().name(name).build();
        ObjectRecord<String, UserDO> record = StreamRecords.objectBacked(userDO).withStreamKey(STREAM_USER_KEY);
        RecordId recordId = stringRedisTemplate.opsForStream().add(record);
        log.info("{}, {}", recordId, userDO);
        return "成功";
    }

    @GetMapping("/admin/{name}")
    public String admin(@PathVariable String name) {
        AdminDO admin = AdminDO.builder().name(name).role("SUPER_ADMIN").build();
        ObjectRecord<String, AdminDO> record = Record.of(admin).withStreamKey(STREAM_ADMIN_KEY);
        RecordId recordId = stringRedisTemplate.opsForStream().add(record);
        log.info("{}, {}", recordId, admin);
        return "成功";
    }
}
