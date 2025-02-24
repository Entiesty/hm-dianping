package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    private static final long BEGIN_TIMESTAMP = 1735689600L;
    private static final int COUNT_BITS = 32;
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public long nextId(String keyPrefix) {
        // 1. 生成时间戳（当前时间戳 - BEGIN_TIMESTAMP）
        long timestamp = (System.currentTimeMillis() / 1000) - BEGIN_TIMESTAMP;

        // 2. 获取当前日期，使用更标准的格式（yyyy-MM-dd）
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        // 3. Redis 键
        String redisKey = "icr:" + keyPrefix + ":" + date;

        // 4. 尝试使用 Redis 增量操作
        Long count = stringRedisTemplate.opsForValue().increment(redisKey, 1);

        // 5. 如果 Redis 返回 null，意味着该键初次访问，手动设置初始值为 0
        if (count == null) {
            count = 1L;
            stringRedisTemplate.opsForValue().set(redisKey, "1");
        }

        // 6. 将时间戳和序列号组合生成唯一 ID
        return (timestamp << COUNT_BITS) | count;
    }
}

