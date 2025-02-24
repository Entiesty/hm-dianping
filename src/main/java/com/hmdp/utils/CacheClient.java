package com.hmdp.utils;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_TTL;

@Slf4j
@Component
@RequiredArgsConstructor
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    private final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    // 设置缓存值
    public void setValue(String key, Object data, Long expireDuration, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSON.toJSONString(data), expireDuration, unit);
    }

    // 设置带有逻辑过期的缓存值
    public void setWithLogicalExpiration(String key, Object data, Long expireDuration, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(expireDuration)));
        redisData.setData(data);

        stringRedisTemplate.opsForValue().set(key, JSON.toJSONString(redisData));
    }

    // 查询缓存，支持缓存穿透
    public <R, ID> R queryWithCachePassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dataFallBack,
                                               Long expireDuration, TimeUnit unit) {
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);

        if (StrUtil.isNotBlank(json)) {
            return JSON.parseObject(json, type);
        }

        if (json != null) {
            return null;
        }

        R data = dataFallBack.apply(id);

        if (data == null) {
            this.setValue(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }

        this.setValue(key, data, expireDuration, unit);
        return data;
    }


    public <R, ID> R queryWithLogicalExpiration(String keyPrefix, ID id, Class<R> type, Function<ID, R> dataFallBack,
                                                Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String lockKey = key + ":lock";
        String json = stringRedisTemplate.opsForValue().get(key);

        // 如果缓存为空，返回 null
        if (StrUtil.isBlank(json)) {
            return null;
        }

        // 解析缓存数据
        RedisData redisData = JSON.parseObject(json, RedisData.class);
        R data = JSON.parseObject(JSON.toJSONString(redisData.getData()), type);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 如果缓存数据没有过期，直接返回
        if (expireTime.isAfter(LocalDateTime.now())) {
            return data;
        }

        // 如果缓存数据已过期，尝试获取锁
        boolean isLockAcquired = tryAcquireLock(lockKey);
        if (isLockAcquired) {
            try {
                // 获取锁后，异步执行缓存重建
                CACHE_REBUILD_EXECUTOR.submit(() -> rebuildCache(key, dataFallBack, id, time, unit));
            } finally {
                releaseLock(lockKey); // 释放锁
            }
        }

        // 返回旧数据，防止缓存击穿
        return data;
    }

    private <R, ID> void rebuildCache(String key, Function<ID, R> dataFallBack, ID id, Long time, TimeUnit unit) {
        try {
            // 获取数据，执行数据回退操作
            R data = dataFallBack.apply(id);

            if (data != null) {
                // 构建并保存缓存数据
                RedisData redisData = new RedisData();
                redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time))); // 设置逻辑过期时间
                redisData.setData(data);

                // 更新缓存
                stringRedisTemplate.opsForValue().set(key, JSON.toJSONString(redisData));
            } else {
                // 如果数据为空，可以选择设置空缓存，避免缓存穿透
                this.setValue(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            }
        } catch (Exception e) {
            log.error("缓存重建失败", e);
        }
    }

    private boolean tryAcquireLock(String lockKey) {
        // 使用 setIfAbsent 获取锁，并设置锁的过期时间
        Boolean lockAcquired = stringRedisTemplate.opsForValue().setIfAbsent(lockKey, "LOCKED", LOCK_SHOP_TTL, TimeUnit.MINUTES);
        return lockAcquired != null && lockAcquired;
    }

    private void releaseLock(String lockKey) {
        // 删除锁
        stringRedisTemplate.delete(lockKey);
    }



}

