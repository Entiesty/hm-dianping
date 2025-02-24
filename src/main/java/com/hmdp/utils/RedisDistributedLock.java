package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class RedisDistributedLock implements ILock {

    private final StringRedisTemplate redisTemplate;
    private final String lockKey;
    private static final String LOCK_KEY_PREFIX = "lock:";
    private static final String LOCK_VALUE_PREFIX = UUID.randomUUID().toString(true) + "-";
    private static final DefaultRedisScript<Long> RELEASE_LOCK_SCRIPT;
    static {
        RELEASE_LOCK_SCRIPT = new DefaultRedisScript<>();
        RELEASE_LOCK_SCRIPT.setLocation(new ClassPathResource("releaseLock.lua"));
        RELEASE_LOCK_SCRIPT.setResultType(Long.class);
    }

    @Override
    public boolean tryAcquireLock(long expirationInSeconds) {
        // 获取当前线程ID作为锁的唯一值
        String lockValue = LOCK_VALUE_PREFIX + Thread.currentThread().getId();
        // 尝试获取锁
        Boolean isLockAcquired = redisTemplate.opsForValue()
                .setIfAbsent(LOCK_KEY_PREFIX + lockKey, lockValue, expirationInSeconds, TimeUnit.SECONDS);
        return Boolean.TRUE.equals(isLockAcquired);
    }

    @Override
    public void releaseLock() {
        redisTemplate.execute(RELEASE_LOCK_SCRIPT,
                Collections.singletonList(LOCK_KEY_PREFIX + lockKey),
                LOCK_VALUE_PREFIX + Thread.currentThread().getId());

    }

/*    @Override
    public void releaseLock() {
        String currentThreadLockValue = LOCK_VALUE_PREFIX + Thread.currentThread().getId();
        String storedLockValue = redisTemplate.opsForValue().get(LOCK_KEY_PREFIX + lockKey);

        // 只有当前线程持有的锁才能释放
        if (currentThreadLockValue.equals(storedLockValue)) {
            redisTemplate.delete(LOCK_KEY_PREFIX + lockKey);
        }
    }*/
}

