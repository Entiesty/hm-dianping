package com.hmdp.service.impl;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import lombok.RequiredArgsConstructor;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@RequiredArgsConstructor
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    private final StringRedisTemplate redisTemplate;

    @Override
    public Result queryById(Long id) {
        // 1. 查询 Redis 缓存
        String shopJson = redisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        // 2. 判断缓存是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            return Result.ok(JSON.parseObject(shopJson, Shop.class));
        }

        // 3. 缓存命中了，但是空值，说明数据库中不存在该店铺
        if (shopJson != null) {
            return Result.fail("该店铺ID不存在！");
        }

        // 4. 获取互斥锁，防止缓存击穿
        boolean lock = BooleanUtil.isTrue(redisTemplate.opsForValue().setIfAbsent("lock:shop:" + id, "1", 10, TimeUnit.SECONDS));

        try{
            if (!lock) {
                // 4.1 竞争失败，等待一段时间后重试，避免递归调用
                Thread.sleep(50);
                return queryById(id);
            }
            //5.竞争成功，查询数据库
            Shop shop = getById(id);
            //6.如果未查询到数据，则将空值写入缓存
            if (shop == null) {
                redisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            }
            //7.如果查询到数据，则将数据写入缓存
            redisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSON.toJSONString(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);

            //8.返回数据
            return Result.ok(shop);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            redisTemplate.delete("lock:shop:" + id);
        }
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    @Override
    public Result queryWithLogicalExpire(Long id) {
        // 1. 查询 Redis 缓存
        String shopJson = redisTemplate.opsForValue().get(CACHE_SHOP_KEY + id);

        if (StrUtil.isBlank(shopJson)) {
            // 1.1 如果缓存数据不存在，则直接查询数据库（防止 key 被意外删除）
            Shop shop = getById(id);
            if (shop == null) {
                return Result.fail("该店铺不存在！");
            }
            return Result.ok(shop);
        }

        // 2. 解析 Redis 缓存数据
        RedisData redisData = JSON.parseObject(shopJson, RedisData.class);
        Shop shop = JSON.parseObject(JSON.toJSONString(redisData.getData()), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 3. 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 3.1 未过期，直接返回
            return Result.ok(shop);
        }

        // 4. 过期，尝试获取互斥锁
        String lockKey = "lock:shop:" + id;
        String lockValue = UUID.randomUUID().toString(); // 生成唯一标识
        boolean lock = BooleanUtil.isTrue(redisTemplate.opsForValue().setIfAbsent(lockKey, lockValue, 10, TimeUnit.SECONDS));

        if (lock) {
            try {
                // 5.1 启动独立线程进行缓存重建
                CACHE_REBUILD_EXECUTOR.submit(() -> {
                    try {
                        shopToRedisData(id, 20L);
                    } catch (Exception e) {
                        log.error("缓存重建失败", e);
                    } finally {
                        // 释放锁（防止误删）
                        String currentValue = redisTemplate.opsForValue().get(lockKey);
                        if (lockValue.equals(currentValue)) {
                            redisTemplate.delete(lockKey);
                        }
                    }
                });
            } catch (Exception e) {
                log.error("线程池提交任务失败", e);
            }
        }
        // 6. 返回旧数据，防止阻塞
        return Result.ok(shop);
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //1.判断是否需要根据坐标查询
        if(x == null || y == null) {
            //不需要坐标，数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }

        //获取所需的分页头尾码
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        //获取附近店铺数据
        String key = "shop:geo:" + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = redisTemplate.opsForGeo()
                .search(key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end));

        if(results == null ) {
            return Result.ok(Collections.emptyList());
        }

        //将店铺数据加入集合来排序
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> content = results.getContent();
        if(content.size() <= from) {
            return Result.ok(Collections.emptyList());
        }


        List<Long> ids = new ArrayList<>();
        Map<String, Distance> distanceMap = new HashMap<>(content.size());
        content.stream().skip(from).forEach(result -> {
            //获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            //获取店铺距离
            distanceMap.put(shopIdStr, result.getDistance());
        });

        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }

        return Result.ok(shops);
    }


    public void shopToRedisData(Long id, Long expireSeconds) throws InterruptedException {
        //1.查询数据库
        Shop shop = getById(id);
        Thread.sleep(200);
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));
        //2.插入数据到redis实现逻辑过期(永不过期)
        redisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSON.toJSONString(redisData));
    }


    @Override
    @Transactional
    public Result updateShopById(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.ok("店铺ID不能为空！");
        }

        //1.修改数据库
        updateById(shop);
        //2.删除缓存
        redisTemplate.delete(CACHE_SHOP_KEY + id);

        return Result.ok();
    }
}
