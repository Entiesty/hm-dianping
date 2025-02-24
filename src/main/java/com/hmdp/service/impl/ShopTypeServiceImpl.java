package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson2.JSON;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
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
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;


    @Override
    public Result queryTypeList() {
        //1.查询redis中的缓存
        String shopTypeList = stringRedisTemplate.opsForValue().get(CACHE_SHOP_TYPE_KEY);
        //2.如果缓存命中，则返回结果
        if (StrUtil.isNotBlank(shopTypeList)) {
            List<ShopType> shopTypeList1 = JSON.parseArray(shopTypeList, ShopType.class);
            return Result.ok(shopTypeList1);
        }
        //3.如果缓存未命中，则查询数据库
        List<ShopType> shopTypeList2 = query().orderByAsc("sort").list();
        //4.如果数据库不存在该数据，则报错
        if (shopTypeList2 == null) {
            return Result.ok("数据不存在！");
        }
        //5.如果数据库存在该数据，则写入redis缓存
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_TYPE_KEY, JSON.toJSONString(shopTypeList2));
        //6.返回数据
        return Result.ok(shopTypeList2);
    }
}
