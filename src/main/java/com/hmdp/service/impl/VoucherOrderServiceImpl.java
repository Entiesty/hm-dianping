package com.hmdp.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisDistributedLock;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.ApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;

@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private ApplicationContext applicationContext;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    @Override
    public Result seckillVoucher(Long voucherId) {
        //1.查询并获取秒杀优惠券信息
        SeckillVoucher seckillVoucher = seckillVoucherService.getById(voucherId);
        //2.是否已经开始
        if(seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("秒杀还未开始！");
        }
        //3.是否已经结束
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("秒杀已经结束！");
        }
        //4.库存是否充足
        if (seckillVoucher.getStock() < 1) {
            return Result.fail("库存不足！");
        }
        //*.一人一单
        Long userId = UserHolder.getUser().getId();
/*        synchronized (userId.toString().intern()) {
            VoucherOrderServiceImpl proxy = applicationContext.getBean(VoucherOrderServiceImpl.class);
            return proxy.createVoucherOrder(voucherId);
        }*/

/*        //1.创建锁
        RedisDistributedLock simpleRedisLock = new RedisDistributedLock(stringRedisTemplate, "order:" + userId);

        //2.使用锁
        boolean isLockAcquired = simpleRedisLock.tryAcquireLock(1200);
        if(!isLockAcquired) {
            return Result.fail("不允许重复下单");
        }

        try{
            VoucherOrderServiceImpl proxy = applicationContext.getBean(VoucherOrderServiceImpl.class);
            return proxy.createVoucherOrder(voucherId);
        } finally {
            simpleRedisLock.releaseLock();
        }*/

        RLock lock = redissonClient.getLock("lock:order:" + userId);
        boolean isLockAcquire = lock.tryLock();
        if(!isLockAcquire) {
            return Result.fail("不允许重复下单");
        }

        try{
            VoucherOrderServiceImpl proxy = applicationContext.getBean(VoucherOrderServiceImpl.class);
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }
    }

    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        //*.1查询订单
        int count = query()
                .eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        //*.2判断是否多买
        if(count > 0) {
            return Result.fail("你已经购买过了！不可多买！");
        }

        //5.更新库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .gt("stock", 0)
                .eq("voucher_id", voucherId)
                .update();

        //6.如果更新失败
        if (!success) {
            return Result.fail("库存不足！");
        }
        //7.创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //7.1获取订单id
        voucherOrder.setId(redisIdWorker.nextId("order"));
        //7.2获取用户id
        voucherOrder.setUserId(UserHolder.getUser().getId());
        //7.3获取秒杀优惠券id
        voucherOrder.setVoucherId(voucherId);
        //8.保存订单到数据库
        save(voucherOrder);
        //9.返回订单id
        return Result.ok(voucherOrder.getId());
    }
}
