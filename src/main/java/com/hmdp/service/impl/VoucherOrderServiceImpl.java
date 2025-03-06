package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.config.RabbitMQConfig;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.ApplicationContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Service
@Slf4j
@RequiredArgsConstructor
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private ApplicationContext applicationContext;

    private final StringRedisTemplate redisTemplate;
    private final RabbitTemplate rabbitTemplate;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    //    private final BlockingQueue<VoucherOrder> orderBlockingQueue = new ArrayBlockingQueue<>(1024 * 1024);
/*    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            String mqName = "stream.orders";
            while (true) {
                try {
                    //1.获取消息队列中的消息 XREADGROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = redisTemplate.opsForStream()
                            .read(Consumer.from("g1", "c1"),
                                    StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                                    StreamOffset.create(mqName, ReadOffset.lastConsumed()));

                    //2.判断获取消息是否成功
                    //2.1如果失败了，则重试，继续下一次循环
                    if (list == null || list.isEmpty()) {
                        continue;
                    }
                    //2.2如果成功了，则进入下一步
                    //3.解析消息中的数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    System.out.println("record:" + record);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    //4.提交订单到数据库
                    VoucherOrderServiceImpl proxy = applicationContext.getBean(VoucherOrderServiceImpl.class);
                    proxy.createVoucherOrder(voucherOrder);
                    //5.确认消息已经被消费
                    redisTemplate.opsForStream().acknowledge(mqName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常：", e);
                    while (true) {
                        try {
                            //1.获取消息队列中的消息 XREADGROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                            List<MapRecord<String, Object, Object>> list = redisTemplate.opsForStream()
                                    .read(Consumer.from("g1", "c1"),
                                            StreamReadOptions.empty().count(1),
                                            StreamOffset.create(mqName, ReadOffset.from("0")));

                            //2.判断获取消息是否成功
                            //2.1如果失败了，则重试，继续下一次循环
                            if (list == null || list.isEmpty()) {
                                break;
                            }
                            //2.2如果成功了，则进入下一步
                            //3.解析消息中的数据
                            MapRecord<String, Object, Object> record = list.get(0);
                            System.out.println("record:" + record);
                            Map<Object, Object> value = record.getValue();
                            VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                            //4.提交订单到数据库
                            VoucherOrderServiceImpl proxy = applicationContext.getBean(VoucherOrderServiceImpl.class);
                            proxy.createVoucherOrder(voucherOrder);
                            //5.确认消息已经被消费
                            redisTemplate.opsForStream().acknowledge(mqName, "g1", record.getId());
                        } catch (Exception e1) {
                            log.error("pending-list异常");
                        }
                    }
                }
            }
        }
    }*/

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        // 创建订单ID并准备订单信息
        long orderId = redisIdWorker.nextId("order");

        // 执行Lua脚本判断是否有秒杀资格
        Long result = redisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(voucherId));

        int ret = 0;
        if (result != null) {
            ret = result.intValue();
        }
        if (ret != 0) {
            // 如果结果不为0，表示秒杀失败
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }

        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);

        rabbitTemplate.convertAndSend(RabbitMQConfig.ORDER_EXCHANGE, RabbitMQConfig.ORDER_ROUTING_KEY, voucherOrder);

        // 返回订单ID
        return Result.ok(orderId);
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();

        // 检查用户是否已下单
        int count = query()
                .eq("user_id", userId)
                .eq("voucher_id", voucherOrder.getVoucherId())
                .count();
        if (count > 0) {
            return;  // 用户已下单，跳过
        }

        // 更新库存，并确保库存大于0
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .gt("stock", 0)
                .eq("voucher_id", voucherOrder.getVoucherId())
                .update();

        if (!success) {
            return;  // 库存不足，订单创建失败
        }

        // 保存订单
        save(voucherOrder);
    }
}

/*    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();

        // 执行Lua脚本判断是否有秒杀资格
        Long result = stringRedisTemplate.execute(SECKILL_SCRIPT,
                Collections.emptyList(), voucherId.toString(), userId.toString());

        if (result != 0) {
            // 如果结果不为0，表示秒杀失败
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }

        // 创建订单ID并准备订单信息
        long orderId = redisIdWorker.nextId("order");
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(orderId);
        voucherOrder.setUserId(userId);
        voucherOrder.setVoucherId(voucherId);

        // 将订单添加到阻塞队列中处理
        orderBlockingQueue.add(voucherOrder);

        // 返回订单ID
        return Result.ok(orderId);
    }*/

        /*    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    // 从队列中获取订单并处理
                    VoucherOrder voucherOrder = orderBlockingQueue.take();
                    processVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    log.error("订单处理被中断", e);
                    Thread.currentThread().interrupt();  // 恢复中断标志
                } catch (Exception e) {
                    log.error("处理订单时出现异常", e);
                }
            }
        }
    }

    private void processVoucherOrder(VoucherOrder voucherOrder) {
        // 使用代理模式创建订单，保证事务的一致性
        VoucherOrderServiceImpl proxy = applicationContext.getBean(VoucherOrderServiceImpl.class);
        proxy.createVoucherOrder(voucherOrder);
    }*/


/*    @Override
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
*//*        synchronized (userId.toString().intern()) {
            VoucherOrderServiceImpl proxy = applicationContext.getBean(VoucherOrderServiceImpl.class);
            return proxy.createVoucherOrder(voucherId);
        }*//*

         *//*        //1.创建锁
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
        }*//*

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
    }*/

/*    @Transactional
    public Result createVoucherOrder(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        //*.1查询订单
        int count = query()
                .eq("user_id", userId)
                .eq("voucher_id", voucherId)
                .count();
        //*.2判断是否多买
        if (count > 0) {
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
    }*/
