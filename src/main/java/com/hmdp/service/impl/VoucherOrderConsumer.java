package com.hmdp.service.impl;

import com.hmdp.config.RabbitMQConfig;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.service.IVoucherOrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class VoucherOrderConsumer {
    private final IVoucherOrderService voucherOrderService;

    @RabbitListener(queues = RabbitMQConfig.ORDER_QUEUE)
    public void handleVoucherOrder(@Payload VoucherOrder voucherOrder) {
        try {
            log.info("收到秒杀订单消息：{}", voucherOrder);
            voucherOrderService.createVoucherOrder(voucherOrder);
        } catch (Exception e) {
            log.error("处理秒杀订单异常", e);
        }
    }

}
