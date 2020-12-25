package study.wyy.mq.rocket.model;

import lombok.Data;

/**
 * @author: wyaoyao
 * @date: 2020-12-25 14:01
 * @description: 订单的流程
 */
@Data
public class OrderStep {
    private long orderId;
    private String desc;

    @Override
    public String toString() {
        return this.getOrderId() + "-->" + this.getDesc();
    }
}
