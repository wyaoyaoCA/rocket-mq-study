package study.wyy.mq.rocket.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import study.wyy.mq.rocket.model.OrderStep;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: wyaoyao
 * @date: 2020-12-25 14:14
 * @description: 顺序消息发送
 */
@Slf4j
public class OrderedMessageProducer {

    public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        // 构造订单数据
        List<OrderStep> orderSteps = buildData();
        // 1 构建生产者
        DefaultMQProducer producer = new DefaultMQProducer("test_producer_group");
        // 2 设置name server
        producer.setNamesrvAddr("localhost:9876");
        // 3 启动
        producer.start();
        // 遍历数据发送
        for (OrderStep orderStep:orderSteps) {
            Message message = new Message("myTopic", "myTags", orderStep.toString().getBytes(Charset.defaultCharset()));
            /****
             *  第一个参数：message 就是要发送的消息
             *  第二个参数MessageQueueSelector： 定义如何选择消息队列
             *  第三个参数：Object类型，可以理解就是业务参数，会传给MessageQueueSelector接口中select方法的第三个参数
             *  用于队列的选择，比如这里就把orderId塞进去
             */
            SendResult send = producer.send(message, new MessageQueueSelector() {
                /***
                 * List<MessageQueue> mqs: 就是消息队列的list集合，也就是topic下的消息队列的集合，默认大小是4
                 * Message message: 就是要发送的消息
                 * Object o： 见上面的解释
                 * 返回值：就是选择的队列
                 */
                @Override
                public MessageQueue select(List<MessageQueue> mqs, Message message, Object o) {
                    // 强转
                    Long orderId = (Long) o;
                    // 对orderId进行取余数,余数相同的放到一个队列中
                    long index = orderId % mqs.size();
                    MessageQueue select = mqs.get((int) index);
                    log.info("队列的大小： {}; orderID: {}; select: {}",mqs.size(), orderId,select.getQueueId());
                    return select;
                }
            }, orderStep.getOrderId());
        }
        producer.shutdown();

    }

    private static List<OrderStep> buildData(){
        List<OrderStep> orderList = new ArrayList<OrderStep>();

        OrderStep orderDemo = new OrderStep();
        orderDemo.setOrderId(1L);
        orderDemo.setDesc("创建");
        orderList.add(orderDemo);


        orderDemo = new OrderStep();
        orderDemo.setOrderId(2L);
        orderDemo.setDesc("创建");
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(1L);
        orderDemo.setDesc("付款");
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(3L);
        orderDemo.setDesc("创建");
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(2L);
        orderDemo.setDesc("付款");
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(3L);
        orderDemo.setDesc("付款");
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(2L);
        orderDemo.setDesc("完成");
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(1L);
        orderDemo.setDesc("推送");
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(3L);
        orderDemo.setDesc("完成");
        orderList.add(orderDemo);

        orderDemo = new OrderStep();
        orderDemo.setOrderId(1L);
        orderDemo.setDesc("完成");
        orderList.add(orderDemo);

        return orderList;
    }
}
