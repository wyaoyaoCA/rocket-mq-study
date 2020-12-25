package study.wyy.mq.rocket.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;

import java.nio.charset.Charset;
import java.util.List;

/**
 * @author: wyaoyao
 * @date: 2020-12-25 16:06
 * @description: 延时消息消费者
 */
@Slf4j
public class ScheduledMessageConsumer {

    public static void main(String[] args) throws MQClientException {
        // 1 定义消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test-consumer-group");
        // 2 设置 name server地址
        consumer.setNamesrvAddr("localhost:9876");
        // 3 设置订阅的主题
        consumer.subscribe("myTopic","*");
        // 4 设置回调函数
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
                System.out.println(" ======遍历消息======= ");
                if (msgs != null && msgs.size() > 0) {
                    for (MessageExt msg : msgs) {
                        System.out.println("消息id: " + msg.getMsgId());
                        System.out.println("topic: " + msg.getTopic());
                        System.out.println("tag: " + msg.getTags());
                        System.out.println("消息体："+ new String(msg.getBody(), Charset.defaultCharset()));
                        System.out.println("=======end===========");
                    }
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 5 启动
        consumer.start();
    }
}
