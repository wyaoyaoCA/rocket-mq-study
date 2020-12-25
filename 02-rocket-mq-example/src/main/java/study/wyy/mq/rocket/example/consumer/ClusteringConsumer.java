package study.wyy.mq.rocket.example.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.nio.charset.Charset;
import java.util.List;

/**
 * @author: wyaoyao
 * @date: 2020-12-25 12:53
 * @description: 集群模式
 */
public class ClusteringConsumer {
    public static void main(String[] args) throws MQClientException {
        // 1 构建消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();
        consumer.setConsumerGroup("test_consumer_group");
        // 2 指定 name server地址
        consumer.setNamesrvAddr("localhost:9876");
        // 3 设置消费模式
        consumer.setMessageModel(MessageModel.CLUSTERING);
        // 4 设置订阅的topic和tag
        consumer.subscribe("myTopic","*");
        // 5 设置回调函数
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                // 遍历
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
        // 6 启动
        consumer.start();
    }
}
