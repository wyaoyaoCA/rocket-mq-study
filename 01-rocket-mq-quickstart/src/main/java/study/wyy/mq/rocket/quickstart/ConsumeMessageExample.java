package study.wyy.mq.rocket.quickstart;

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
 * @author by wyaoyao
 * @Description： 消费消息example
 * @Date 2020/12/24 10:48 下午
 */
public class ConsumeMessageExample {
    public static void main(String[] args) throws MQClientException {
        // 1 实例化消息生产者,指定组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("test_consumer_group");
        // 2 指定Namesrv地址信息.
        consumer.setNamesrvAddr("localhost:9876");
        // 3 订阅Topic
        consumer.subscribe("myTopic", "*");
        // 4 负载均衡模式消费
        consumer.setMessageModel(MessageModel.CLUSTERING);
        // 5 注册回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {
            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                System.out.printf("%s 收到消息 : %s %n",
                        Thread.currentThread().getName(), msgs);
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
        //6 启动消息者
        consumer.start();
        consumer.shutdown();
    }

}
