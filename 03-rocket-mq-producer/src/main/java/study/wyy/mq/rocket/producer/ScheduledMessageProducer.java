package study.wyy.mq.rocket.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.Charset;

/**
 * @author: wyaoyao
 * @date: 2020-12-25 15:53
 * @description: 延时消息发送
 *
 */
public class ScheduledMessageProducer {

    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // 1 创建生产者
        DefaultMQProducer producer = new DefaultMQProducer("test-producer-group");
        // 2 设置 name server地址
        producer.setNamesrvAddr("localhost:9876");
        // 3 启动
        producer.start();
        // 4 构建消息
        Message message = new Message();
        message.setTopic("myTopic");
        message.setTags("myTags");
        // 设置延时等级3,这个消息将在10s之后发送(现在只支持固定的几个时间,详看delayTimeLevel)
        message.setDelayTimeLevel(3);
        message.setBody("我的第一个延时消息".getBytes(Charset.defaultCharset()));
        producer.send(message);
        producer.shutdown();
    }
}

