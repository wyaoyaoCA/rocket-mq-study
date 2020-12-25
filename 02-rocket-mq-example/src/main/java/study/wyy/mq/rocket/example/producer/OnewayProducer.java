package study.wyy.mq.rocket.example.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.Charset;

/**
 * @author: wyaoyao
 * @date: 2020-12-25 10:52
 * @description:  单向消息
 */
@Slf4j
public class OnewayProducer {
    public static void main(String[] args) throws RemotingException, MQClientException, InterruptedException {
        // 1 构建生产者, 指定生产者组
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setProducerGroup("test_producer_group");
        // 2 设置name server diz
        producer.setNamesrvAddr("localhost:9876");
        // 3 构建消息
        Message message = new Message();
        message.setTags("myTags");
        message.setTopic("myTopic");
        message.setBody("我的第一条单向消息".getBytes(Charset.defaultCharset()));
        // 4 启动producer
        producer.start();
        // 5 发送消息
        producer.sendOneway(message);
        // 6 关闭
        producer.shutdown();
    }
}
