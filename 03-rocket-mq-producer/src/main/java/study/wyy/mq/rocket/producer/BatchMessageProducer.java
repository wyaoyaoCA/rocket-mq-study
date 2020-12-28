package study.wyy.mq.rocket.producer;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @author: wyaoyao
 * @date: 2020-12-25 16:24
 * @description:
 */
public class BatchMessageProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException, MQBrokerException {
        // 1 创建生产者
        DefaultMQProducer producer = new DefaultMQProducer("test-producer-group");
        // 2 设置 name server地址
        producer.setNamesrvAddr("localhost:9876");
        // 3 启动
        producer.start();
        // 4 构建消息
        List<Message> messages = new ArrayList<>();
        Message message = new Message();
        message.setTopic("myTopic");
        message.setTags("myTags");
        message.setBody("批量消息1".getBytes(Charset.defaultCharset()));
        messages.add(message);
        message = new Message();
        message.setTopic("myTopic");
        message.setTags("myTags");
        message.setBody("批量消息2".getBytes(Charset.defaultCharset()));
        messages.add(message);
        producer.send(messages);
        producer.shutdown();
    }
}
