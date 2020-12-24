package study.wyy.mq.rocket.quickstart;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.UnsupportedEncodingException;

/**
 * @author by wyaoyao
 * @Description 发送消息
 * @Date 2020/12/24 10:41 下午
 */
public class SendMessageExample {

    public static void main(String[] args) throws MQClientException, UnsupportedEncodingException, RemotingException, InterruptedException, MQBrokerException {
        // 1 创建 生产者 并指定生产者组
        DefaultMQProducer producer = new DefaultMQProducer("test_producer_group");
        // 2 指定name server 地址
        producer.setNamesrvAddr("localhost:9876");
        // 3 启动producer
        producer.start();
        // 4 发送消息
        // 4.1 构建消息
        Message message = buildMessage();
        SendResult result = producer.send(message);
        System.out.println("发送结果：" + result);
        // 关闭
        producer.shutdown();

    }

    private static Message buildMessage() throws UnsupportedEncodingException {
        Message message = new Message();
        // 设置topic
        message.setTopic("myTopic");
        // 设置标签
        message.setTags("mytag");
        String content = "我的第一个消息";
        // 消息体
        message.setBody(content.getBytes("UTF-8"));
        return message;
    }
}
