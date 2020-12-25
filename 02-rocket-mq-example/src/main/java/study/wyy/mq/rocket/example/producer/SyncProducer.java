package study.wyy.mq.rocket.example.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.Charset;

/**
 * @author: wyaoyao
 * @date: 2020-12-25 9:04
 * @description: 发送同步消息
 */
@Slf4j
public class SyncProducer {

    public static void main(String[] args) throws InterruptedException, RemotingException, MQClientException, MQBrokerException {
        // 1 创建消息生产者，指定生产者组
        DefaultMQProducer producer = new DefaultMQProducer("test_producer_group");
        // 2 设置 name server 地址
        producer.setNamesrvAddr("localhost:9876");
        // 3 构建消息
        Message message = new Message();
        message.setTopic("myTopic");
        message.setBody("我的第一条同步消息".getBytes(Charset.defaultCharset()));
        message.setTags("myTags");
        // 4 启动生产者
        producer.start();
        // 5 发送下下哦
        SendResult result = producer.send(message);
        log.info("发送结果: {}", result);
        // 6. 关闭
        producer.shutdown();

    }
}
