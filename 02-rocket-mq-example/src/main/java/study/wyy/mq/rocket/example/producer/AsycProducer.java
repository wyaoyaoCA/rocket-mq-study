package study.wyy.mq.rocket.example.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.nio.charset.Charset;

/**
 * @author: wyaoyao
 * @date: 2020-12-25 10:25
 * @description: 异步消息发送
 */
@Slf4j
public class AsycProducer {
    public static void main(String[] args) throws MQClientException, RemotingException, InterruptedException {
        // 1 创建生产者，并指定生产者组
        DefaultMQProducer producer = new DefaultMQProducer();
        producer.setProducerGroup("test_producer_group");
        // 2 设置 name server 地址
        producer.setNamesrvAddr("localhost:9876");
        // 3 创建消息
        Message message = new Message();
        message.setTopic("myTopic");
        message.setTags("myTags");
        message.setBody("我的第一条异步消息".getBytes(Charset.defaultCharset()));
        // 启动生产者
        producer.start();
        // 设置重试次数
        producer.setRetryTimesWhenSendAsyncFailed(0);
        // 异步发送, 设置回调函数
        producer.send(message,new SendCallback(){
            /***
             * 发送成功时候的回调
             * @param sendResult
             */
            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("发送成功: {}",sendResult);
            }
            // 发送异常的时候回调
            @Override
            public void onException(Throwable throwable) {
                log.error("发送失败: {}",throwable);
            }
        });
        // 由于是异步消息，不能在这直接关闭，可能会导致还未的发送过去，就关闭了，这里简单的处理一下，不关闭了
        // producer.shutdown();
    }
}
