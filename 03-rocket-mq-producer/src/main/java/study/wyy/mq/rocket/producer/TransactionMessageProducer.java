package study.wyy.mq.rocket.producer;


import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import study.wyy.mq.rocket.spi.TransactionListenerImpl;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * @author: wyaoyao
 * @date: 2020-12-25 17:06
 * @description: 事务消息
 */
public class TransactionMessageProducer {
    // 记录事务的状态，key为事务的id，value为事务状态
    private static Map<String,LocalTransactionState> STATE_MAP = new HashMap<String, LocalTransactionState>();

    public static void main(String[] args) throws MQClientException {

        // 1 创建消息发送者,注意这里是事务消息生产者
        TransactionMQProducer producer = new TransactionMQProducer();
        // 2 设置 生产者组
        producer.setProducerGroup("test-producer-group");
        // 3 设置name sever
        producer.setNamesrvAddr("localhost:9876");
        // 4 设置事务监听器
        producer.setTransactionListener(new TransactionListener(){
            // 记录事务的状态，key为事务的id，value为事务状态
            /*****
             * 当发送半消息成功时，我们使用 executeLocalTransaction 方法来执行本地事务。
             * 也就是执行具体的业务逻辑
             * 返回三个事务状态之一。
             * - TransactionStatus.CommitTransaction: 提交事务，它允许消费者消费此消息。
             * - TransactionStatus.RollbackTransaction: 回滚事务，它代表该消息将被删除，不允许被消费。
             * - TransactionStatus.Unknown: 中间状态，它代表需要检查消息队列来确定状态。
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                LocalTransactionState localTransactionState= null;
                try{
                    // 执行本地的事务逻辑
                    System.out.println("执行本地的事务逻辑");
                    System.out.println("arg: " + arg);
                    String s = new String(msg.getBody(), Charset.defaultCharset());
                    System.out.println("message: " + s);
                    // 模拟出现异常
                     int a = 1/0;
                    // 没有异常就返回可以提交
                    localTransactionState = LocalTransactionState.COMMIT_MESSAGE;
                }catch (Exception e){
                    e.printStackTrace();
                    // 出现异常就回滚
                     localTransactionState = LocalTransactionState.ROLLBACK_MESSAGE;
                }finally {
                    // 记录一下
                    STATE_MAP.put(msg.getTransactionId(),localTransactionState);
                    // 没有异常就返回可以提交
                    return localTransactionState;
                }

            }
            /***
             * 用于检查本地事务状态，并回应消息队列的检查请求。它也是返回三个事务状态之一。
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                return STATE_MAP.get(msg.getTransactionId());
            }
        });

        // 5 启动
        producer.start();
        // 6 发送消息,注意发送消息的方法为sendMessageInTransaction
        Message message = new Message();
        message.setTopic("myTopic");
        message.setBody("一条事务消息".getBytes(Charset.defaultCharset()));
        // 第二个参数其他参数，会传到executeLocalTransaction的第二个参数
        producer.sendMessageInTransaction(message,"事务消息测试");
        // 还存在会查生产者的逻辑，生产者就不关闭了
        // producer.shutdown();

    }
}
