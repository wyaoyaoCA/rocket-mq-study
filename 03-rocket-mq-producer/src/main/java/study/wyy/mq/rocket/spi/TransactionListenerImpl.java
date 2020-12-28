package study.wyy.mq.rocket.spi;

import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * @author 20116651
 * @description
 * @date 2020/12/28 15:45
 */
public class TransactionListenerImpl implements TransactionListener {
    /*****
     *
     * 当发送半消息成功时，我们使用 executeLocalTransaction 方法来执行本地事务。
     * 也就是执行具体的业务逻辑
     */
    @Override
    public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
        return null;
    }

    @Override
    public LocalTransactionState checkLocalTransaction(MessageExt msg) {
        return null;
    }
}
