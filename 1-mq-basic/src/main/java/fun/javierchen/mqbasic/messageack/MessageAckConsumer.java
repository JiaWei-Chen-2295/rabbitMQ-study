package fun.javierchen.mqbasic.messageack;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageAckConsumer {
    private final static String QUEUE_NAME = "ack-test-1";
    private final static Logger logger = LoggerFactory.getLogger(MessageAckConsumer.class);


    public static void handelMessage(String message) {
        if (message.contains("error")) {
            throw new RuntimeException("error message");
        }
    }

    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            try {
                handelMessage(message);
                // 如果消息处理完毕 确认消息
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                logger.info(" [x] Received '{}'", message);
            } catch (Exception e) {
                // 如果出问题 则进行消息拒绝
                // requeue 决定这个消息是否丢弃
                // 建议 requeue 为 true 时增加重试次数
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, false);
                logger.error(" [ScannerProvider] error message: {}", e.getMessage());
            }
        };

        // 自动确认机制
        // 当收到消息后 自动确认 消息就会从消息队列移除
//        channel.basicConsume(QUEUE_NAME, true, deliverCallback, (tag) -> {
//        });
//
        // 手动确认机制
        // 在回调中进行确认
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, (tag) -> {
        });
    }
}
