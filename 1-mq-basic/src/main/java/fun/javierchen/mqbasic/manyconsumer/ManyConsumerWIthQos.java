package fun.javierchen.mqbasic.manyconsumer;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 */
public class ManyConsumerWIthQos {
    private final static String QUEUE_NAME = "basic-1";
    private static final Logger logger = LoggerFactory.getLogger(ManyConsumerWIthQos.class);

    /**
     * 模拟耗时的操作
     * @param message
     */
    public static void handelMessage(Delivery message, String channelName) {
        String receivedMessage = new String(message.getBody(), StandardCharsets.UTF_8);
        logger.info(" [x] [{}] Received message: '{}', DeliveryTag: {} at {}",
                channelName, receivedMessage, message.getEnvelope().getDeliveryTag(), System.currentTimeMillis());
        try {
            Thread.sleep(10000);
            logger.info(" [x] [{}] Handled message with DeliveryTag: {} at {}",
                    channelName, message.getEnvelope().getDeliveryTag(), System.currentTimeMillis());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel defaultChannel = connection.createChannel();
        Channel otherChannel = connection.createChannel();

        // 通过 channel.basicQos 设置这个 channel 中的每个消费者可以获取的未确认消息的最大数量
        // 只设置了 default的basicQos 其他的 channel 会获取更多的消息(因为默认是 0 -- 不限制获取的消息数量)
        defaultChannel.basicQos(2);

        defaultChannel.queueDeclare(QUEUE_NAME, false, false, false, null);

        DeliverCallback defaultCallBack = (consumerTag, message) -> {
            handelMessage(message, "default");
            defaultChannel.basicAck(message.getEnvelope().getDeliveryTag(), false);
        };

        DeliverCallback otherCallBack = (consumerTag, message) -> {
            handelMessage(message, "other");
            otherChannel.basicAck(message.getEnvelope().getDeliveryTag(), false);
        };


        defaultChannel.basicConsume(QUEUE_NAME, false, defaultCallBack, consumerTag -> {
        });

        otherChannel.basicConsume(QUEUE_NAME, false, otherCallBack, consumerTag -> {
        });

    }


}
