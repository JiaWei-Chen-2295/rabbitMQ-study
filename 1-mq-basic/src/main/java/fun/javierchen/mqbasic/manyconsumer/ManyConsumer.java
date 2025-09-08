package fun.javierchen.mqbasic.manyconsumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

/**
 * 多消费者情况下 在同一个 queue 上 默认轮询消费(Round-robin)
 * 一个消息只能被一个消费者消费
 */
public class ManyConsumer {
    private final static String QUEUE_NAME = "basic-1";
    private static final Logger logger = LoggerFactory.getLogger(ManyConsumer.class);


    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        // 当多个线程需要使用 channel 建议每个线程创建一个 channel
        // 如果只是单线程 多个操作只需要一个 channel
        Channel defaultChannel = connection.createChannel();
        Channel otherChannel = connection.createChannel();

        defaultChannel.queueDeclare(QUEUE_NAME, false, false, false, null);

        DeliverCallback defaultCallBack = (consumerTag, message) -> {
            String receivedMessage = new String(message.getBody(), StandardCharsets.UTF_8);
            logger.info(" [default] Received '{}'", receivedMessage);
            defaultChannel.basicAck(message.getEnvelope().getDeliveryTag(), false);
        };

        DeliverCallback otherCallBack = (consumerTag, message) -> {
            String receivedMessage = new String(message.getBody(), StandardCharsets.UTF_8);
            logger.info(" [other] Received '{}'", receivedMessage);
            otherChannel.basicAck(message.getEnvelope().getDeliveryTag(), false);
        };

        CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    defaultChannel.basicConsume(QUEUE_NAME, false, defaultCallBack, consumerTag -> {
                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        CompletableFuture.runAsync(new Runnable() {
            @Override
            public void run() {
                try {
                    otherChannel.basicConsume(QUEUE_NAME, false, otherCallBack, consumerTag -> {
                    });
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

    }


}
