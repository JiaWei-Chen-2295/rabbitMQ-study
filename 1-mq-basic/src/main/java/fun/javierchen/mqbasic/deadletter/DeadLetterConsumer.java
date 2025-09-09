package fun.javierchen.mqbasic.deadletter;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class DeadLetterConsumer {

    private static final String DLQ_DEFAULT_NAME = "default-dead-letter-queue";
    private static final Logger logger = LoggerFactory.getLogger(DeadLetterConsumer.class);

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(DLQ_DEFAULT_NAME, false, false, false, null);

        DeliverCallback defaultCallBack = (consumerTag, message) -> {
            String receivedMessage = new String(message.getBody(), StandardCharsets.UTF_8);
            logger.info(" [死信] Received '{}'", receivedMessage);
        };

        channel.basicConsume(DLQ_DEFAULT_NAME, true, defaultCallBack, consumerTag -> { });
    }

}
