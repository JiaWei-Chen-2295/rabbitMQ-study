package fun.javierchen.mqbasic.ttl;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {
    private final static String QUEUE_NAME = "basic-1";
    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);


    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);

        DeliverCallback defaultCallBack = (consumerTag, message) -> {
            String receivedMessage = new String(message.getBody(), "UTF-8");
            logger.info(" [x] Received '{}'", receivedMessage);
        };

        channel.basicConsume(QUEUE_NAME, true, defaultCallBack, consumerTag -> { });
    }


}
