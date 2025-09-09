package fun.javierchen.mqbasic.exchange.direct;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectExchangeConsumer {
    private final static String DEFAULT_QUEUE_NAME = "basic-1";
    private final static String OTHER_QUEUE_NAME = "basic-2";
    private final static String EXCHANGE_NAME = "exchange-fanout";
    private final static Logger logger = LoggerFactory.getLogger(DirectExchangeConsumer.class);

    public static void main(String[] args) throws Exception {
        Connection connection = new ConnectionFactory().newConnection();
        Channel channel = connection.createChannel();

        DeliverCallback defaultDeliverCallback = (message, delivery) -> {
            logger.info("[default]: received {}", new String(delivery.getBody()));
        };

        DeliverCallback otherDeliverCallback = (message, delivery) -> {
            logger.info("[other]: received {}", new String(delivery.getBody()));
        };



        channel.basicConsume(DEFAULT_QUEUE_NAME, true, defaultDeliverCallback, consumerTag -> {
        });
        channel.basicConsume(OTHER_QUEUE_NAME, true, otherDeliverCallback, consumerTag -> {
        });


    }
}
