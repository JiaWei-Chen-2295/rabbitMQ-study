package fun.javierchen.mqbasic.exchange.direct;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import fun.javierchen.mqbasic.exchange.RabbitMQExchangeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * direct 将消息发送到指定队列(不只是单个)
 */
public class DirectExchangeProvider {
    private final static String DEFAULT_QUEUE_NAME = "basic-1";
    private final static String OTHER_QUEUE_NAME = "basic-2";
    private final static String EXCHANGE_NAME = "exchange-direct";
    private final static Logger logger = LoggerFactory.getLogger(DirectExchangeProvider.class);

    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(
                DEFAULT_QUEUE_NAME, false, false, false, null);
        channel.queueDeclare(
                OTHER_QUEUE_NAME, false, false, false, null);

        /*
            创建交换机 direct 并绑定
        */
        // 创建 direct 交换机
        channel.exchangeDeclare(EXCHANGE_NAME, RabbitMQExchangeType.DIRECT.getValue());
        // 绑定队列到交换机
        channel.queueBind(DEFAULT_QUEUE_NAME, EXCHANGE_NAME, "one");
        channel.queueBind(OTHER_QUEUE_NAME, EXCHANGE_NAME, "two");

        // 读取键盘输入发布消息
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String message = scanner.next();
            String[] split = message.split(";");
            if (split.length == 2) {
                String key = split[0];
                String messageBody = split[1];
                channel.basicPublish(EXCHANGE_NAME, key, null, messageBody.getBytes());
                logger.info(" [ScannerProvider] Sent '{}' to key {} ", messageBody, key);
            }
        }
    }
}
