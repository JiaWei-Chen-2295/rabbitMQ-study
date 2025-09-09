package fun.javierchen.mqbasic.exchange.fanout;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import fun.javierchen.mqbasic.exchange.RabbitMQExchangeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * exchange 的作用就是路由信息到队列
 * fan-out 类型的交换机，将信息发送给所有绑定的队列
 */
public class FanoutExchangeProvider {
    private final static String DEFAULT_QUEUE_NAME = "basic-1";
    private final static String OTHER_QUEUE_NAME = "basic-2";
    private final static String EXCHANGE_NAME = "exchange-fanout";
    private final static Logger logger = LoggerFactory.getLogger(FanoutExchangeProvider.class);

    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(
                DEFAULT_QUEUE_NAME, false, false, false, null);
        channel.queueDeclare(
                OTHER_QUEUE_NAME, false, false, false, null);


        /*
            创建交换机 fanout 并绑定
        */
        // 创建 fanout 交换机
        channel.exchangeDeclare(EXCHANGE_NAME, RabbitMQExchangeType.FANOUT.getValue());
        // 绑定队列到交换机
        channel.queueBind(DEFAULT_QUEUE_NAME, EXCHANGE_NAME, "");
        channel.queueBind(OTHER_QUEUE_NAME, EXCHANGE_NAME, "");

        // 读取键盘输入发布消息
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String message = scanner.next();
            String[] split = message.split(";");
            if (split.length == 2) {
                String queueName = split[0];
                String messageBody = split[1];
                channel.basicPublish(EXCHANGE_NAME, "", null, messageBody.getBytes());
                logger.info(" [ScannerProvider] Sent '{}' to {} ", messageBody, queueName);
            }
        }
    }
}
