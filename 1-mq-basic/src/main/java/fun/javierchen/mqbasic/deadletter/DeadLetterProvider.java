package fun.javierchen.mqbasic.deadletter;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;


public class DeadLetterProvider {

    private static final String QUEUE_NAME = "a_queue";
    private static final String DLE_NAME = "dead-letter-exchange";
    private static final String DLQ_DEFAULT_NAME = "default-dead-letter-queue";
    private static final String DLQ_OTHER_NAME = "other-dead-letter-queue";

    private static final Logger logger = LoggerFactory.getLogger(DeadLetterProvider.class);

    public static void main(String[] argv) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        // 创建一个交换机用于存储死信消息
        channel.exchangeDeclare(DLE_NAME, "direct");

        // 添加死信队列
        channel.queueDeclare(DLQ_DEFAULT_NAME, false, false, false, null);
        channel.queueBind(DLQ_DEFAULT_NAME, DLE_NAME, DLQ_DEFAULT_NAME);

        // 为这个队列属性添加死信交换机
        Map<String, Object> args = new HashMap<>();
        args.put("x-dead-letter-exchange", DLE_NAME);
        // 这里指定默认的死信队列
        args.put("x-dead-letter-routing-key", DLQ_DEFAULT_NAME);
        channel.queueDeclare(QUEUE_NAME, false,
                false, false, args);
        // 读取键盘输入发布消息
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String message = scanner.next();
            String[] split = message.split(";");
            if (split.length == 2) {
                String queueName = split[0];
                String messageBody = split[1];
                // 方式二: 构造消息属性，设置消息的过期时间
                AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                        .expiration("10000")
                        .build();
                channel.basicPublish("", queueName, properties, messageBody.getBytes());
                logger.info(" [ScannerProvider] Sent '{}' to {} ", messageBody, queueName);
            }
        }

    }
}
