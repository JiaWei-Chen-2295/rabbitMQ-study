package fun.javierchen.mqbasic.ttl;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class ScannerProvider {
    private final static String QUEUE_NAME = "basic-with-ttl";
    private final static Logger logger = LoggerFactory.getLogger(ScannerProvider.class);

    public static void main(String[] args) throws Exception {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        // 方式一：通过增加参数x-message-ttl，设置队列消息的过期时间
//        Map<String, Object> arv = new HashMap<>();
//        arv.put("x-message-ttl", 60000);
//        channel.queueDeclare(QUEUE_NAME, false, false, false, arv);
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
