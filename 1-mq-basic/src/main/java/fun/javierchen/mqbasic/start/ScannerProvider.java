package fun.javierchen.mqbasic.start;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

public class ScannerProvider {
    private final static String QUEUE_NAME = "basic-1";
    private final static Logger logger = LoggerFactory.getLogger(ScannerProvider.class);

    public static void main(String[] args) throws Exception {

        ConnectionFactory connectionFactory = new ConnectionFactory();
        // 已经有了默认值
        // connectionFactory.setHost("localhost");
        Connection connection = connectionFactory.newConnection();
        // 创建 channel 对象
        Channel channel = connection.createChannel();
        // 创建队列
        // 参数：队列名称，是否持久化，是否独占，是否自动删除，参数
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        // 读取键盘输入发布消息
        Scanner scanner = new Scanner(System.in);
        while (true) {
            String message = scanner.next();
            String[] split = message.split(";");
            if (split.length == 2) {
                String queueName = split[0];
                String messageBody = split[1];
                channel.basicPublish("", queueName, null, messageBody.getBytes());
                logger.info(" [ScannerProvider] Sent '{}' to {} ", messageBody, queueName);
            }
        }
    }
}
