package fun.javierchen.mqbasic.start;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class SimpleProvider {
    private final static String QUEUE_NAME = "basic-1";

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
        // 发布消息
        String message = "hello world";
        channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        System.out.println(" [SimpleProvider] Sent '" + message + "'");
    }
}
