package fun.javierchen.mqbasic.create.queue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

/**
 * 测试 排他性队列
 * 只能使用创建队列时的连接进行访问的队列
 * 队列会对其他连接的访问进行限制
 * 创建队列时指定 exclusive 属性为 true
 * 当连接关闭或者出异常时 队列会被删除
 *
 * 使用场景：
 *  - 天然的临时队列
 *  - 一次性的消息处理，确保队列仅在特定的连接生命周期内有效
 * 建议
 *  - 建议给队列命名时 按照服务进行生成 不需要固定 名称
 */
public class ExclusiveQueueTest {
    private static final String QUEUE_NAME = "exclusive_queue";

    public static void main(String[] args) throws Exception {
        ConnectionFactory factory = new ConnectionFactory();
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();
        // 当 exclusive 这个属性为 true 时
        // 能让一个队列被当前连接使用 当连接关闭时 队列就被删除
        channel.queueDeclare(QUEUE_NAME, false, true, false, null);

        // 使用当前连接发送消息
        // 当发送完信息后 关闭连接 使用其他的连接进行消费
        for (int i = 0; i < 5; i++) {
            channel.basicPublish("", QUEUE_NAME, null, ("hello world: " + i).getBytes());
        }

        // 消费回调
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(message);
        };
        // 使用当前连接进行消费
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
        });

    }
}
