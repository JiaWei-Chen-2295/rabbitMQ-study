package fun.javierchen.mqbasic.create.queue;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class OtherProvider {
    private static final String QUEUE_NAME = "exclusive_queue";

    public static void main(String[] args) {

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8");
            System.out.println(message);
        };

        try {
            Connection connection1 = new ConnectionFactory().newConnection();
            Channel otherChannel = connection1.createChannel();
            // cannot obtain exclusive access to locked queue 'exclusive_queue' in vhost '/'.
            // 不可以使用其他的连接去访问 inclusive 队列
            otherChannel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> {
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
