package fun.javierchen.mqbasic.exchange.topic;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import fun.javierchen.mqbasic.exchange.RabbitMQExchangeType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

/**
 * topic 将消息发送到指定队列(通过多个标准)
 *
 * 这个交换机的路由方式不是依靠单个路由键 而是多个单词通过 . 来进行分隔
 * 路由键的长度不超过 255 个字节
 *
 * 一个中文的词也通过 . 来进行分隔
 *
 * 支持占位符
 *  - * 表示一个单词 或者 一个中文的词语
 *  - # 匹配零个或者多个单词
 *
 * 占位符不可以和单词在一起 必须通过 . 来隔开
 */
public class TopicExchangeProvider {
    private final static String DEFAULT_QUEUE_NAME = "basic-1";
    private final static String OTHER_QUEUE_NAME = "basic-2";
    private final static String THIRD_QUEUE_NAME = "basic-3";
    private final static String EXCHANGE_NAME = "exchange-topic";
    private final static Logger logger = LoggerFactory.getLogger(TopicExchangeProvider.class);

    public static void main(String[] args) throws Exception {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();
        channel.queueDeclare(
                DEFAULT_QUEUE_NAME, false, false, false, null);
        channel.queueDeclare(
                OTHER_QUEUE_NAME, false, false, false, null);
        channel.queueDeclare(
                THIRD_QUEUE_NAME, false, false, false, null);

        /*
            创建交换机 topic 并绑定
        */
        // 创建 topic 交换机
        channel.exchangeDeclare(EXCHANGE_NAME, RabbitMQExchangeType.TOPIC.getValue());
        // 绑定队列到交换机
        // one. 为后缀的键
        channel.queueBind(DEFAULT_QUEUE_NAME, EXCHANGE_NAME, "#.one");
        // two. 为前缀的 后面有多个词汇的键
        channel.queueBind(OTHER_QUEUE_NAME, EXCHANGE_NAME, "two.#");
        // three. 为前缀的 后面只有一个词汇的键
        channel.queueBind(THIRD_QUEUE_NAME, EXCHANGE_NAME, "three.*");


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
