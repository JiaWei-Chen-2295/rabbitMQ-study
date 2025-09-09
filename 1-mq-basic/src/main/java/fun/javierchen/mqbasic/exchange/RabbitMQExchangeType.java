package fun.javierchen.mqbasic.exchange;

public enum RabbitMQExchangeType {
    /**
     * 将消息发送到所有绑定的队列
     */
    FANOUT("fanout"),
    /**
     * 将消息发送给匹配到的队列
     */
    DIRECT("direct"),
    TOPIC("topic"),
    HEADERS("header");

    private String value;

    RabbitMQExchangeType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }
}
