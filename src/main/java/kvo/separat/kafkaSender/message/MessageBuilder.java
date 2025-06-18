package kvo.separat.kafkaSender.message;

import kvo.separat.kafkaSender.producer.KafkaProducerFactory;
import kvo.separat.kafkaSender.producer.MessageSender;
import kvo.separat.kafkaSender.producer.Producer;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

//Строитель для сообщений
public class MessageBuilder {
    private String to;
    private String toCC;
    private String caption;
    private String body;
    private List<String> urls;
    private UUID uuid;

    private MessageBuilder() {
    }

    public static MessageBuilder builder() {
        return new MessageBuilder();
    }

    public MessageBuilder uuid() {
        this.uuid = UUID.randomUUID();
        return this;
    }

    public MessageBuilder to(String to) {
        this.to = to;
        return this;
    }
    public MessageBuilder toCC(String toCC) {
        this.toCC = toCC;
        return this;
    }

    public MessageBuilder caption(String caption) {
        this.caption = caption;
        return this;
    }

    public MessageBuilder body(String body) {
        this.body = body;
        return this;
    }

    public MessageBuilder urls(List<String> urls) {
        this.urls = urls;
        return this;
    }

    public Message build() {
        return new JsonMessage(to, toCC, caption, body, urls, uuid);
    }
}
