package kvo.separat.kafkaSender.producer;

import kvo.separat.kafkaSender.message.Message;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import java.util.logging.Logger;

//Отправка сообщений
public class MessageSender {
    private final Producer producer;
    Logger logger  = Logger.getLogger(getClass().getName());
    public MessageSender(Producer producer) {
        this.producer = producer;
    }

    public void sendMessage(String topic, Message message) {
        producer.send(topic, message.toJson().toString(), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    logger.info("Message sent successfully: " + metadata.toString());
                }
            }
        });
    }
}
