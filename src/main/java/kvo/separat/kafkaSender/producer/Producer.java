package kvo.separat.kafkaSender.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import kvo.separat.kafkaSender.message.Message;

import java.util.Properties;

//Интерфейс для Producer
public interface Producer extends AutoCloseable{
    void send(String topic, String message, Callback callback);
    @Override
    void close();
}
