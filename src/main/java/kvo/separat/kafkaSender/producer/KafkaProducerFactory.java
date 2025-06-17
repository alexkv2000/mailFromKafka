package kvo.separat.kafkaSender.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

//Фабрика для создания KafkaProducer
public class KafkaProducerFactory {

    private final String bootstrapServers;

    public KafkaProducerFactory(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public Producer createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        return new KafkaProducerImpl(kafkaProducer);
    }
}
