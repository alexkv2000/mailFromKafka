package kvo.separat;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.json.JSONObject;

import java.io.BufferedInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaFileUploader_sendKafka {
    private static final String TOPIC = "test_topic";
    private static final String BROKER = "localhost:9092";
    private static final String GROUP_ID = "consumer-nifi-test";

    public static void main(String[] args) {
        // Настройки для подключения к Kafka Consumer
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC));

        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                processMessage(record.value());
            }
        }
    }

    private static void processMessage(String message) {
        JSONObject jsonMessage = new JSONObject(message);
        String to = jsonMessage.getString("To");
        String caption = jsonMessage.getString("Caption");
        String body = jsonMessage.getString("Body");
        String url = jsonMessage.getString("Url");

        // Загрузка файла из URL
        String filePath = downloadFile(url);

        // Отправка сообщения с вложенным файлом
        sendMessage(to, caption, body, filePath);
    }

    private static String downloadFile(String fileUrl) {
        String filePath = "C:\\Users\\KvochkinAY\\Desktop\\tmp\\attach"; // Укажите путь для сохранения файла
        try (InputStream in = new URL(fileUrl).openStream();
             BufferedInputStream bis = new BufferedInputStream(in);
             FileOutputStream fos = new FileOutputStream(filePath)) {

            byte[] dataBuffer = new byte[1024];
            int bytesRead;
            while ((bytesRead = bis.read(dataBuffer, 0, dataBuffer.length)) != -1) {
                fos.write(dataBuffer, 0, bytesRead);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return filePath;
    }

    private static void sendMessage(String to, String caption, String body, String filePath) {
        // Настройки для подключения к Kafka Producer
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", BROKER);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);

        // Создание сообщения
        String message = String.format("To: %s, Caption: %s, Body: %s, File: %s", to, caption, body, filePath);
        ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, message);

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    exception.printStackTrace();
                } else {
                    System.out.println("Message sent successfully: " + metadata.toString());
                }
            }
        });

        producer.close();
    }
}
