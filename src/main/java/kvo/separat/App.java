package kvo.separat;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static java.time.LocalTime.now;

public class App {
    public static void main(String[] args) {
        String startDate = now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
        // Настройки для подключения к Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", "doc-test:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // Создание KafkaProducer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        //StringBuilder sbName = new StringBuilder();
        for (int i = 0; i < 1; i++) {
           // sbName.append("test").append(i);
            JSONObject msg = new JSONObject();
            msg.put("To", "KvochkinAY@itsnn.ru");
           // msg.put("ToСС", "AlexKv2000@mail.ru");
            msg.put("Caption", String.format("%s %d: %s ", "Потоков 20: Тема сообщения ", i,  now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))));
            msg.put("Body", String.format("%s : %d %s","Тело сообщения", i, "сообщение"));
            // Создание массива URLS
            JSONArray urls = new JSONArray();
            urls.put("http://zagorie.ru/upload/iblock/4ea/4eae10bf98dde4f7356ebef161d365d5.pdf");
            urls.put("http://tvojkomp.ru/wp-content/uploads/2016/08/pdf-forma-dlya-zapolneniya.pdf");
            urls.put("http://usefulscript.ru/download/pdf_doc_site.doc");
            urls.put("http://crewmarket.net/wp-content/uploads/application_form.xls");
            // Добавление массива URLS в JSON объект
            msg.put("Url", urls);
           // sbName.delete(0,20);
            sendMessage(producer, "test-topic", msg);
        }
        // Закрытие producer
        producer.close();
        System.out.println("Дата старта    : " + startDate + "\nДата окончания : " + now().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
    }

    private static void sendMessage(KafkaProducer<String, String> producer, String topic, JSONObject message) {
        // Отправка сообщения в Kafka
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, message.toString());

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
    }
}
