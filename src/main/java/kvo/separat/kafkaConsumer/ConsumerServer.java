package kvo.separat.kafkaConsumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ConsumerServer {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerServer.class);
    private final KafkaConsumerWrapper kafkaConsumer;
    private final DatabaseService databaseService;
    private final EmailService emailService;
    private final FileService fileService;
    private final String topic;
    private final String server;
    private final int limitSelect;
    //    private final int numThreads;
    private final ExecutorService executor;

    public ConsumerServer(KafkaConsumerWrapper kafkaConsumer, DatabaseService databaseService, EmailService emailService, FileService fileService, ConfigLoader configLoader) {
        this.kafkaConsumer = kafkaConsumer;
        this.databaseService = databaseService;
        this.emailService = emailService;
        this.fileService = fileService;
        this.topic = configLoader.getProperty("TOPIC");
        this.server = configLoader.getProperty("SERVER");
        this.limitSelect = Integer.parseInt(configLoader.getProperty("LIMIT_SELECT"));
        //int numThreads = Integer.parseInt(configLoader.getProperty("NUM_THREADS"));
        this.executor = Executors.newFixedThreadPool(Integer.parseInt(configLoader.getProperty("NUM_THREADS")));
    }

    public void start() throws SQLException, InterruptedException {
        databaseService.createTableIfNotExist();

        while (true) {
            Iterable<ConsumerRecord<String, String>> records = kafkaConsumer.pollRecords();

            // Преобразуем Iterable в List
            List<ConsumerRecord<String, String>> recordList = StreamSupport.stream(records.spliterator(), false)
                    .collect(Collectors.toList());

            databaseService.insertMessages(recordList, topic, server);
            databaseService.updateMessagesStatus(topic, server, limitSelect, server);

            List<MessageData> resultSet = databaseService.selectMessages(topic, server, limitSelect);
            for (MessageData result : resultSet) {

                executor.submit(() -> {
                    try {
                        emailService.sendMessage(result, fileService); // --> Отправить сообщение
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                databaseService.updateMessageStatus(result.getId(), "send"); // --> Обновление статуса и времени отправки
            }
        }

//            databaseService.processMessages(topic, server, limitSelect, message -> {
//                executor.submit(() -> {
//                    try {
//                        emailService.sendMessage(message, fileService);
//                    } catch (IOException e) {
//                        logger.error("Error sending message", e);
//                    } finally {
//                        try {
//                            databaseService.updateMessageStatus(message.getId(), "send");
//                        } catch (SQLException e) {
//                            logger.error("Error updating message status", e);
//                        }
//                    }
//                });
//            });
    }


    public static void main(String[] args) throws SQLException, IOException, InterruptedException {
        ConfigLoader configLoader = new ConfigLoader("src/main/setting.txt");

        KafkaConsumerWrapper kafkaConsumer = new KafkaConsumerWrapper(configLoader);
        DatabaseService databaseService = new DatabaseService(configLoader);
        EmailService emailService = new EmailService(configLoader);
        FileService fileService = new FileService(configLoader);

        ConsumerServer consumerServer = new ConsumerServer(kafkaConsumer, databaseService, emailService, fileService, configLoader);
        consumerServer.start();
    }
}
