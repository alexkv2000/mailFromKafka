package kvo.separat.kafkaConsumer;

import kvo.separat.SoapDownloadBinaryDV;
import static kvo.separat.SoapDownloadBinaryDV.deleteDirectory;

import kvo.separat.mssql.MSSQLConnectionExample;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class ConsumerServer {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerServer.class);
    private final KafkaConsumerWrapper kafkaConsumer;
    private final DatabaseService databaseService;
    private final EmailService emailService;
//    private  final SoapDownloadBinaryDV downloadFilesFromJSON;
    private final String topic;
    private final String server;
    private final int limitSelect;
    private final ExecutorService executor;
    private final String typeMes;
    private final MSSQLConnectionExample mssqlConnectionExample;

    public ConsumerServer(KafkaConsumerWrapper kafkaConsumer, DatabaseService databaseService, EmailService emailService, SoapDownloadBinaryDV downloadFilesFromJSON, MSSQLConnectionExample mssqlConnectionExample, ConfigLoader configLoader) {
        this.kafkaConsumer = kafkaConsumer;
        this.databaseService = databaseService;
        this.emailService = emailService;
//        this.downloadFilesFromJSON = downloadFilesFromJSON;
        this.topic = configLoader.getProperty("TOPIC");
        this.server = configLoader.getProperty("SERVER");
        this.limitSelect = Integer.parseInt(configLoader.getProperty("LIMIT_SELECT"));
        this.typeMes = configLoader.getProperty("TYPE_MES");
        this.executor = Executors.newFixedThreadPool(Integer.parseInt(configLoader.getProperty("NUM_THREADS")));
        this.mssqlConnectionExample = mssqlConnectionExample;
    }

    public void start() throws SQLException {
        databaseService.createTableIfNotExist();
        List<ConsumerRecord<String, String>> recordList;
        List<MessageData> resultSet;

        while (true) {
            recordList = getConsumerRecords();
            databaseService.insertMessages(recordList, server, typeMes);

            databaseService.updateMessagesStatus(topic, server, "select", typeMes,limitSelect);
            resultSet = databaseService.selectMessages(topic, server, typeMes, limitSelect);
            List<Future<?>> futures = new ArrayList<>();
            for (MessageData result : resultSet) {
                Future<?> future = executor.submit(() -> {
                    try {
                        result.setCaption(result.getId() + " " + result.getCaption()); ///ToDo на ПРОДЕ закоментировать!!!
                        //TODO заменить на MSSQLConnectionExample
                        // Передаем UUID получаем StringBuilder из path полный_путь_к_файлам
                        StringBuilder sPath = SoapDownloadBinaryDV.downloadFilesFromJSON(result);
                        emailService.sendMail(result.getTo(),result.getToCC(),result.getCaption(), result.getBody(), String.valueOf(sPath));
                        deleteDirectory(result.getUuid());
                        databaseService.updateMessageStatusDate(topic, server, result.getId(), "send", new Timestamp(System.currentTimeMillis()));
                    } catch (SQLException e) {
                        try {
                            databaseService.updateMessageStatusDate(topic, server, result.getId(), "error", new Timestamp(System.currentTimeMillis())); // --> добавил подсчет попыток NUM_ATTEMPT
                        } catch (SQLException ex) {
                            throw new RuntimeException(ex);
                        }
                        throw new RuntimeException(e);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
                futures.add(future);
            }
            for (Future<?> future : futures) {
                try {
                    future.get(); // Блокируем до завершения задачи
//TODO Удалить записи успешно отправленные и попытки которые превысили NUM_ATTEMPT
                } catch (Exception e) {
                    logger.info("Ошибка закачки данных по url");
                    e.printStackTrace();
                }
            }
        }
    }

    private List<ConsumerRecord<String, String>> getConsumerRecords() {
        List<ConsumerRecord<String, String>> recordList;
        Iterable<ConsumerRecord<String, String>> records;
        records = kafkaConsumer.pollRecords();
        recordList = StreamSupport.stream(records.spliterator(), false)
                .collect(Collectors.toList());
        return recordList;
    }

    public static void main(String[] args) throws SQLException, IOException {
        String currentDir = System.getProperty("user.dir");
        String configPath = currentDir + "\\config\\setting.txt";
        ConfigLoader configLoader = new ConfigLoader(configPath);

        KafkaConsumerWrapper kafkaConsumer = new KafkaConsumerWrapper(configLoader);
        DatabaseService databaseService = new DatabaseService(configLoader);
        EmailService emailService = new EmailService(configLoader);
        SoapDownloadBinaryDV downloadFilesFromJSON = new SoapDownloadBinaryDV(configLoader);

        MSSQLConnectionExample mssqlConnectionExample = new MSSQLConnectionExample(configLoader);
        ConsumerServer consumerServer = new ConsumerServer(kafkaConsumer, databaseService, emailService, downloadFilesFromJSON, mssqlConnectionExample, configLoader);
        consumerServer.start();
    }
}