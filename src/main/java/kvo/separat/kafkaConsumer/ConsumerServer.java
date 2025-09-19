package kvo.separat.kafkaConsumer;

import kvo.separat.mssql.MSSQLConnection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.lang.Thread.sleep;

public class ConsumerServer {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerServer.class);
    private final KafkaConsumerWrapper kafkaConsumer;
    private final DatabaseService databaseService;
    private final EmailService emailService;
    private final String topic;
    private final String server;
    private final int limitSelect;
    private final ExecutorService executor;
    private final String typeMes;
    private final String filePath;
    private final long threadSleep;
    private final ScheduledExecutorService scheduler;
    private final String urlMssql;
    private final String userMssql;
    private final String passwordMssql;
    private int deleteAfterDay;

    public ConsumerServer(KafkaConsumerWrapper kafkaConsumer, DatabaseService databaseService,
                          EmailService emailService, ConfigLoader configLoader) {
        this.kafkaConsumer = kafkaConsumer;
        this.databaseService = databaseService;
        this.emailService = emailService;
        this.topic = configLoader.getProperty("TOPIC");
        this.server = configLoader.getProperty("SERVER");
        this.limitSelect = Integer.parseInt(configLoader.getProperty("LIMIT_SELECT"));
        this.typeMes = configLoader.getProperty("TYPE_MES");
        this.filePath = configLoader.getProperty("FILE_PATH");
        this.executor = Executors.newFixedThreadPool(Integer.parseInt(configLoader.getProperty("NUM_THREADS")));
        this.threadSleep = Long.parseLong(configLoader.getProperty("THREAD_SLEEP"));
        this.scheduler = Executors.newScheduledThreadPool(3);
        this.urlMssql = configLoader.getProperty("URL_MSSQL");
        this.userMssql = configLoader.getProperty("USER_MSSQL");
        this.passwordMssql = configLoader.getProperty("PASSWORD_MSSQL");
        deleteAfterDay = Integer.parseInt(configLoader.getProperty("DELETE_AFTER_DAY"));
    }

    public void startProcessing() {
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                Connection connection = DriverManager.getConnection(urlMssql, userMssql, passwordMssql);
                MSSQLConnection.deleteBinMoreSevenDays(connection, java.time.LocalDate.now());  // Удаление Binary из MSSQL больше 7 дней
                DatabaseService.deleteOldMessages(deleteAfterDay); // Уделание Messages из MySQL больше 7 дней
            } catch (Exception e) {
                logger.error("Error in ConsumerServer.startProcessing.MSSQLConnection.deleteBinMoreSevenDay ", e);
            }
        }, 0, 1, TimeUnit.DAYS);

        scheduler.scheduleWithFixedDelay(() -> {
            try {
                this.setKafkaConsumer();
            } catch (Exception e) {
                logger.error("Error in ConsumerServer.startProcessing.setKafkaConsumer", e);
            }
        }, 0, 5, TimeUnit.SECONDS);

        scheduler.scheduleWithFixedDelay(() -> {
            try {
                this.processMessages();
            } catch (Exception e) {
                logger.error("Error in ConsumerServer.startProcessing.processMessages", e);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    private void setKafkaConsumer() {
        logger.debug("Start setKafkaConsumer: getConsumerRecords()");
        List<ConsumerRecord<String, String>> recordList = getConsumerRecords();
        logger.debug("Start setKafkaConsumer: AddCorrectDataJSONFromBrokerToDBSQL()");
        addCorrectDataJSONFromBrokerToDBSQL(recordList);
    }

    private void processMessages() {
        try {
            logger.debug("Поиск данных сообщений из БД");
            databaseService.updateMessagesForProcessing(topic, server, "select", typeMes, limitSelect);

            logger.debug("Выборка данных сообщений из БД");
            List<MessageData> resultSet = databaseService.selectMessages(topic, server, typeMes, limitSelect);

            if (resultSet == null || resultSet.isEmpty()) {
                logger.debug("Не сообщений в обработке");
                return;
            }

            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (MessageData result : resultSet) {
                if (!isValidMessage(result)) {
                    logger.error("Некорректные данные в сообщении с ID: {}", result != null ? result.getId() : "null");
                    updateMessageStatusWithError(result, "error DATA JSON");
                    continue;
                }

                futures.add(processSingleMessageAsync(result));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        } catch (Exception e) {
            logger.error("Критическая ошибка в обработке сообщений ", e);
        }
    }

    private boolean isValidMessage(MessageData message) {
        return message != null && message.getUuid() != null &&
                (message.getTo() != null || message.getToCC() != null);
    }

    private CompletableFuture<Void> processSingleMessageAsync(MessageData message) {
        Path dir = Paths.get(filePath);
        if (!Files.exists(dir)) {
            try {
                Files.createDirectories(dir);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return CompletableFuture.runAsync(() -> {
            try {
                StringBuilder filePathBuilder = MSSQLConnection.DownloadBinaryDV(
                        message.getUuid(), urlMssql, userMssql, passwordMssql, filePath);

                emailService.sendMail(message.getTo(), message.getToCC(), message.getBCC(),
                        message.getCaption(), message.getBody(), filePathBuilder.toString());

                cleanupTempFiles(String.valueOf(message.getUuid()));
                updateMessageStatus(String.valueOf(message.getId()), "send");

            } catch (IOException e) {
                logger.error("IO error процесса сообщения с ID: {}", message.getId(), e);
                updateMessageStatusWithError(message, "IO error");
            }
        }, executor);
    }

    private void cleanupTempFiles(String uuid) throws IOException {
        Path tempDir = Path.of(filePath + uuid);
        if (Files.exists(tempDir)) {
            MSSQLConnection.deleteDirectory(UUID.fromString(uuid), filePath);
        }
    }

    private void updateMessageStatus(String messageId, String status) {
        try {
            databaseService.updateMessageStatusDate(topic, server, Integer.valueOf(messageId),
                    status, new Timestamp(System.currentTimeMillis()));
        } catch (SQLException e) {
            logger.error("Ошибка обновления Статуса '{}' для сообщения с ID: {}", status, messageId, e);
        }
    }

    private void updateMessageStatusWithError(MessageData message, String errorType) {
        if (message != null) {
            updateMessageStatus(String.valueOf(message.getId()), errorType);
        }
    }

    private void handleProcessingError(MessageData message, SQLException e) {
        logger.error("Ошибка процесса обработки сообщения ID: {}", message.getId(), e);
        updateMessageStatus(String.valueOf(message.getId()), "error");
    }

    public void start() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                processSingleIteration();
                sleep(threadSleep);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info("Поток прерван, останавливается");
                break;
            } catch (Exception e) {
                logger.error("Ошибка в main процессе цикла loop", e);
            }
        }
    }

    private void processSingleIteration() {
        List<ConsumerRecord<String, String>> recordList = getConsumerRecords();
        addCorrectDataJSONFromBrokerToDBSQL(recordList);

        databaseService.updateMessagesForProcessing(topic, server, "select", typeMes, limitSelect);
        List<MessageData> resultSet = databaseService.selectMessages(topic, server, typeMes, limitSelect);

        if (resultSet == null || resultSet.isEmpty()) {
            logger.debug("Нет сообщений для обработки ");
            return;
        }

        List<CompletableFuture<Void>> futures = resultSet.stream()
                .filter(this::isValidMessage)
                .map(this::processSingleMessageAsync)
                .toList();
        //.collect(Collectors.toList()); //заменил на  .toList();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private void addCorrectDataJSONFromBrokerToDBSQL(List<ConsumerRecord<String, String>> recordList) {
        for (ConsumerRecord<String, String> record : recordList) {
            try {
                JSONObject json = new JSONObject(record.value());
                // Проверяем наличие ключа "typeMes" и вставляем в БД
                if (json.has("typeMes")) {
                    String extractedTypeMes = json.getString("typeMes");  // Извлекаем значение {typeMes} как строку
                    String extractedUUID = json.getString("uuid"); // Извлекаем значение {uuid} как строку
                    databaseService.insertMessages(Collections.singletonList(record), extractedTypeMes, extractedUUID);
                } else {
                    logger.warn("Ключ 'typeMes' отсутствует в JSON-сообщении: {}", record.value());
                }
            } catch (JSONException e) {
                logger.error("Ошибка обработки JSON в сообщении (возможно, некорректный JSON или typeMes): {}", record.value(), e);
            }
        }
    }

    private List<ConsumerRecord<String, String>> getConsumerRecords() {
        Iterable<ConsumerRecord<String, String>> records = kafkaConsumer.pollRecords();
        return StreamSupport.stream(records.spliterator(), false)
                .collect(Collectors.toList());
    }

    public void stopProcessing() {
        scheduler.shutdown();
        executor.shutdown();

        try {
            if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    public static void main(String[] args) throws IOException {
        String currentDir = System.getProperty("user.dir");
        String configPath = currentDir + "/config/setting.txt";
        boolean useWhileLoop = false;

        for (String arg : args) {
            if (arg.startsWith("config.path=")) {
                configPath = arg.substring("config.path=".length());
            } else if (arg.startsWith("while=")) {
                useWhileLoop = Boolean.parseBoolean(arg.substring("while=".length()));
            }
        }

        logger.info("Старовала схема : {}", useWhileLoop ? "While-Loop" : "Scheduled");

        ConfigLoader configLoader = new ConfigLoader(configPath);
        KafkaConsumerWrapper kafkaConsumer = new KafkaConsumerWrapper(configLoader);
        DatabaseService databaseService = new DatabaseService(configLoader);
        EmailService emailService = new EmailService(configLoader);

        ConsumerServer consumerServer = new ConsumerServer(kafkaConsumer, databaseService, emailService, configLoader);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Послан сигнал остановки приложения");
            consumerServer.stopProcessing();
            logger.info("Приложение остановлено корректно");
        }));

        if (useWhileLoop) {
            consumerServer.start();
        } else {
            consumerServer.startProcessing();
        }
    }
}
