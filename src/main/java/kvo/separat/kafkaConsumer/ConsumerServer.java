package kvo.separat.kafkaConsumer;

import io.prometheus.client.Histogram;
import kvo.separat.mssql.MSSQLConnection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.prometheus.client.CollectorRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.exporter.HTTPServer;

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
    private static PrometheusMeterRegistry registery;
    private static final Logger logger = LoggerFactory.getLogger(ConsumerServer.class);

    private static HTTPServer metricsServer;

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
    private static Counter messagesConsumed;
    private static Counter messagesProcessed;
    private static Timer messagesProcessedTimer;
    private static Counter messagesSendingFailed;
    private static Timer messagesConsumerTimer;
    private static Counter messagesConsumerFailed;
    private static Counter messagesProcessedFailed;
    private static Histogram processingLatency;
    private static final String ConsumerServer = "ConsumerServer";
    private static void initializeMonitoring() {
        try {
            // Создание Prometheus registry
            registery = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

            // Инициализация метрик
            messagesConsumed = Counter.builder("consumer_messages_consumed_total")
                    .description("Total number consumer messages")
                    .tag("application", ConsumerServer)
                    .register(registery);

            messagesProcessed = Counter.builder("messages_sent_total")
                    .description("Total number sent messages")
                    .tag("application", ConsumerServer)
                    .register(registery);

           messagesSendingFailed = Counter.builder("messages_sent_failed_total")
                   .description("Total number failed sent messages")
                   .tag("application", ConsumerServer)
                   .register(registery);

            messagesConsumerTimer = Timer.builder("messages_duration_seconds")
                    .description("Messages synchronization duration in seconds")
                    .tag("application", ConsumerServer)
                    .register(registery);
            messagesProcessedTimer = Timer.builder("messages_sent_failed_duration")
                    .description("Total number Failed sent messages")
                    .tag("application", ConsumerServer)
                    .register(registery);
            messagesConsumerFailed = Counter.builder("consumer_messages_failed_total")
                    .description("Total number failed consumer messages")
                    .tag("application", ConsumerServer)
                    .register(registery);
            messagesProcessedFailed = Counter.builder("messages_sent_processed_failed_total")
                    .description("Total number failed processed messages")
                    .tag("application", ConsumerServer)
                    .register(registery);
// Инициализация Histogram через Prometheus client
            processingLatency = Histogram.build()
                    .name("processing_latency_seconds")
                    .help("Latency of message processing in seconds.")
                    .register();

            // Биндеры для мониторинга JVM
            new JvmMemoryMetrics().bindTo(registery);
            new JvmGcMetrics().bindTo(registery);
            new JvmThreadMetrics().bindTo(registery);
            new ProcessorMetrics().bindTo(registery);
            new UptimeMetrics().bindTo(registery);

            // Запуск HTTP сервера для Prometheus
            CollectorRegistry collectorRegistry = registery.getPrometheusRegistry();

            metricsServer = new HTTPServer.Builder()
                    .withPort(9090)
                    .withRegistry(collectorRegistry)
                    .build();

            logger.info("Metrics server started on http://......:9090/metrics");

        } catch (Exception e) {
            logger.error("Failed to initialize monitoring in ConsumerServer", e);
        }
    }
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
        this.deleteAfterDay = Integer.parseInt(configLoader.getProperty("DELETE_AFTER_DAY"));
    }

    public void startProcessing() {
        initializeMonitoring();
        scheduler.scheduleWithFixedDelay(() -> {
            try (Connection connection = DriverManager.getConnection(urlMssql, userMssql, passwordMssql)) {
                MSSQLConnection.deleteBinMoreSevenDays(connection, java.time.LocalDate.now());
                DatabaseService.deleteOldMessages(deleteAfterDay);
            } catch (Exception e) {
                logger.error("Error in MSSQL and DB cleanup tasks", e);
            }
        }, 0, 1, TimeUnit.DAYS);

        scheduler.scheduleWithFixedDelay(() -> {
            Timer.Sample sample = Timer.start(registery); // Регистрируем Метрику
            try {
                setKafkaConsumer();
            } catch (Exception e) {
                logger.error("Error in setKafkaConsumer", e);
                messagesConsumerFailed.increment(); // ошибка получения сообщения KAFKA
            }
            sample.stop(messagesConsumerTimer); // Стоп Метрика
        }, 0, 5, TimeUnit.SECONDS);

        scheduler.scheduleWithFixedDelay(() -> {
            Timer.Sample sample = Timer.start(registery); // Регистрируем Метрику
            try {
                processMessages();
            } catch (Exception e) {
                logger.error("Error in processMessages", e);
                messagesProcessedFailed.increment(); // ошибка отправки сообщения
            }
            sample.stop(messagesProcessedTimer); // Стоп Метрика
        }, 0, 5, TimeUnit.SECONDS);

    }

    private void setKafkaConsumer() {
        logger.debug("Polling Kafka records...");
        List<ConsumerRecord<String, String>> recordList = getConsumerRecords();
        addCorrectDataJSONFromBrokerToDBSQL(recordList);
    }

    private void processMessages() {
        try {
            logger.debug("Updating messages status to 'select' for processing");
            databaseService.updateMessagesForProcessing(topic, server, "select", typeMes, limitSelect);

            logger.debug("Selecting messages for processing");
            List<MessageData> resultSet = databaseService.selectMessages(topic, server, typeMes, limitSelect);

            if (resultSet == null || resultSet.isEmpty()) {
                logger.debug("No messages to process");
                return;
            }

            List<CompletableFuture<Void>> futures = new ArrayList<>();

            for (MessageData message : resultSet) {
                if (!isValidMessage(message)) {
                    logger.error("Invalid message data with ID: {}", message != null ? message.getId() : "null");
                    updateMessageStatusWithError(message, "error DATA JSON");
                    continue;
                }

                futures.add(processSingleMessageAsync(message));
            }

            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        } catch (Exception e) {
            logger.error("Critical error during message processing", e);
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
                logger.error("Failed to create directory: {}", filePath, e);
            }
        }
        return CompletableFuture.runAsync(() -> {
            Histogram.Timer timer = processingLatency.startTimer();
            try {
                StringBuilder filePathBuilder = MSSQLConnection.DownloadBinaryDV(
                        message.getUuid(), urlMssql, userMssql, passwordMssql, filePath);

                emailService.sendMail(message.getTo(), message.getToCC(), message.getBCC(),
                        message.getCaption(), message.getBody(), filePathBuilder.toString());

                cleanupTempFiles(String.valueOf(message.getUuid()));
                updateMessageStatus(String.valueOf(message.getId()), "send");

                messagesProcessed.increment();

            } catch (IOException e) {
                messagesSendingFailed.increment();
                logger.error("IO error processing message ID: {}", message.getId(), e);
                updateMessageStatusWithError(message, "IO error");
            } finally {
                timer.observeDuration();
            }
        }, executor);
    }

    private void cleanupTempFiles(String uuid) {
        Path tempDir = Paths.get(filePath + uuid);
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

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
    }

    private void addCorrectDataJSONFromBrokerToDBSQL(List<ConsumerRecord<String, String>> recordList) {
        for (ConsumerRecord<String, String> recordMessage : recordList) {
            try {
                JSONObject json = new JSONObject(recordMessage.value());
                // Проверяем наличие ключа "typeMes" и вставляем в БД
                if (json.has("typeMes")) {
                    String extractedTypeMes = json.getString("typeMes");  // Извлекаем значение {typeMes} как строку
                    String extractedUUID = json.getString("uuid"); // Извлекаем значение {uuid} как строку
                    databaseService.insertMessages(Collections.singletonList(recordMessage), extractedTypeMes, extractedUUID);
                    messagesConsumed.increment();
                } else {
                    logger.warn("Ключ 'typeMes' отсутствует в JSON-сообщении: {}", recordMessage.value());
                }
            } catch (JSONException e) {
                logger.error("Ошибка обработки JSON в сообщении (возможно, некорректный JSON или typeMes): {}", recordMessage.value(), e);
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
