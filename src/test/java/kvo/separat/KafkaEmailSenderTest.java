package kvo.separat;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.mail.Session;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Properties;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
public class KafkaEmailSenderTest {
    static ConfigLoader configLoader;
    //   private KafkaEmailSender kafkaEmailSender;
    static {
        try {
            configLoader = new ConfigLoader("src/main/setting.txt");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    private static final String TOPIC = configLoader.getProperty("TOPIC");
    private static final String BROKER = configLoader.getProperty("BROKER");
    private static final String GROUP_ID = configLoader.getProperty("GROUP_ID");
    private static final String EMAIL = configLoader.getProperty("EMAIL");
    private static final String PASSWORD = configLoader.getProperty("PASSWORD");
    private static final String SMTP_SERVER = configLoader.getProperty("SMTP_SERVER");
    private static final String FILE_PATH = configLoader.getProperty("FILE_PATH");
    private static final int NUM_THREADS = Integer.parseInt(configLoader.getProperty("NUM_THREADS"));
    private static final int NUM_ATTEMPT = Integer.parseInt(configLoader.getProperty("NUM_ATTEMPT"));
    @BeforeEach
    public void setUp() {
        KafkaEmailSender kafkaEmailSender = new KafkaEmailSender();
    }

    @Test
    public void testDownloadFile_Success() throws IOException {
        String testUrl = "http://zagorie.ru/upload/iblock/4ea/4eae10bf98dde4f7356ebef161d365d5.pdf";
        UUID uuid = UUID.randomUUID();
        // Создать директорию (из IDDoc)
        Files.createDirectories(Path.of((FILE_PATH + uuid)));
        String result = KafkaEmailSender.downloadFile(testUrl, uuid);

        // Проверяем, что файл был загружен по ожидаемому пути
        //ssertTrue(Files.exists(Path.of(KafkaEmailSender.configLoader.getProperty(FILE_PATH) + uuid.toString() + "/4eae10bf98dde4f7356ebef161d365d5.pdf")));
        Path expectedFilePath = Paths.get(result);
        assertTrue(Files.exists(expectedFilePath), "Файл не был загружен по ожидаемому пути: " + expectedFilePath);
        // Удаляем файл после теста
        Files.walk(Path.of((FILE_PATH + uuid))).sorted((a, b) -> b.compareTo(a)).forEach(p -> {
                    try {
                        Files.delete(p);
                    } catch (IOException e) {
                    }
                });
        assertTrue(Files.notExists(expectedFilePath), "директория " + expectedFilePath + "не удалилась");
    }

    @Test
    public void testSendEmail_Success() throws NoSuchAlgorithmException, KeyManagementException {
        // Создаем мок-объект для Session
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", SMTP_SERVER);
        props.put("mail.smtp.port", "587");
        props.put("mail.smtp.ssl.socketFactory", SSLSocketFactory.getDefault());
        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, null, new java.security.SecureRandom());
        SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
        props.put("mail.smtp.ssl.socketFactory", sslSocketFactory);
        KafkaEmailSender.setSession(Session.getInstance(props)); // Предполагаем, что есть метод для установки сессии

        String to = "test@example.com";
        String toCC = "cc@example.com";
        String caption = "Test Email";
        String body = "This is a test email.";
        String filePaths = ""; // Без вложений

        // Вызов метода sendEmail
        assertDoesNotThrow(() -> KafkaEmailSender.sendEmail(to, toCC, caption, body, filePaths));
    }

    @Test
    public void testProcessMessage_Success() throws IOException {
        // Создаем тестовое сообщение JSON
        JSONObject jsonMessage = new JSONObject();
        jsonMessage.put("To", "test@example.com");
        jsonMessage.put("ToCC", "cc@example.com");
        jsonMessage.put("Caption", "Test Email");
        jsonMessage.put("Body", "This is a test email.");
        jsonMessage.put("Url", new JSONArray().put("4eae10bf98dde4f7356ebef161d365d5.pdf"));

        // Вызов метода processMessage
        assertDoesNotThrow(() -> KafkaEmailSender.processMessage(jsonMessage.toString()));
    }

    @Test
    public void testDeleteDirectory_Success() throws IOException {
        // Создаем временную директорию для тестирования
        Path tempDir = Files.createTempDirectory("testDir");
        Files.createFile(tempDir.resolve("4eae10bf98dde4f7356ebef161d365d5.pdf"));

        // Убедимся, что файл существует перед удалением
        assertTrue(Files.exists(tempDir.resolve("4eae10bf98dde4f7356ebef161d365d5.pdf")));

        // Вызов метода deleteDirectory
        KafkaEmailSender.deleteDirectory(tempDir);

        // Проверяем, что директория и файл были удалены
        assertFalse(Files.exists(tempDir));
    }
}
