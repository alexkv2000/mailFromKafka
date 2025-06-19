package kvo.separat.kafkaConsumer;

import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.mail.*;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import static kvo.separat.ConsumerServerDV.configLoader;

public class EmailService {
    private static final int NUM_ATTEMPT = Integer.parseInt(configLoader.getProperty("NUM_ATTEMPT"));
    private static final Logger logger = LoggerFactory.getLogger(EmailService.class);
    private final String email;
    private final String password;
    private final String smtpServer;

    public EmailService(ConfigLoader configLoader) {
        this.email = configLoader.getProperty("EMAIL");
        this.password = configLoader.getProperty("PASSWORD");
        this.smtpServer = configLoader.getProperty("SMTP_SERVER");
    }

    public void sendMessage(MessageData messageData, FileService fileService) throws IOException {
        String to = messageData.getTo();
        String toCC = messageData.getToCC();
        String caption = messageData.getCaption();
        String body = messageData.getBody();
        JSONArray urls = messageData.getUrls();
        UUID uuid = messageData.getUuid();

        String filePaths = fileService.downloadFiles(urls, uuid);

        logger.info("Start send Email ...");
        sendMail(to, toCC, caption, body, filePaths);
        logger.info("Stop send Email ...");

        fileService.deleteDirectory(uuid);
        logger.info("Directory deleted access ..." + fileService.getFilePath(uuid));

    }

    private void sendMail(String to, String toCC, String caption, String body, String filePaths) {
        Properties props = new Properties();
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.host", smtpServer);
        props.put("mail.smtp.port", "587");

        Session session = Session.getInstance(props, new Authenticator() {
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(email, password);
            }
        });

        try {
            Message message = new MimeMessage(session);
            message.setFrom(new InternetAddress(email));
            message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
            if (toCC != null && !toCC.isEmpty()) {
                message.setRecipients(Message.RecipientType.CC, InternetAddress.parse(toCC));
            }
            message.setSubject(caption);

            MimeMultipart multipart = new MimeMultipart();

            MimeBodyPart textPart = new MimeBodyPart();
            textPart.setText(body);
            multipart.addBodyPart(textPart);

            if (filePaths != null && !filePaths.isEmpty()) {
                String[] files = filePaths.split(", ");
                for (String file : files) {
                    MimeBodyPart attachmentPart = new MimeBodyPart();
                    attachmentPart.attachFile(file);
                    multipart.addBodyPart(attachmentPart);
                }
            }

            message.setContent(multipart);
            int num_attempts = NUM_ATTEMPT;
            while (num_attempts != 0) {
                try {
                    Transport.send(message);
                    logger.info("Email sent successfully " + message.getSubject());
                    break;
                } catch (Exception ee) {
                    ee.printStackTrace();
                    logger.error("An error 'sendEmail' To or ToCC " + message.getSubject(), ee);
                }
                num_attempts--;
            }
            logger.info("Email " + message.getSubject() + "sent successfully to: " + to);
        } catch (MessagingException e) {
            logger.error("Error sending email", e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
