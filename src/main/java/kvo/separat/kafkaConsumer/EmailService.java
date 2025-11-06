package kvo.separat.kafkaConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.mail.Session;

import jakarta.mail.*;
import jakarta.mail.internet.*;
import java.io.IOException;
import java.util.Properties;


public class EmailService {
    private final int THREAD_SLEEP;
    private final int NUM_ATTEMPT;
    private static final Logger logger = LoggerFactory.getLogger(EmailService.class);
    private final String email;
    private final String password;
    private final String smtpServer;

    public EmailService(ConfigLoader configLoader) {
        this.email = configLoader.getProperty("EMAIL");
        this.password = configLoader.getProperty("PASSWORD");
        this.smtpServer = configLoader.getProperty("SMTP_SERVER");
        this.NUM_ATTEMPT = Integer.parseInt(configLoader.getProperty("NUM_ATTEMPT"));
        this.THREAD_SLEEP = Integer.parseInt(configLoader.getProperty("THREAD_SLEEP"));
    }

    public void sendMail(String to, String toCC, String BCC, String caption, String body, String filePaths) {
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
            if (to != null && !to.isEmpty()) {
                message.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to));
            }
            if (toCC != null && !toCC.isEmpty()) {
                message.setRecipients(Message.RecipientType.CC, InternetAddress.parse(toCC));
            }
            if (BCC != null && !BCC.isEmpty()) {
                message.setRecipients(Message.RecipientType.BCC, InternetAddress.parse(BCC));
            }
            message.setSubject(caption);

            MimeMultipart multipart = new MimeMultipart();

            MimeBodyPart textPart = new MimeBodyPart();
            textPart.setContent(body, "text/html; charset=UTF-8");
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
            while (num_attempts >= 1) {
                if (sendMessageToMail(message)) {break;};
                num_attempts--;
                Thread.sleep(THREAD_SLEEP);
            }
            logger.info("Email " + message.getSubject() + " sent successfully to: " + to + " => Attempts left: " + --num_attempts);
        } catch (MessagingException e) {
            logger.error("Error sending email ", e);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static boolean sendMessageToMail(Message message) throws MessagingException {
        try {
            Transport.send(message);
            logger.info("Email sent successfully " + message.getSubject() + " " + Message.RecipientType.TO);
            return true;
        } catch (Exception ee) {
            ee.printStackTrace();
            logger.error("An error 'sendEmail' (To or ToCC)" + message.getSubject() + " To:" + Message.RecipientType.TO + " ToCC:" + Message.RecipientType.CC, ee);
            return false;
        }
    }
}
