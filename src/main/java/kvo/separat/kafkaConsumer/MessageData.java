package kvo.separat.kafkaConsumer;

import java.util.UUID;

public class MessageData {
    private final Integer id;
    private final String to;
    private final String toCC;

    private final String BCC;
    private String caption;
    private final String body;
    private final UUID uuid;

    public MessageData(int id, String to, String toCC, String bCC, String caption, String body, /*JSONObject urls,*/ UUID uuid) {
        this.id = id;
        this.to = to;
        this.toCC = toCC;
        this.BCC = bCC;
        this.caption = caption;
        this.body = body;
        this.uuid = uuid;
    }

    public Integer getId() {
        return id;
    }

    public String getTo() {
        return to;
    }

    public String getToCC() {
        return toCC;
    }
    public String getBCC() {
        return BCC;
    }
    public String getCaption() {
        return caption;
    }

    public void setCaption(String caption) {
        this.caption = caption;
    }

    public String getBody() {
        return body;
    }

    public UUID getUuid() {
        return uuid;
    }

    public boolean has(String uuid) {
        return !uuid.isEmpty();
    }
}
