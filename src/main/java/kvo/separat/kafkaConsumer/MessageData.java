package kvo.separat.kafkaConsumer;

import org.json.JSONObject;
import java.util.UUID;

public class MessageData {
    private final int id;
    private final String to;
    private final String toCC;
    private String caption;
    private final String body;
    private final JSONObject urls;
    private final UUID uuid;

    public MessageData(int id, String to, String toCC, String caption, String body, JSONObject urls, UUID uuid) {
        this.id = id;
        this.to = to;
        this.toCC = toCC;
        this.caption = caption;
        this.body = body;
        this.urls = urls;
        this.uuid = uuid;
    }

    public int getId() {
        return id;
    }

    public String getTo() {
        return to;
    }

    public String getToCC() {
        return toCC;
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

    public JSONObject getUrls() {
        return urls;
    }

    public UUID getUuid() {
        return uuid;
    }

    public boolean has(String uuid) {
        return !uuid.isEmpty();
    }
}
