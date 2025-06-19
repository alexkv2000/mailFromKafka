package kvo.separat.kafkaConsumer;

import org.json.JSONArray;

import java.util.UUID;

public class MessageData {
    private final int id;
    private final String to;
    private final String toCC;
    private String caption;
    private final String body;
    private final JSONArray urls;
    private final UUID uuid;

    public MessageData(int id, String to, String toCC, String caption, String body, JSONArray urls, UUID uuid) {
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

    public JSONArray getUrls() {
        return urls;
    }

    public UUID getUuid() {
        return uuid;
    }
}
