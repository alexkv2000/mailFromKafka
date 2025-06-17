package kvo.separat.kafkaSender.message;

import org.json.JSONArray;
import org.json.JSONObject;
import java.util.List;

//Реализация интерфейса Message
public class JsonMessage implements Message {
    private final String to;
    private final String toCC;
    private final String caption;
    private final String body;
    private final List<String> urls;

    public JsonMessage(String to, String toCC, String caption, String body, List<String> urls) {
        this.to = to;
        this.toCC = toCC;
        this.caption = caption;
        this.body = body;
        this.urls = urls;
    }

    @Override
    public JSONObject toJson() {
        JSONObject msg = new JSONObject();
        msg.put("To", to);
        if (toCC != null && !toCC.isEmpty()) {
            msg.put("ToСС", toCC);
        }
        msg.put("Caption", caption);
        msg.put("Body", body);
        JSONArray urlsArray = new JSONArray();
        if (urls != null) {
            for (String url : urls) {
                urlsArray.put(url);
            }
        }
        msg.put("Url", urlsArray);
        return msg;
    }
}

