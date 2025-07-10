package kvo.separat.kafkaSender;

import kvo.separat.kafkaSender.message.Message;
import org.json.JSONObject;
import org.json.JSONArray;
import java.util.Map;
import java.util.stream.Collectors;

public class MessageImpl implements Message {
    private String uuid;
    private String typeMes;
    private String to;
    private String toCC;
    private String caption;
    private String body;
    private Map<String, String> urlAttachments;

    public MessageImpl(String jsonString) {
        JSONObject json = new JSONObject(jsonString);
        this.uuid = json.getString("uuid");
        this.typeMes = json.getString("typeMes");
        this.to = json.getString("To");
        this.toCC = json.getString("ToCC");
        this.caption = json.getString("Caption");
        this.body = json.getString("Body");

        JSONObject urlJson = json.getJSONObject("Url");
        this.urlAttachments = urlJson.toMap().entrySet().stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().toString()
                ));
    }

    @Override
    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("uuid", uuid);
        json.put("typeMes", typeMes);
        json.put("To", to);
        json.put("ToCC", toCC);
        json.put("Caption", caption);
        json.put("Body", body);

        JSONObject urlJson = new JSONObject();
        urlAttachments.forEach(urlJson::put);
        json.put("Url", urlJson);

        return json;
    }
}
