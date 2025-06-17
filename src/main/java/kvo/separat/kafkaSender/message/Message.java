package kvo.separat.kafkaSender.message;

import org.json.JSONArray;
import org.json.JSONObject;
import java.util.List;

//Интерфейс для сообщений
public interface Message {
    JSONObject toJson();
}
