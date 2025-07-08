package kvo.separat;


import java.io.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class SoapDownloadBinaryDV {

    public static void main(String[] args) throws JsonProcessingException {
        String jsonString = null;
        String currentDir = System.getProperty("user.dir");
        String configPath = currentDir + "\\config\\url.txt";
        InputStream is = null;
        // Проверяем и считываем файл
        try {
            is = new FileInputStream(configPath);
        } catch (FileNotFoundException e) {
            System.err.println("Файл не найден: " + e.getMessage());
            return; //  Прерываем выполнение, если файл не найден
        }
        // формируем строку json
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append("\n");
            }
            jsonString = sb.toString();
        } catch (IOException e) {
            System.err.println("Ошибка при чтении: " + e.getMessage());
        }

        // парсим json в обьект DocumentFile
        ObjectMapper mapper = new ObjectMapper();
        DocumentFile myObject = null;
        myObject = mapper.readValue(jsonString, DocumentFile.class);
        System.out.println("Десериализованный объект: " + myObject);

        // Пример доступа к данным:
//        System.out.println("UUID: " + myObject.getUuid());
//        System.out.println("URLs: " + myObject.getUrl());

        // Получение данных из Url:
        byte[] printBin; // = myObject.getUrl().get("Печатная форма1.pdf");
        System.out.println("Binary file выбран для : " + myObject.Url.keySet());

        //**************

        for (String key : myObject.Url.keySet()) {
            printBin = myObject.getUrl().get(key);
            //     saveFile(DatatypeConverter.parseBase64Binary(Arrays.toString(printBin)), "C:\\Users\\KvochkinAY\\Desktop\\tmp\\attach\\" + key);
            saveFile(printBin, "C:\\Users\\KvochkinAY\\Desktop\\tmp\\attach\\" + key);
        }
    }

    public static void saveFile(byte[] fileData, String filePath) {
        FileOutputStream fos = null;
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.createNewFile();
            }
            fos = new FileOutputStream(file);
            // Записываем данные в файл
            fos.write(fileData);
            System.out.println("Файл успешно сохранен: " + filePath);
        } catch (IOException e) {
            System.err.println("Ошибка при сохранении файла: " + e.getMessage());
        } finally {
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    System.err.println("Ошибка при закрытии потока: " + e.getMessage());
                }
            }
        }
    }

    public static class DocumentFile {
        @JsonProperty("uuid")
        private String uuid;
        @JsonProperty("typeMes")
        private String typeMes;
        @JsonProperty("To")
        private String To;
        @JsonProperty("ToCC")
        private String ToCC;
        @JsonProperty("Caption")
        private String Caption;
        @JsonProperty("Body")
        private String Body;
        @JsonProperty("Url")
        private Map<String, byte[]> Url;  // Map для хранения "Url"

        // Геттеры и сеттеры для всех полей
        public String getUuid() {
            return uuid;
        }

        public void setUuid(String uuid) {
            this.uuid = uuid;
        }

        public String getTypeMes() {
            return typeMes;
        }

        public void setTypeMes(String typeMes) {
            this.typeMes = typeMes;
        }

        public String getTo() {
            return To;
        }

        public void setTo(String to) {
            this.To = to;
        }

        public String getToCC() {
            return ToCC;
        }

        public void setToCC(String toCC) {
            this.ToCC = toCC;
        }

        public String getCaption() {
            return Caption;
        }

        public void setCaption(String caption) {
            this.Caption = caption;
        }

        public String getBody() {
            return Body;
        }

        public void setBody(String body) {
            this.Body = body;
        }

        public Map<String, byte[]> getUrl() {
            return Url;
        }

        public void setUrl(Map<String, byte[]> url) {
            this.Url = url;
        }

        @Override
        public String toString() { // Для удобства отладки
            return "MyJsonObject{" +
                    "uuid='" + uuid + '\'' +
                    ", typeMes='" + typeMes + '\'' +
                    ", to='" + To + '\'' +
                    ", toCC='" + ToCC + '\'' +
                    ", caption='" + Caption + '\'' +
                    ", body='" + Body + '\'' +
                    ", url=" + Url.toString() +
                    '}';
        }

    }
}