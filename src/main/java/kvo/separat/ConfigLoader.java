package kvo.separat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
public class ConfigLoader {
    private final Properties properties = new Properties();

    public ConfigLoader(String filePath) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            properties.load(reader);
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }
}