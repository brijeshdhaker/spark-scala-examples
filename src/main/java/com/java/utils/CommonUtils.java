package com.java.utils;

import com.java.kafka.ProducerExample;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class CommonUtils {

    public static Properties loadConfig(final String configFile) throws IOException {
        final Properties cfg = new Properties();
        InputStream inputStream = inputStream = ProducerExample.class.getResourceAsStream(configFile);
        cfg.load(inputStream);
        if (inputStream != null){
            inputStream.close();
        }
        return cfg;
    }

}
