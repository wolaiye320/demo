package com.jsg.util.client;

import java.util.Properties;

/**
 * @Auther: jiashaoguan
 * @Date: 2019/4/12
 * @Description:
 * @return
 */
public class KafkaUtil {
    private static String bootstrapServers = "192.168.136.131:9092";
    private static Properties properties;

    public static Properties getProperties(){
        properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        return properties;
    }
}
