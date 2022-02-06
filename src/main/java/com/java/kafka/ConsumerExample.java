package com.java.kafka;

import com.java.utils.CommonUtils;
import org.apache.kafka.clients.consumer.*;
// import io.confluent.examples.clients.cloud.model.DataRecord;
//import io.confluent.kafka.serializers.KafkaJsonDeserializerConfig;

import java.util.Arrays;
import java.util.Properties;

public class ConsumerExample {

    public static void main(final String[] args) throws Exception {

        final String topic = "test-partitioned-topic";


        // Follow these instructions to create this file: https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/java.html
        final Properties props = CommonUtils.loadConfig("/kafka_consumer.properties");

        // Add additional properties.
        //props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        //props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonDeserializer");
        //props.put(KafkaJsonDeserializerConfig.JSON_VALUE_TYPE, DataRecord.class);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "demo-consumer-1");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");


        final Consumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));

        Long total_count = 0L;

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    total_count += 1;
                    System.out.printf("Consumed record with key %s and value %s, and updated total count to %d%n", key, value, total_count);
                }
                consumer.commitAsync();
            }
        } finally {
            consumer.commitSync();
            consumer.close();
        }
    }

}
