package com.java.kafka;

import com.java.utils.CommonUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
kafka-topics --create --zookeeper zookeeper:2181 --partitions 5 --replication-factor 1 --topic SensorTopic
kafka-topics --zookeeper zookeeper:2181 --alter --topic SensorTopic --partitions 5
kafka-topics --list --zookeeper zookeeper:2181

**/

public class SensorProducer {

    // Create topic in Confluent Cloud
    public static void createTopic(final String topic,final Properties cloudConfig) {
        final NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.empty());
        try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(final String[] args) throws Exception {

        final Properties props = CommonUtils.loadConfig("/kafka_producer.properties");

        // Create topic if needed
        final String topic = "SensorTopic";
        createTopic(topic, props);

        // Add additional properties.
        //props.put(ProducerConfig.ACKS_CONFIG, "all");
        //props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        //props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        // Produce sample data
        final Long numMessages = 100L;
        for (Long i = 1L; i <= numMessages; i++) {

            String key = i.toString();
            String  record = new String("Event - " + i.toString());

            System.out.printf("Producing record: %s\t%s%n", key, record);
            producer.send(new ProducerRecord<String, String>(topic, key, record), new Callback() {
                @Override
                public void onCompletion(RecordMetadata m, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                    }
                }
            });
        }
        producer.flush();
        System.out.printf("100 messages were produced to topic %s%n", topic);
        producer.close();
    }

}
