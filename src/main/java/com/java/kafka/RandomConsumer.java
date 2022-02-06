package com.java.kafka;

import java.util.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

public class RandomConsumer{


    public static void main(String[] args) throws Exception{

        String topicName = "test-partitioned-topic";

        String groupName = "RG";
        Properties props = new Properties();
        props.put("bootstrap.servers", "kafka-broker:9092");
        props.put("group.id", groupName);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        RebalanceListner rebalanceListner = new RebalanceListner(consumer);

        consumer.subscribe(Arrays.asList(topicName),rebalanceListner);
        try{
            while (true){
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records){
                    //System.out.println("Topic:"+ record.topic() +" Partition:" + record.partition() + " Offset:" + record.offset() + " Value:"+ record.value());
                    // Do some processing and save it to Database
                    rebalanceListner.addOffset(record.topic(), record.partition(),record.offset());
                }
                //consumer.commitSync(rebalanceListner.getCurrentOffsets());
            }
        }catch(Exception ex){
            System.out.println("Exception while processing kafka" + ex.getMessage());
            ex.printStackTrace();
        }
        finally{
            consumer.close();
        }
    }

}
