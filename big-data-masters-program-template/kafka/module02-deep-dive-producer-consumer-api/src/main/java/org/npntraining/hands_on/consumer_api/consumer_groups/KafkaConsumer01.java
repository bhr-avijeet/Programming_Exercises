package org.npntraining.hands_on.consumer_api.consumer_groups;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumer01 {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put("zookeeper.connect","localhost:2181");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class);
        properties.put("group.id","test");

        Consumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList("test-consumer-group"));

        while(true){
            ConsumerRecords<String,String> records = consumer.poll(1000);
            for(ConsumerRecord<String,String> record : records){
                System.out.println(record);
                System.out.println(record.value());
            }
        }


    }
}
