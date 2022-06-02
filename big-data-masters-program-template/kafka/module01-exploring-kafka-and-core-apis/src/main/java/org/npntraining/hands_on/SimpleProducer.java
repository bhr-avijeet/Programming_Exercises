package org.npntraining.hands_on;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Scanner;

public class SimpleProducer {
    private static final String TOPIC_NAME = "simple-producer-consumer";

    public static void main(String[] args) {
        // Step 01 - Initialize Properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step 02 : Instantiate KafkaProducer
        Producer<Integer, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            String message = "This message is for key " + i;
            producer.send(new ProducerRecord<Integer, String>(TOPIC_NAME, i, message));
        }

        producer.close();
    }
}
