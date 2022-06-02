package org.npntraining.hands_on.consumer_api.consumer_groups;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.swing.*;
import java.util.Properties;

public class SimpleProducer {
    private static final String TOPIC_NAME = "test-consumer-group";

    public static void main(String[] args) {
        // Step 01 - Initialize Properties
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Step 02 : Instantiate KafkaProducer
        Producer<Integer, String> producer = new KafkaProducer<>(properties);
        String message = null;
        do {
            message = JOptionPane.showInputDialog("Enter message");
            int partition = Integer.parseInt(JOptionPane.showInputDialog("Enter partition"));
            if (!message.equals("quit")) {
                producer.send(new ProducerRecord<Integer, String>(TOPIC_NAME, partition, 1, message));
            }
        } while (!message.equals("quit"));

        producer.close();
    }
}
