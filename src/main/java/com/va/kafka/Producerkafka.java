package com.va.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class Producerkafka {

    private static final String[] NAMES = {"Avi", "Ravi", "Kiran", "Leo", "Mira", "Saanvi", "Arjun", "Kabir", "Zara"};
    private static final String[] GENDERS = {"Male", "Female", "Other"};

    public static void main(String[] args) {

        KafkaProducer<String, String> producer = null;
        Random random = new Random();

        try {
            Properties props = new Properties();
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            producer = new KafkaProducer<>(props);

            System.out.println("Sending random JSON events...");

            try {
                for (int i = 0; i < 3; i++) {

                    String name = NAMES[random.nextInt(NAMES.length)];
                    int age = 10 + random.nextInt(50); // 10–60
                    String gender = GENDERS[random.nextInt(GENDERS.length)];
                    long timestamp = System.currentTimeMillis();

                    // JSON payload
                    String json = "{"
                            + "\"Name\":\"" + name + "\","
                            + "\"Age\":\"" + age + "\","
                            + "\"Gender\":\"" + gender + "\","
                            + "\"Timestamp\":\"" + timestamp + "\""
                            + "}";

                    
                    producer.send(new ProducerRecord<>("testva", json));
                    System.out.println("Produced: " + json);
                }
            } catch (Exception innerEx) {
                System.err.println("Error during message production: " + innerEx.getMessage());
                innerEx.printStackTrace();
            }

            producer.flush();
            System.out.println("All messages sent!");

        } catch (Exception e) {
            System.err.println("Error initializing Kafka producer: " + e.getMessage());
            e.printStackTrace();

        } finally {
            if (producer != null) {
                producer.close();
            }
        }
    }
}
