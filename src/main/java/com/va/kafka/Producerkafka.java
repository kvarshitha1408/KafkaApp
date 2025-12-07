package com.va.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;

import java.util.Properties;
import java.util.Random;

public class Producerkafka {

    private static final String[] NAMES = {"Avi", "Ravi", "Kiran", "Leo", "Mira", "Saanvi", "Arjun", "Kabir", "Zara"};
    private static final String[] GENDERS = {"Male", "Female", "Other"};
    private static final Logger LOGGER = LoggerFactory.getLogger(Producerkafka.class);

    public static void main(String[] args) {

        KafkaProducer<String, String> producer = null;
        Random random = new Random();

        try {
            Properties props = new Properties();
        	props.load(Producerkafka.class.getClassLoader().getResourceAsStream("producer.properties"));

            producer = new KafkaProducer<>(props);

            LOGGER.info("Sending random JSON events...");

            try {
                for (int i = 0; i < 3; i++) {

                    String name = NAMES[random.nextInt(NAMES.length)];
                    int age = 10 + random.nextInt(50); // 10–60
                    String gender = GENDERS[random.nextInt(GENDERS.length)];
                    long timestamp = System.currentTimeMillis();

                    // JSON payload
                    ObjectMapper mapper = new ObjectMapper();

                    Map<String, Object> data = new HashMap<>();
                    data.put("Name", name);
                    data.put("Age", age);
                    data.put("Gender", gender);
                    data.put("Timestamp", timestamp);

                    String json = mapper.writeValueAsString(data);
                                       
                    producer.send(new ProducerRecord<>("testva", name, json), (metadata, exception) -> {
                        if (exception == null) {
                        	LOGGER.info("Sent to topic " + metadata.topic() +
                                               ", partition " + metadata.partition() +
                                               ", offset " + metadata.offset());
                        } else {
                            exception.printStackTrace();
                        }
                    });

                    LOGGER.info("Produced: " + json);
                }
            } catch (Exception innerEx) {
            	LOGGER.error("Error while producing message: " + innerEx.getMessage());
                innerEx.printStackTrace();
            }

            producer.flush();
            LOGGER.info("All messages sent!");

        } catch (Exception e) {
        	LOGGER.error("Error initializing Kafka producer: " + e.getMessage());
            e.printStackTrace();

        } finally {
            if (producer != null) {
                producer.close();
            }
        }
    }
}
