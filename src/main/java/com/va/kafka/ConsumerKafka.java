package com.va.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

public class ConsumerKafka {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerKafka.class);
    public static void main(String[] args) {

        Properties props = new Properties();
        try (InputStream input = ConsumerKafka.class.getClassLoader().getResourceAsStream("consumer.properties")) {
            if (input == null) {
                throw new RuntimeException("consumer.properties not found");
            }
            props.load(input);

        } catch (IOException e) {
            throw new RuntimeException("Failed to load consumer.properties", e);
        }
      
        int runDurationSec = Integer.parseInt(props.getProperty("run.duration.seconds", "60"));
        try(final Consumer<String , String> consumer = new KafkaConsumer<>(props)) {
        	consumer.subscribe(Collections.singletonList("testva"));
        	
        	Instant endTime= Instant.now().plusSeconds(runDurationSec);
        	int totalmsgs=0;
        	
        	LOGGER.info("Consumer started... Listening for configured seconds.\n");
        	
            while (Instant.now().isBefore(endTime)) {
            	ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                	totalmsgs++;
                    LOGGER.info("Received -> " + record.value());
                }
            }

            LOGGER.info("\nDuration seconds over. Closing consumer...");
            LOGGER.info("\nTotal messages consumed: " + totalmsgs);
            LOGGER.info("Consumed json msgs succssfully");

        } catch (Exception e) {
        	LOGGER.error("Error in consumer: " + e.getMessage());
            e.printStackTrace();

        }
        LOGGER.info("Consumer closed successfully.");
    }
}
