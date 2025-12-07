package com.va.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

public class ConsumerKafka {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "va-consumer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

      
        final int runDurationsec=60;


        try(final Consumer<String , String> consumer = new KafkaConsumer<>(props)) {
        	consumer.subscribe(Collections.singletonList("testva"));
        	
        	Instant endTime= Instant.now().plusSeconds(runDurationsec);
        	int totalmsgs=0;
        	
        	System.out.println("Consumer started... Listening for configured seconds.\n");
        	
            while (Instant.now().isBefore(endTime)) {
            	ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                	totalmsgs++;
                    System.out.println("Received -> " + record.value());
                }
            }

            System.out.println("\nDuration seconds over. Closing consumer...");
            System.out.println("Consumed json msgs succssfully");

        } catch (Exception e) {
            System.err.println("Error in consumer: " + e.getMessage());
            e.printStackTrace();

        }
        System.out.println("Consumer closed successfully.");
    }
}
