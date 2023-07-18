package org.ir.consumer;

import java.util.*;
import java.io.*;
import java.nio.file.*;

import org.apache.kafka.clients.consumer.*;


public class Consumer {

    final static String OFFSET_FILE_PREFIX = "./offsets/offset_";
    final static String CONSUMER_GROUP = "cg-1";
    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }

    public static void main(String[] args) throws Exception {

        final Properties props = loadConfig("client.properties");
        props.put("group.id", CONSUMER_GROUP);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        RebalanceListener rebalanceListner = new RebalanceListener(consumer);

        try {
            consumer.subscribe(Arrays.asList("test-topic"), rebalanceListner);
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                        System.out.println("Topic:"+ record.topic() +
                            " Partition:" + record.partition() +
                            " Offset:" + record.offset() + " Value:"+ record.value());
                    // Process record and save it to Database
                    Files.write(Paths.get(OFFSET_FILE_PREFIX + record.topic()+'_'+record.partition()+'_'+CONSUMER_GROUP),
                            Long.valueOf(record.offset() + 1).toString().getBytes());
                }
            }
        } catch (Exception ex) {
            System.out.println("Exception.");
            ex.printStackTrace();
        } finally {
            consumer.close();
        }
    }
}
