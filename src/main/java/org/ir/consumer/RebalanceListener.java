package org.ir.consumer;

import java.io.IOException;
import java.util.*;
import java.nio.file.*;
import java.nio.charset.Charset;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.*;

public class RebalanceListener implements ConsumerRebalanceListener {

    final static String OFFSET_FILE_PREFIX = "./offsets/offset_";
    final static String CONSUMER_GROUP = "cg1";
    private KafkaConsumer consumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap();

    public RebalanceListener(KafkaConsumer con) {
        this.consumer = con;
    }

    public void addOffset(String topic, int partition, long offset) {
        currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, "Commit"));
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }

    // Offsets are stored for topic-partition-consumerGroup Key

    public void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
        System.out.println("Following Partitions Assigned: ");
        for (TopicPartition tp : topicPartitions){
            System.out.println(tp.topic() + "," + tp.partition());
            try{
                // This portion of code to be replaced by call to an external DB / shared cache to fetch offsets for a given topic,partition,cg
                if (Files.exists(Paths.get(OFFSET_FILE_PREFIX + tp.partition()))) {
                    long offset = Long
                            .parseLong(Files.readAllLines(Paths.get(OFFSET_FILE_PREFIX + tp.topic()+'_'+tp.partition()+'_'+CONSUMER_GROUP),
                                    Charset.defaultCharset()).get(0));
                    consumer.seek(tp, offset);
                }
            } catch(IOException e) {
                System.out.printf("ERR: Could not read offset from file.\n");
            }
        }
    }

    public void onPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
        System.out.println("Following Partitions Revoked: ");
        for (TopicPartition tp : topicPartitions)
            System.out.println(tp.topic() + "," + tp.partition());


        System.out.println("Following Partitions commited: ");
        for (TopicPartition tp : currentOffsets.keySet()) {
            System.out.println(tp.topic() + "," + tp.partition());
            long offset =consumer.position(tp);
            // This portion of code to be replaced by call to an external DB / shared cache to store offsets for a given topic,partition,cg
            try {
                Files.write(Paths.get(OFFSET_FILE_PREFIX + tp.topic()+'_'+tp.partition()+'_'+CONSUMER_GROUP),
                        Long.valueOf(offset).toString().getBytes());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }


    }
}
