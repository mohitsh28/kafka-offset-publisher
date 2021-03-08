package com.poc.offset.replay.kafka;

import com.poc.offset.replay.constants.IKafkaConstants;
import com.poc.offset.replay.consumer.ConsumerCreator;
import com.poc.offset.replay.producer.ProducerCreator;
import com.poc.offset.replay.utility.AppUtility;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

public class KafkaHelper {

    private final String monitoringConsumerGroupID = "monitoring_consumer_" + UUID.randomUUID().toString();

    public static void runConsumer(long startingOffset,long endingOffset,String brokerConsumer,String brokerPublished,
                                   String consumerTopic,String publisherTopic) {
        Consumer<Long, String> consumer = ConsumerCreator.createConsumer(brokerConsumer,consumerTopic);
        Producer<String, String> producer = ProducerCreator.createProducer(brokerPublished,publisherTopic);
        int noMessageToFetch = 0;
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        for (TopicPartition tp : assignment) {
            consumer.seek(tp,startingOffset);
        }
        while (true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

            if (consumerRecords.count() == 0) {
                noMessageToFetch++;
                if (noMessageToFetch > IKafkaConstants.MAX_NO_MESSAGE_FOUND_COUNT)
                    break;
                else
                    continue;
            }
            consumerRecords.forEach(record -> {
                System.out.println("Consuming offset " + record.offset());
                if (record.offset() >= startingOffset && record.offset() < endingOffset) {
                    runProducer(record.offset(),record.value(),brokerPublished,publisherTopic,producer);
                }
            });
            consumer.commitAsync();
        }
        consumer.close();
    }

    public static void runProducer(long offset,String message,String brokerPublished,String publisherTopic,Producer<String, String> producer) {
        final ProducerRecord<String, String> record = new ProducerRecord<String, String>(publisherTopic,
                message);
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.println("Publishing to partition " + metadata.partition() + " with offset " + metadata.offset());
            AppUtility.writeToFile(offset + "," + message + "," + metadata.offset());
        } catch (ExecutionException e) {
            System.out.println(e);
        } catch (InterruptedException e) {
            System.out.println(e);
        }
    }

    public Map<TopicPartition, KafkaHelper.PartionOffsets> getConsumerGroupOffsets(String host,String topic,String groupId) {
        Map<TopicPartition, Long> logEndOffset = getLogEndOffset(topic,host);
        KafkaConsumer consumer = createNewConsumer(groupId,host);
        BinaryOperator<KafkaHelper.PartionOffsets> mergeFunction = (a,b) -> {
            throw new IllegalStateException();
        };
        Map<TopicPartition, KafkaHelper.PartionOffsets> result = null;
        System.out.println(Instant.now() + " => Getting Partion-Offsets results......");
        try {
            result = logEndOffset.entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            entry -> (entry.getKey()),
                            entry -> {
                                OffsetAndMetadata committed = consumer.committed(entry.getKey());
                                return new KafkaHelper.PartionOffsets(entry.getValue(),committed.offset(),entry.getKey().partition(),topic);
                            },mergeFunction));
        } catch (Exception e) {
            System.out.println(Instant.now() + " => Issue in fetching committed offsets.");
        }
        return result;
    }

    public Map<TopicPartition, Long> getLogEndOffset(String topic,String host) {
        System.out.println("Getting endOffsets......");
        Map<TopicPartition, Long> endOffsets = new ConcurrentHashMap<>();
        KafkaConsumer<?, ?> consumer = createNewConsumer(monitoringConsumerGroupID,host);
        List<PartitionInfo> partitionInfoList = consumer.partitionsFor(topic);
        List<TopicPartition> topicPartitions = partitionInfoList.stream().map(pi -> new TopicPartition(topic,pi.partition())).collect(Collectors.toList());
        consumer.assign(topicPartitions);
        consumer.seekToEnd(topicPartitions);
        topicPartitions.forEach(topicPartition -> endOffsets.put(topicPartition,consumer.position(topicPartition)));
        consumer.close();
        return endOffsets;
    }

    private KafkaConsumer<?, ?> createNewConsumer(String groupId,String host) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,host);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class);
        return new KafkaConsumer<>(properties);
    }

    public void checkKafkaLag(String env,String topic,String groupId) throws IOException {
        Map<String, String> properties = AppUtility.loadProperties();
        String kafkaBroker = null;
        switch (env.toUpperCase()) {
            case "QA": {
                kafkaBroker = properties.get("kafkaQA");
                break;
            }
            case "STAGE": {
                kafkaBroker = properties.get("kafkaStage");
                break;
            }
            case "PROD": {
                kafkaBroker = properties.get("kafkaProd");
                break;
            }
        }
        try {
            Map<TopicPartition, KafkaHelper.PartionOffsets> topicPartitionPartionOffsetsMap = getConsumerGroupOffsets(kafkaBroker,topic,groupId);
            for (Map.Entry<TopicPartition, KafkaHelper.PartionOffsets> entry : topicPartitionPartionOffsetsMap.entrySet()) {
                System.out.println(" => Topic: " + entry.getValue().topic + " with Partition:" + entry.getValue().partion);
                System.out.println(Instant.now() + " => Current offset: " + entry.getValue().currentOffset);
                System.out.println(Instant.now() + " => EndOffset: " + entry.getValue().endOffset);
                System.out.println("------------------------------------------------------------------------------------------------");
            }
        } catch (Exception e) {
            System.out.println(Instant.now() + " => Empty Values returned /Error in Groups /Error in access.");
        }

    }

    public static class PartionOffsets {
        private long endOffset;
        private long currentOffset;
        private int partion;
        private String topic;

        public PartionOffsets(long endOffset,long currentOffset,int partion,String topic) {
            this.endOffset = endOffset;
            this.currentOffset = currentOffset;
            this.partion = partion;
            this.topic = topic;
        }

        public long getEndOffset() {
            return endOffset;
        }

        public long getCurrentOffset() {
            return currentOffset;
        }

        public int getPartion() {
            return partion;
        }

        public String getTopic() {
            return topic;
        }
    }
}
