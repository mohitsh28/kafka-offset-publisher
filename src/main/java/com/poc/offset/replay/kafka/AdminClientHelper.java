package com.poc.offset.replay.kafka;

import com.poc.offset.replay.utility.AppUtility;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;

import java.io.IOException;
import java.time.Instant;
import java.util.*;

public class AdminClientHelper {

    private static Map<String, String> properties;

    static {
        try {
            properties = AppUtility.loadProperties();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public AdminClientHelper() throws IOException {
    }

    public static AdminClient getClient(String env) {
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
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaBroker);
        return AdminClient.create(props);
    }

    public static void showAllTopicsName(AdminClient client) {
        System.out.println(Instant.now() + " => Scanning Kafka Broker for topics......");
        client.listTopics().names().thenApply(topics -> {
            Set<Map.Entry<String, KafkaFuture<TopicDescription>>> topicEntries = client.describeTopics(topics).values().entrySet();
            String topicName = "";
            System.out.println();
            for (Map.Entry<String, KafkaFuture<TopicDescription>> topicEntry : topicEntries) {
                topicName = topicEntry.getKey();
                System.out.println("topic name => " + topicName);
            }
            return topics;
        });
    }

    public static CreateTopicsResult createTopic(AdminClient client,String topicName,int partitions,short replicas) {
        return client.createTopics(Arrays.asList(new NewTopic(topicName,partitions,replicas)));
    }

    public static DeleteTopicsResult deleteTopics(AdminClient client,Collection<String> topics) {
        return client.deleteTopics(topics);
    }

    public static void showTopicInfoByName(AdminClient client,String topicName) {
        System.out.println(Instant.now() + " => fetching info from kafka........");
        client.describeTopics(Arrays.asList(topicName)).all().thenApply(AdminClientHelper::retrieveTopic);
    }

    private static TopicDescription retrieveTopic(Map<String, TopicDescription> value) {
        TopicDescription topicDesc = null;
        System.out.println();
        for (Map.Entry<String, TopicDescription> topicDescriptionEntry : value.entrySet()) {
            System.out.println("=> topic name - " + topicDescriptionEntry.getKey());
            topicDesc = topicDescriptionEntry.getValue();
            System.out.println("=> internal topic - " + topicDesc.isInternal());
            topicDesc.partitions().forEach(partition -> {
                StringBuilder isrStringBuilder = new StringBuilder();
                for (Node node : partition.isr()) {
                    isrStringBuilder.append(node.id()).append(" ");
                }
                StringBuilder replicaStringBuilder = new StringBuilder();
                for (Node node : partition.replicas()) {
                    replicaStringBuilder.append(node.id()).append(" ");
                }
                System.out.println("---------------------------------------------------------------------------------");
                System.out.println("partition: " + partition.partition());
                System.out.println("leader: " + partition.leader().id());
                System.out.println("replicas: " + replicaStringBuilder.toString());
            });
        }
        return topicDesc;
    }

}
