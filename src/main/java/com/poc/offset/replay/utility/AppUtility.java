package com.poc.offset.replay.utility;

import com.poc.offset.replay.apis.Post_data_id_Publisher;
import com.poc.offset.replay.kafka.AdminClientHelper;
import com.poc.offset.replay.kafka.KafkaHelper;
import com.poc.offset.replay.runner.OffSetReplayRunner;
import org.apache.kafka.clients.admin.AdminClient;

import java.io.*;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

public class AppUtility {
    private static final String newLine = System.getProperty("line.separator");
    static InputStream inputStream;

    public static void writeToFile(String message) {
        String date = new SimpleDateFormat("yyyyMMddHH").format(new Date());
        String fileName = date + "_logs.txt";
        File file = new File(fileName);
        try (PrintWriter printWriter = new PrintWriter(new FileOutputStream(fileName,true))) {
            if (!file.exists()) file.createNewFile();
            printWriter.write(newLine + message);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Map<String, String> loadProperties() throws IOException {
        Map<String, String> topicGroups = new HashMap<>();
        try {
            Properties prop = new Properties();
            String propFileName = "application.properties";
            inputStream = OffSetReplayRunner.class.getClassLoader().getResourceAsStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }
            topicGroups.put("topic-1",prop.getProperty("topic-1"));
            topicGroups.put("topic-2",prop.getProperty("topic-2"));
            topicGroups.put("topic-3",prop.getProperty("topic-3"));
            topicGroups.put("topic-4",prop.getProperty("topic-4"));
            topicGroups.put("kafkaQA",prop.getProperty("kafkaQA"));
            topicGroups.put("kafkaStage",prop.getProperty("kafkaStage"));
            topicGroups.put("kafkaProd",prop.getProperty("kafkaProd"));
            topicGroups.put("QA",prop.getProperty("QA"));
            topicGroups.put("Stage",prop.getProperty("Stage"));
            topicGroups.put("Prod",prop.getProperty("Prod"));
            topicGroups.put("qaPath",prop.getProperty("qaPath"));
            topicGroups.put("stagePath",prop.getProperty("stagePath"));
            topicGroups.put("prodPath",prop.getProperty("prodPath"));
            topicGroups.put("qaJDBCURL",prop.getProperty("qaJDBCURL"));
            topicGroups.put("qaPassword",prop.getProperty("qaPassword"));
            topicGroups.put("qaUsername",prop.getProperty("qaUsername"));
            topicGroups.put("stageJDBCURL",prop.getProperty("stageJDBCURL"));
            topicGroups.put("stagePassword",prop.getProperty("stagePassword"));
            topicGroups.put("stageUsername",prop.getProperty("stageUsername"));
            topicGroups.put("prodJDBCURL",prop.getProperty("prodJDBCURL"));
            topicGroups.put("prodPassword",prop.getProperty("prodPassword"));
            topicGroups.put("prodUsername",prop.getProperty("prodUsername"));
        } catch (Exception e) {
            System.out.println("Exception: " + e);
        } finally {
            inputStream.close();
        }
        return topicGroups;
    }

    public static void commandLine() throws IOException {
        String env = null;
        String topicName = null;
        String partitions = null;
        String replicas = null;
        String check = null;
        String brokerConsumer = null;
        String brokerPublished = null;
        String consumerTopic = null;
        String publisherTopic = null;
        String startingOffset = null;
        String endingOffset = null;
        String decision = null;
        String topics = null;
        String topic = null;

        System.out.println("UTC: " + Instant.now() + " | Enter the option as required.....");
        System.out.println("----------------------------------------------------------------------------------------------------------------------");
        Scanner scanner = new Scanner(System.in);
        System.out.println("1 => Topic creation." + newLine +
                "2 => Message replication from Cross Env Topic to Topic(Single Partition supported)" + newLine +
                "3 => Data Publishing on Hermes." + newLine +
                "4 => Check Kafka Lag." + newLine +
                "5 => List all topics of Kafka." + newLine +
                "6 => Describe topics of Kafka" + newLine +
                "7 => Delete topic or topics from kafka" + newLine +
                "999 => Reset" + newLine +
                "0 => Exit");
        int value = scanner.nextInt();
        switch (value) {
            case 1: {
                System.out.println(" Enter the env for creating the topics....(QA,STAGE,PROD)");
                if (checkReset(env))
                    commandLine();
                else
                    env = scanner.next();
                System.out.println("You are using Env:" + env + ".Enter the Topic Name:");
                if (checkReset(topicName))
                    commandLine();
                else
                    topicName = scanner.next();
                System.out.println("Enter the number of partitions:");
                if (checkReset(partitions))
                    commandLine();
                else
                    partitions = scanner.next();
                System.out.println("Enter the number of replicas:");
                if (checkReset(replicas))
                    commandLine();
                else
                    replicas = scanner.next();
                System.out.println("If you still want to check if topic exist press y or n to continue creation");
                if (checkReset(check))
                    commandLine();
                else
                    check = scanner.next();
                if (check.equalsIgnoreCase("y")) {
                    try (AdminClient client = AdminClientHelper.getClient(env)) {
                        AdminClientHelper.showAllTopicsName(client);
                    }
                    System.out.println("Press y to continue creation or n to stop");
                    check = scanner.next();
                    if (check.equalsIgnoreCase("y")) {
                        try (AdminClient client = AdminClientHelper.getClient(env)) {
                            AdminClientHelper.createTopic(client,topicName,Integer.parseInt(partitions),Short.parseShort(replicas));
                        }
                    }
                } else if (check.equalsIgnoreCase("n")) {
                    try (AdminClient client = AdminClientHelper.getClient(env)) {
                        AdminClientHelper.createTopic(client,topicName,Integer.parseInt(partitions),Short.parseShort(replicas));
                    }
                }
                break;
            }
            case 2: {
                System.out.println("Enter the Consumer Broker ");
                if (checkReset(brokerConsumer))
                    commandLine();
                else
                    brokerConsumer = scanner.next();
                System.out.println("Enter the Publisher Broker ");
                if (checkReset(brokerPublished))
                    commandLine();
                else
                    brokerPublished = scanner.next();
                System.out.println("Enter the Consumer Topic ");
                if (checkReset(consumerTopic))
                    commandLine();
                else
                    consumerTopic = scanner.next();
                System.out.println("Enter the Publisher Topic ");
                if (checkReset(publisherTopic))
                    commandLine();
                else
                    publisherTopic = scanner.next();
                System.out.println("Enter the starting offset: ");
                if (checkReset(startingOffset))
                    commandLine();
                else
                    startingOffset = scanner.next();
                System.out.println("Enter the ending offset: ");
                if (checkReset(endingOffset))
                    commandLine();
                else
                    endingOffset = scanner.next();
                KafkaHelper.runConsumer(Long.parseLong(startingOffset),Long.parseLong(endingOffset),brokerConsumer,brokerPublished,consumerTopic,publisherTopic);
                break;
            }
            case 3: {
                System.out.println("Enter the Env (QA,STAGE,PROD) :");
                if (checkReset(env))
                    commandLine();
                else
                    env = scanner.next();
                System.out.println("Load from File System enter yes or no:");
                if (checkReset(decision))
                    commandLine();
                else
                    decision = scanner.next();
                Post_data_id_Publisher postdataidPublisher = new Post_data_id_Publisher();
                if (decision.equalsIgnoreCase("yes")) {
                    postdataidPublisher.bulkPublishHermes(env);
                } else if (decision.equalsIgnoreCase("no")) {
                    System.out.println("Enter the data file id's with separators as , and max limit is 5:");
                    String dataFileIdGroup = scanner.next();
                    postdataidPublisher.publishDataToHermes(dataFileIdGroup,env);
                }
                break;
            }
            case 4: {
                Map<String, String> map = AppUtility.loadProperties();
                System.out.println("Enter the env for checking....(QA,STAGE,PROD)");
                if (checkReset(env))
                    commandLine();
                else
                    env = scanner.next();
                System.out.println("Select the topic (1,2 and so on):");
                int count = 0;
                System.out.println("------------------------------------------------------------------------------------------------");
                System.out.println("topic-1,topic-2,topic-3,topic-4");
                System.out.println("------------------------------------------------------------------------------------------------");
                System.out.println("Enter the topic from above list: ");
                if (checkReset(topic))
                    commandLine();
                else
                    topic = scanner.next();
                String[] groupIds = map.get(topic).split(",");
                for (int i = 0; i < groupIds.length; i++) {
                    System.out.println("Selected topic: " + topic + " and group: " + groupIds[i] + " with Env: " + env);
                    KafkaHelper kafkaHelper = new KafkaHelper();
                    kafkaHelper.checkKafkaLag(env,topic,groupIds[i]);
                }
                break;
            }
            case 5: {
                System.out.println("Enter the env for checking....(QA,STAGE,PROD)");
                if (checkReset(env))
                    commandLine();
                else
                    env = scanner.next();
                try (AdminClient client = AdminClientHelper.getClient(env)) {
                    AdminClientHelper.showAllTopicsName(client);
                }
                break;
            }
            case 6: {
                System.out.println("Enter the env....(QA,STAGE,PROD)");
                if (checkReset(env))
                    commandLine();
                else
                    env = scanner.next();
                System.out.println("Enter the topics to describe separated.");
                if (checkReset(topics))
                    commandLine();
                else
                    topics = scanner.next();
                try (AdminClient client = AdminClientHelper.getClient(env)) {
                    AdminClientHelper.showTopicInfoByName(client,topics);
                }
                break;
            }
            case 7: {
                System.out.println("Enter the env....(QA,STAGE,PROD)");
                if (checkReset(env))
                    commandLine();
                else
                    env = scanner.next();
                System.out.println("Enter the topics to delete ',' separated.");
                if (checkReset(topics))
                    commandLine();
                else
                    topics = scanner.next();
                List<String> list = Arrays.asList(topics.split(","));
                try (AdminClient client = AdminClientHelper.getClient(env)) {
                    AdminClientHelper.deleteTopics(client,list);
                }
                break;
            }
            case 999: {
                commandLine();
                break;
            }
            case 0: {
                System.exit(0);
            }
            default: {
                System.out.println("Invalid Input.");
            }
        }
        commandLine();
    }

    static boolean checkReset(String input) {
        if (input != null && input.equalsIgnoreCase("0"))
            return true;
        else
            return false;
    }
}
