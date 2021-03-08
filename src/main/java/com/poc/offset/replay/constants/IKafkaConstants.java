package com.poc.offset.replay.constants;

public interface IKafkaConstants {

	public static String KAFKA_BROKERS_PRODUCER = "localhost:9092";
	public static String KAFKA_BROKERS = "localhost:9092";

	public static Integer MESSAGE_COUNT = 1000;

	public static String CLIENT_ID = "offsetReplayer";

	public static String TOPIC_NAME = "gce-events";
	public static String TOPIC_NAME_PUBLISHED = "gce-events";

	public static String GROUP_ID_CONFIG = "consumerGroup10";

	public static Integer MAX_NO_MESSAGE_FOUND_COUNT = 100;

	public static String OFFSET_RESET_LATEST = "latest";

	public static String OFFSET_RESET_EARLIER = "earliest";

	public static Integer MAX_POLL_RECORDS = 50;
}
