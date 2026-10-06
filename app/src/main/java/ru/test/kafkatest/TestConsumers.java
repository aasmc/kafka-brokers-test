package ru.test.kafkatest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Two independent consumers, each in its own consumer group (and therefore with its own group coordinator).
 * Every consumed record is logged with a stable "CONSUMED" marker so the experiment logs can be parsed.
 */
@Component
public class TestConsumers {

	private static final Logger log = LoggerFactory.getLogger(TestConsumers.class);

	@KafkaListener(id = "consumer-1", groupId = "kafka-test-service-consumer-1", topics = "${app.topics.consumer}")
	public void consume(ConsumerRecord<String, String> record) {
		logRecord(record);
	}

	@KafkaListener(id = "consumer-2", groupId = "kafka-test-service-consumer-2", topics = "${app.topics.consumer2}")
	public void consume2(ConsumerRecord<String, String> record) {
		logRecord(record);
	}

	private static void logRecord(ConsumerRecord<String, String> record) {
		log.info("CONSUMED topic={} partition={} offset={} value={}",
				record.topic(), record.partition(), record.offset(), record.value());
	}

}
