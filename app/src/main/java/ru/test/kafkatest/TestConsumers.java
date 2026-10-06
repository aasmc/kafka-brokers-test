package ru.test.kafkatest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Два независимых консюмера, каждый в своей группе (и поэтому со своим координатором группы).
 * Каждое полученное сообщение логируется с маркером "CONSUMED", по нему анализатор разбирает логи эксперимента.
 */
@Component
public class TestConsumers {

	private static final Logger log = LoggerFactory.getLogger(TestConsumers.class);

	@KafkaListener(id = "consumer-1", groupId = "${app.groups.consumer1}", topics = "${app.topics.consumer}")
	public void consume(ConsumerRecord<String, String> record) {
		logRecord(record);
	}

	@KafkaListener(id = "consumer-2", groupId = "${app.groups.consumer2}", topics = "${app.topics.consumer2}")
	public void consume2(ConsumerRecord<String, String> record) {
		logRecord(record);
	}

	private static void logRecord(ConsumerRecord<String, String> record) {
		log.info("CONSUMED topic={} partition={} offset={} value={}",
				record.topic(), record.partition(), record.offset(), record.value());
	}

}
