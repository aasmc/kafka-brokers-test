package ru.test.kafkatest;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1")
public class TestController {

	private static final Logger log = LoggerFactory.getLogger(TestController.class);

	private static final long SEND_TIMEOUT_SECONDS = 10;

	private final KafkaTemplate<String, String> kafkaTemplate;

	private final TopicsProperties topics;

	public TestController(KafkaTemplate<String, String> kafkaTemplate, TopicsProperties topics) {
		this.kafkaTemplate = kafkaTemplate;
		this.topics = topics;
	}

	/**
	 * Sends a random message to the producer topic and waits for the broker ack, so that
	 * a broken producer is visible to the caller as a non-2xx response.
	 */
	@GetMapping("/test")
	public ResponseEntity<Map<String, Object>> test() {
		String key = UUID.randomUUID().toString();
		String value = "random-" + key + " at " + Instant.now();
		long started = System.nanoTime();
		try {
			SendResult<String, String> result = kafkaTemplate.send(topics.producer(), key, value)
				.get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
			long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
			var metadata = result.getRecordMetadata();
			log.info("PRODUCED topic={} partition={} offset={} tookMs={} key={}",
					metadata.topic(), metadata.partition(), metadata.offset(), tookMs, key);
			return ResponseEntity.ok(Map.of("status", "OK", "topic", metadata.topic(),
					"partition", metadata.partition(), "offset", metadata.offset(), "tookMs", tookMs));
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			return failure(key, started, ex);
		}
		catch (Exception ex) {
			return failure(key, started, ex);
		}
	}

	private ResponseEntity<Map<String, Object>> failure(String key, long started, Exception ex) {
		long tookMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
		Throwable cause = ex.getCause() != null ? ex.getCause() : ex;
		log.error("PRODUCE_FAILED topic={} tookMs={} key={} error={}: {}",
				topics.producer(), tookMs, key, cause.getClass().getName(), cause.getMessage());
		return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
			.body(Map.of("status", "FAILED", "error", cause.getClass().getSimpleName() + ": " + cause.getMessage(),
					"tookMs", tookMs));
	}

}
