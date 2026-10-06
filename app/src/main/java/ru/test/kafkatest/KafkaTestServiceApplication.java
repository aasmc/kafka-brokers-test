package ru.test.kafkatest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class KafkaTestServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaTestServiceApplication.class, args);
	}

}
