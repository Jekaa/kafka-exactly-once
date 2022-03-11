package com.learning.kafkaexactlyonce;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaExactlyOnceApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaExactlyOnceApplication.class, args);
	}

}
