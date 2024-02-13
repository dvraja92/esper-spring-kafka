package com.decipherzone.esper;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class EsperKafkaExampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(EsperKafkaExampleApplication.class, args);
	}

}
