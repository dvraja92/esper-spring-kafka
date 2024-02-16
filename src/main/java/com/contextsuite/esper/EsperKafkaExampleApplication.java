package com.contextsuite.esper;

import io.swagger.v3.oas.annotations.OpenAPIDefinition;
import io.swagger.v3.oas.annotations.info.Info;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableConfigurationProperties
@SpringBootApplication
@EnableScheduling
@OpenAPIDefinition(info = @Info(title = "Esper Stream Analyzer API", version = "3.0", description = "Esper Stream Analyzer Information"))
public class EsperKafkaExampleApplication {

	public static void main(String[] args) {
		SpringApplication.run(EsperKafkaExampleApplication.class, args);
	}

}
