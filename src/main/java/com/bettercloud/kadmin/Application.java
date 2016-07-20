package com.bettercloud.kadmin;

import ch.qos.logback.classic.Level;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.kafka.clients.NetworkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

@EnableAutoConfiguration
@ComponentScan
@SpringBootApplication
public class Application {

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Bean(name = "defaultClient")
	public HttpClient defaultHttpClient() {
		return HttpClients.createDefault();
	}

	@Bean
	public CommandLineRunner logConfigurationRunner() {
		return (args) -> {
			Logger kafkaClientLogger = LoggerFactory.getLogger(NetworkClient.class);
//			((ch.qos.logback.classic.Logger)kafkaClientLogger).setLevel(Level.OFF);
		};
	}
}
