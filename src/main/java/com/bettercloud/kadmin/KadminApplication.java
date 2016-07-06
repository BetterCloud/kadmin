package com.bettercloud.kadmin;

import com.bettercloud.logger.services.Logger;
import com.bettercloud.logger.services.LoggerFactory;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class KadminApplication {

	private static final Logger logger = LoggerFactory.getLogger(KadminApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(KadminApplication.class, args);
	}

	@Bean(name = "defaultClient")
	public HttpClient defaultHttpClient() {
		return HttpClients.createDefault();
	}
}
