package com.bettercloud.kadmin;

import com.bettercloud.environment.exception.LoadEnvironmentOverlayException;
import com.bettercloud.environment.util.EnvironmentPropertiesLoader;
import com.bettercloud.logger.services.LogLevel;
import com.bettercloud.logger.services.Logger;
import com.bettercloud.logger.services.LoggerFactory;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;

import java.util.concurrent.ConcurrentMap;

@EnableAutoConfiguration
@ComponentScan
@SpringBootApplication
public class KadminApplication {

	private static final Logger logger = LoggerFactory.getLogger(KadminApplication.class);

	public static final String SERVICE_NAME = "kadmin-micro";
	private static ConcurrentMap<String, String> environmentOverlayProperties;
	private static EnvironmentPropertiesLoader propertiesLoader = new EnvironmentPropertiesLoader();

	public static void main(String[] args) {
		ConcurrentMap<String, String> environmentOverlayProperties = getEnvironmentOverlayProperties();
		if (environmentOverlayProperties == null || environmentOverlayProperties.isEmpty()) {
			logger.log(LogLevel.ERROR, "No environment properties loaded. Stop application.");
			return;
		}

		SpringApplication.run(KadminApplication.class, args);
	}

	public static ConcurrentMap<String, String> getEnvironmentOverlayProperties() {
		try {
			synchronized (KadminApplication.class) {
				if (environmentOverlayProperties == null) {
					environmentOverlayProperties = propertiesLoader.loadEvironmentOverlays(SERVICE_NAME);
				}
			}
		} catch (LoadEnvironmentOverlayException e) {
			logger.log(LogLevel.ERROR, "Failed to load environment properties", e);
		}

		return environmentOverlayProperties;
	}

	/**
	 * Only used for unit testing, get will populate the properties for production code.
	 */
	public static void setEnvironmentOverlayProperties(ConcurrentMap<String, String> environmentOverlayProperties) {
		KadminApplication.environmentOverlayProperties = environmentOverlayProperties;
	}

	@Bean(name = "defaultClient")
	public HttpClient defaultHttpClient() {
		return HttpClients.createDefault();
	}
}
