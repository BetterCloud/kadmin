package com.bettercloud.kadmin;

import com.bettercloud.kadmin.api.kafka.KafkaProviderService;
import com.bettercloud.kadmin.kafka.ConsoleKafkaMessageHandler;
import com.bettercloud.kadmin.kafka.QueuedKafkaMessageHandler;
import com.bettercloud.logger.services.LogLevel;
import com.bettercloud.logger.services.Logger;
import com.bettercloud.logger.services.LoggerFactory;
import com.bettercloud.messaging.kafka.consume.ConsumerGroup;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KadminApplication implements CommandLineRunner {

	private static final Logger logger = LoggerFactory.getLogger(KadminApplication.class);

	public static void main(String[] args) {
		SpringApplication.run(KadminApplication.class, args);
	}

	@Autowired private ConsoleKafkaMessageHandler console;
	@Autowired private KafkaProviderService kps;


	@Override
	public void run(String... args) throws Exception {
//		ConsumerGroup<String, Object> consumer = kps.consumerService(console, "pi31415");
//		consumer.start();

//		final QueuedKafkaMessageHandler queue = new QueuedKafkaMessageHandler(100, 1 * 60 * 1000);
//		kps.consumerService(queue, "pi31415").start();
////
//		new Thread(() -> {
//			while (true) {
//				logger.log(LogLevel.INFO, "Queue Size: {}, Since 30s: {}", queue.count(), queue.count(System.currentTimeMillis() - 30_000));
//				try {
//					Thread.sleep(10_000L);
//				} catch (InterruptedException e) {
//					e.printStackTrace();
//				}
//			}
//		}).start();
	}
}
