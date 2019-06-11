package com.bettercloud.kadmin.services;

import com.bettercloud.kadmin.api.services.IntrospectionService;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

@Service
public class DefaultIntrospectionService implements IntrospectionService {
	
	private String defaultKafkaHost;
	
	@Autowired
	public DefaultIntrospectionService(@Value("${kafka.host:localhost:9092}") String defaultKafkaHost) {
		this.defaultKafkaHost = defaultKafkaHost;
	}
	
	@Override
	public Collection<String> getAllTopicNames(Optional<String> kafkaUrl) {
		Set<String> topics;
		
		Properties props = new Properties();
		props.put("bootstrap.servers", kafkaUrl.orElse(defaultKafkaHost));
		props.put("group.id", "kadmin-topic-listing-group");
		props.put("key.deserializer", StringDeserializer.class);
		props.put("value.deserializer", StringDeserializer.class);
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		topics = consumer.listTopics().keySet();
		consumer.close();
		return topics;
	}
}
