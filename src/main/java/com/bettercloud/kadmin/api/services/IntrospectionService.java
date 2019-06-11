package com.bettercloud.kadmin.api.services;


import java.util.Collection;
import java.util.Optional;

public interface IntrospectionService {
	
	Collection<String> getAllTopicNames(Optional<String> kafkaUrl);
}
