package com.bettercloud.kadmin.services;

import com.bettercloud.kadmin.api.kafka.exception.KafkaOffsetException;
import com.bettercloud.kadmin.api.kafka.KafkaOffsetService;
import kafka.api.ConsumerMetadataRequest;
import kafka.cluster.Broker;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.ConsumerMetadataResponse;
import kafka.network.BlockingChannel;

import java.util.Optional;

/**
 * Created by davidesposito on 7/19/16.
 */
public class DefaultKafkaOffsetService implements KafkaOffsetService {



    @Override
    public long getOffset(Optional<String> oUrl, String consumerGroupId, String topic) throws KafkaOffsetException {
        BlockingChannel channel = new BlockingChannel("localhost", 9092,
                BlockingChannel.UseDefaultBufferSize(),
                BlockingChannel.UseDefaultBufferSize(),
                5000 /* read timeout in millis */);
        channel.connect();
        final String MY_GROUP = "demoGroup";
        final String MY_CLIENTID = "demoClientId";
        int correlationId = 0;
        final TopicAndPartition testPartition0 = new TopicAndPartition("demoTopic", 0);
        final TopicAndPartition testPartition1 = new TopicAndPartition("demoTopic", 1);
        channel.send(new ConsumerMetadataRequest(MY_GROUP, ConsumerMetadataRequest.CurrentVersion(), correlationId++, MY_CLIENTID));
        ConsumerMetadataResponse metadataResponse = ConsumerMetadataResponse.readFrom(channel.receive().buffer());

        if (metadataResponse.errorCode() == ErrorMapping.NoError()) {
            Broker offsetManager = metadataResponse.coordinator();
            // if the coordinator is different, from the above channel's host then reconnect
            channel.disconnect();
            channel = new BlockingChannel(offsetManager.host(), offsetManager.port(),
                    BlockingChannel.UseDefaultBufferSize(),
                    BlockingChannel.UseDefaultBufferSize(),
                    5000 /* read timeout in millis */);
            channel.connect();
        } else {
            // retry (after backoff)
        }
        return 0;
    }

    @Override
    public boolean setOffset(Optional<String> oUrl, String topic, long newOffset) throws KafkaOffsetException {
        return false;
    }
}
