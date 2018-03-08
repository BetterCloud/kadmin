# Kadmin - Kafka Producer/Consumer UI

## Purpose

Companies like Confluent provide command line utilities for producing/consumer Kafka
topics. However, these tools are extremely difficult to use when other technologies
are introduced, e.g. avro, or attempting to debug your application by attempting to
parse globs of unformatted json.

Depending on deployment strategies, developers and QA are not able to use these
tools in deployed environment for testing/debugging.

This service provides similar functionality with a more user friendly interface
wrapped in an easily deployable micro-service.

## Overview

Similar to the command line tools Kadmin can spin up consumers and producers on
the fly.

### Consumers

Consumers keep a queue of the latest `n` (default 100) messages from a topic. When a consumer spins up, it starts with the `auto.offset.reset=latest` configuration. After an
initialization poll, the consumer will seek to `max(po - n / pc, 0)` for each partition where
`po` is the offset for that partition and `pc` is the total number of partitions.
This allows the consumer to pull the latest messages from each partition.

### Producers
Producers behave the same as command line tools. Not that depending on your kafka/schema registry configurations, you might need to create topics and schemas manually.

# Installing and Running

Kadmin uses Kafka 0.9.&#42; and Confluent 3.&#42;. Follow the [confluent quickstart guide](http://docs.confluent.io/3.0.0/quickstart.html) to get your local environment setup.

Kadmin requires Java 1.8+.

## Configuring

See the [Configuration](#Configuration) section for a full list of configurations, descriptions, and default values.

## Running From Executable
0. You can find a prebuilt executable jar in the `/dist` directory.
0. You can run the jar using `java -jar shared-kafka-admin-micro-0.1.0.jar --spring.profiles.active=desired,Spring,profiles`
0. Access the application using http://localhost:8080/kadmin/ providing the correct port and path context.

**Example**
```
cd <base>
cd dist
cp ../application.properties .
java -jar shared-kafka-admin-micro-*.jar --spring.profiles.active=kadmin,local
```

## Run From Source
1. run `./gradlew bootRun` or use your cli/ide to execute `com.bettercloud.kadmin.Application#main`
2. Access the application using http://localhost:8080/kadmin/ or the provide the correct port and path context.

## Run From Docker Container
1. run docker compose file
``` 
docker-compose up
```

Take the following variables into consideration when running the container:
0. `ZOOKEEPER_HOST`: zookeeper host
0. `KAFKA_HOST`: kafka host
0. `SECURITY_PROTOCOL`: security protocol (SSL or PLAINTEXT)
0. `TRUST_STORE_LOCATION`: absolute location of the truststore jks file
0. `TRUST_STORE_PASSWORD`: truststore password
0. `KEY_STORE_LOCATION`: absolute location of the keystore jks file
0. `KEY_STORE_PASSWORD`: keystore password
0. `KEY_PASSWORD`: key password

## Building and Running From An Executable JAR
0. run `./gradlew clean build`
0. You can find the executable jar in `build/libs` e.g. `build/libs/shared-kafka-admin-micro-0.1.0.jar`
0. If you used `<base>/application.properties` then copy that file to the same directory as the executable jar.
0. You can run the jar using `java -jar shared-kafka-admin-micro-0.1.0.jar --spring.profiles.active=desired,Spring,profiles`
0. Access the application using http://localhost:8080/kadmin/ providing the correct port and path context.



**Example**
```
cd <base>
rm -rf dist
mkdir dist
./gradlew clean build
cp build/libs/shared-kafka-admin-micro-*.jar dist/
cp application.properties dist/
cd dist
java -jar shared-kafka-admin-micro-*.jar --spring.profiles.active=kadmin,local
```

# Usage

* **Home:** This page does nothing
* **Basic Producer:** Pushes messages onto topics using the message serializers built into kafka.
* **Avro Producer:** Pushes messages onto topics using the avro message serializer.
* **Consumer:** Consumes the last 100 messages from a topic, using the specified deserializer.
* **Manager:** Lists information about the currently connected producers and consumers.

## Consuming Messages

### Starting The Consumer

0. Depending on the configuration, specify the kafka/schema registry urls.
1. Select the topic
2. Select the deserializer
3. Click "Start Consumer"

![Consumer](/images/consumer.png?raw=true "Consumer")

### Viewing the Consumer

Once the consumer has been started, you have the following options.

* Change the refresh interval. This is not the kafka consumer poll interval, but how often the client polls the server.
* Refresh - Performs a manual poll to the server. This can be used while the poll interval is set to a fixed time or to manual.
* Truncate - Clears the queue and consumed message count.
* Close - Disposes of the Kafka Consumer, clearing all memory and releasing the connection to the kafka cluster.
* Permalink - This is a deeplink to a consumer with the same topic and deserializer.
* Page Title - contains the total number of messages consumed. This can be helpful when multiple consumers are open in multiple tabs.

![Consumer Tab](/images/consumerTab.png?raw=true "Consumer Tab")

### Viewing the Messages

* Each message is formatted as a JSON object.
* Errors during deserialization are reported in place of the corrupted message
* Each message container can be collapsed (NOTE: this does not play well with refresh intervals)
* There is a copy button at the top of each message that will copy the raw message payload to the clipboard. This is useful for producing the same or modified messages.

## Producing Messages

### Primitive Types

0. Depending on the configuration, specify the kafka/schema registry urls.
0. Select the topic from the drop down or enter the topic name in the text box
0. Specify the count i.e. number of messages to send e.g. 10000 for a load test.
0. Select the serializer
0. Optionally specify a message key.
0. Enter message payload
0. Click "Send"
0. Check the results and stats at the bottom of the page for success and production rate.

![Basic Producer](/images/basicProducer.png?raw=true "Basic Producer")

### Avro

0. Depending on the configuration, specify the kafka/schema registry urls.
0. Select the topic from the drop down or enter the topic name in the text box
0. Specify the count i.e. number of messages to send e.g. 10000 for a load test.
0. Select the schema key and version from the dropdowns or enter the raw schema
0. Optionally specify a message key.
0. Enter message payload
0. Click "Send"
0. Check the results and stats at the bottom of the page for success and production rate.

![Avro Producer](/images/avroProducer.png?raw=true "Avro Producer")

## Manager

Allows management of active connections to brokers. Displays information about
topics currently being consumed. Provides actions for each active consumer

* Deep link to consumer
* Dispose of consumer, releasing all memory and network resources.

Also, allows for clean disposal of producers.

![Manager](/images/manager.png?raw=true "Avro Producer")

# Configuration

## Config locations

- `<base>/application.properties` => External configuration (preferred)
- `<base>/src/resources/application.properties` => Class path configuration (built into executable)

## Kafka Consumers and Producers

The consumer and producer has reasonable defaults, but these can be overridden using configurations from http://kafka.apache.org/documentation.html#newconsumerconfigs.

## Kadmin Configs
Config | Description | Default | Possible Values
--- | --- | --- | ---
server.contextPath | The following config sets the spring context path. You will access the application at http://<host-and-port>/<context> e.g. http://localhost:8080/kadmin | `null` |  Any valid url path element e.g. `/kadmin`
server.port | The port that the tomcat service will listen on. | `8080` |  Any valid port
ff.producer.enabled | Toggles read only mode i.e. Kafka producers are disabled. You can use the following Spring profile or the raw config. | `false` | `true`, `false`
ff.customKafkaUrl.enabled | Allows custom urls to be used for Kafka and Service Registry for each producer and consumer. | `false` | `true`, `false`
kafka.host | If `ff.customKafkaUrl.enabled` is disabled then you need to configure the default endpoints using the following configs. `kafka.host` is a comma separated list of kafka brokers. | `localhost:9092` | Valid hosts separated by commas.
schema.registry.url | Allows custom urls to be used for Kafka and Service Registry for each producer and consumer. | `http://localhost:8081` |  Any valid host url


# Version History

Note that changes to the major version (i.e. the first number) represent possible breaking changes, and may require modifications in your code to migrate. Changes to the minor version (i.e. the second number) should represent non-breaking changes. The third number represents any very minor bugfix patches.

* **0.9.1** - Bug Fixes
  * Fixed #2 - REST String serialization issues when raw strings contain quotes.
  * Fixed producer error metric

* **0.9.0** - Initial public release.
  * Consumer
    * each consumers uses a UUID as consumer group id to avoid colliding with existing consumer groups.
    * allows Avro, String, Int, Long deserialization.
    * On initialization consumers perform topic rewind to retrieve the last 100 messages from a topic.
    * copy consumed messages to place back on the queue using a producer
  * Producer
    * allows Avro, String, Int, Long serialization.
    * load test by sending duplicate messages
  * Manager allows management of active connections to brokers.
  * Configuration for secure mode
    * only allow connecting to configured kafka hosts
    * read only mode
  * Configuration for custom context path when service is behind proxy.

# Development

Pull requests are welcomed for bugfixes or small enhancements. For more complex changes please open up an issue to discuss it first.

All code changes should include unit test and/or integration test coverage as appropriate. Unit tests are any that can be run in isolation, with no external dependencies. Integration tests are those which require kafka, zookeeper, and/or a schema registry instance
(at least a Dev instances) up and running.

Unit tests are located under the `src/test` directory, and can be run with the Grade `unitTest` task.

Integration tests are located under the `src/test-integration` directory, and can be run with the Gradle `integrationTest` task. See the additional `README.md` file in this directory for more detailed information on the kafka, zookeeper, schema registry, and avro required to run the integration test suite.

## Adding New Serializers

// coming soon...

Please contact david.esposito@bettercloud.com if you need this documentation sooner than later.

## Adding New Deserializers

// coming soon...

Please contact david.esposito@bettercloud.com if you need this documentation sooner than later.

# License

The MIT License (MIT)

Copyright (c) 2016 BetterCloud

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# Other Notes

Please contact david.esposito@bettercloud.com with any questions, or create an issue on Github with any bugs or feature requests.
