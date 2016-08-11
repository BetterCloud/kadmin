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

Kadmin requires Java 8.

## Configuring
1. Set the desired port in `src/main/resources/application.properties` e.g. `server.port=9090` (defaults to `8080`)
2. Set the desired url path context in `src/main/resources/application.properties` e.g. `server.contextPath=/kadmin` (defaults to `/kadmin`)

## Run From Source
1. run `./gradlew bootRun`
2. Access the application using http://localhost:8080/kadmin/ or the provide the correct port and path context.

## Building and Running From An Executable JAR
1. run `./gradlew clean build`
2. You can find the executable jar in `build/libs` e.g. `build/libs/shared-kafka-admin-micro-0.1.0.jar`
3. You can run the jar using `java -jar shared-kafka-admin-micro-0.1.0.jar`
4. Access the application using http://localhost:8080/kadmin/ or the provide the correct port and path context.


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

### Viewing the Consumer

Once the consumer has been started, you have the following options.

* Change the refresh interval. This is not the kafka consumer poll interval, but how often the client polls the server.
* Refresh - Performs a manual poll to the server. This can be used while the poll interval is set to a fixed time or to manual.
* Truncate - Clears the queue and consumed message count.
* Close - Disposes of the Kafka Consumer, clearing all memory and releasing the connection to the kafka cluster.
* Permalink - This is a deeplink to a consumer with the same topic and deserializer.
* Page Title - contains the total number of messages consumed. This can be helpful when multiple consumers are open in multiple tabs.

### Viewing the Messages

* Each message is formatted as a JSON object.
* Errors during deserialization are reported in place of the corrupted message
* Each message container can be collapsed (NOTE: this does not play well with refresh intervals)
* There is a copy button at the top of each message that will copy the raw message payload to the clipboard. This is useful for producing the same or modified messages.

## Producing Messages

### Avro

0. Depending on the configuration, specify the kafka/schema registry urls.
1. Select the topic
2. Select the deserializer
3. Click "Start Consumer"

### Primitive Types

## Manager

# Configuration

Config | Description | Default | Possible Values
--- | --- | --- | ---
test | Useful for ... | `false` | `true`, `false`

# Version History

Note that changes to the major version (i.e. the first number) represent possible breaking changes, and may require modifications in your code to migrate. Changes to the minor version (i.e. the second number) should represent non-breaking changes. The third number represents any very minor bugfix patches.

* **1.0.0** - Initial public release.

# Development

Pull requests are welcomed for bugfixes or small enhancements. For more complex changes please open up an issue to discuss it first.

All code changes should include unit test and/or integration test coverage as appropriate. Unit tests are any that can be run in isolation, with no external dependencies. Integration tests are those which require kafka, zookeeper, and/or a schema registry instance
(at least a Dev instances) up and running.

Unit tests are located under the `src/test` directory, and can be run with the Grade `unitTest` task.

Integration tests are located under the `src/test-integration` directory, and can be run with the Gradle `integrationTest` task. See the additional `README.md` file in this directory for more detailed information on the kafka, zookeeper, schema registry, and avro required to run the integration test suite.

## Adding New Serializers

## Adding New Deserializers

# License

The MIT License (MIT)

Copyright (c) 2016 BetterCloud

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

# Other Notes
