# KafkaThing

**Disclaimer**

This repository is provided "AS-IS" with **no warranty or support** given. This is not an official or supported product/use case.

Download the extension package from

[https://github.com/migreenePTC/KafkaThing_Configurable/releases/download/1.0/KafkaExtension_Configurable.zip](https://github.com/migreenePTC/KafkaThing_Configurable/releases/download/1.0/KafkaExtension_Configurable.zip)

**Description**

An extension that allows you to send and receive messages from a Kafka server.

- It implements a basic producer and consumer.
- It exposes a test service via the KafkaThing Template.
- It uses the spring-kafka library and the Thingworx Java SDK.

This extension was tested to work with Thingworx 8.4.4.

### Configuration

If you don't have a Kafka server already running, you can easily create one with docker-compose. Here is the docker-compose.yml file that was used for development.

```
version: '2'
services:
  zookeeper:
    image: wurstmeister/zookeeper
    ports:
      - "2181:2181"
  kafka:
    build: .
    ports:
      - "9092"
    environment:
      KAFKA_ADVERTISED_HOST_NAME: 10.128.49.181
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

It is based on [kafka-docker](https://github.com/wurstmeister/kafka-docker). For the Kafka advertised hostname, when doing this type of setup specify the IP address of the host machine.

To view the status of the Kafka server, it may be useful to install [Kafka-Manager](https://github.com/yahoo/kafka-manager), this will allow you to create topics and verify that the server is alive. You must have at least one topic defined in order for the test to complete.

Once you have imported the extension into Thingworx, use the KafkaThing Template to create a Thing. On the configuration page, for the serverName, specify the hostname or IP address and the port for the Kafka server (not the Zookeeper).

Edit your connection setting directly in the Configuration table of the implemented Kafka Thing on your Thingworx enviornment.

No authentication is implemented in the current release.

```
    private String _serverName = "localhost:9092";
    private String _clientID = "client1";
    private String _topicName = "demo";
    private String _groupID = "consumerGroup1";
    private Integer _timeout = 100;

    //default config
    public String KAFKA_BROKERS = this._serverName;
    public static Integer MESSAGE_COUNT = 1000;
    public static String CLIENT_ID = "client1";
    public static String TOPIC_NAME = "demo";
```

### Services

Navigate to the Services section of the Thing and you can run:

- **runConnectivityTest**. Specify a topic name (that you have previously set up), then Execute. If a successful connection is made the result will be 'Test Complete', and it will only take 1-2 seconds to execute. In the Monitoring section of Thingworx, in the Application Log you should see something like:

```
received: ConsumerRecord(topic = topic1, partition = 0, leaderEpoch = 0, offset = 2, CreateTime = 1573806098140, serialized key size = 4, serialized value size = 3, headers = RecordHeaders(headers = [], isReadOnly = false), key = 0, value = baz)
```

- **sendMessage**. Specify a topic name, a message content(i.e. "Hello from TWX") and optionally a message key (integer). Execute to send the message to the server.
- **receiveMessages**. Specify a topic name, the datashape(download and import into Thingworx [kafkaConsumer](https://github.com/ptc-iot-sharing/KafkaThing/tree/master/twx) datashape), a maximum amount of messages to wait for before returning the infotable, and a consumer group name. This service returns an infotable with the messages received (up to when the max message count is reached). The infotable returned contains value, key, offset, and headers as STRING objects.

## Online Documentation

This README file only contains basic setup instructions. For more
comprehensive documentation, visit:

- [Basic Overview of Kafka](https://www.cloudkarafka.com/blog/2016-11-30-part1-kafka-for-beginners-what-is-apache-kafka.html)

* [Kafka Documentation](https://kafka.apache.org/documentation/)
* [Spring Kafka](https://docs.spring.io/spring-kafka/docs/current/reference/html/#introduction)
* [Kafka Clients](https://docs.spring.io/spring-kafka/docs/current/reference/html/#introduction)
