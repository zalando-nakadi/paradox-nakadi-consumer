# parardox-nakadi-consumer

[![Build Status](https://travis-ci.org/zalando-incubator/paradox-nakadi-consumer.svg?branch=master)](https://travis-ci.org/zalando-incubator/paradox-nakadi-consumer)
[![Maven Central](https://img.shields.io/maven-central/v/org.zalando.paradox/paradox-nakadi-consumer-core.svg)]()
[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/zalando-incubator/paradox-nakadi-consumer/master/LICENSE.txt)

Java high level [Nakadi](https://github.com/zalando/nakadi) consumer.


### What does the project already implement?

* [x] Automatic reconnect to Nakadi server
* [x] Consumer group partition offset management with Zookeeper
* [x] Leader election of the topic partition pro consumer group
* [x] Redistribution of partitions between consumer groups
* [x] Automatic failover between clients in consumer group
* [x] Automatic rebalance after new partition was added to Nakadi topic
* [x] OAuth2
* [x] Separate generic spring boot client
* [x] Handle failed events with SQS queue



### Project structure


    .
    ├── paradox-nakadi-consumer-core                        # Core implementation
    ├── paradox-nakadi-consumer-partitioned-zk              # Offset management with Zookeeper / Exhibitor
    ├── paradox-nakadi-consumer-boot                        # Spring Boot bindings
    ├── paradox-nakadi-consumer-example-boot                # Spring Boot usage example
    ├── paradox-nakadi-consumer-sqs-error-handler           # Failed event handling with Amazon SQS
    └── README.md


### Build the project and install locally

    gradlew clean
    gradlew build
    gradlew install


## Usage

### Maven dependencies

```xml
<dependency>
    <groupId>org.zalando.paradox</groupId>
    <artifactId>paradox-nakadi-consumer-boot</artifactId>
    <version>see above</version>
</dependency>
```

For error handling

```xml
<dependency>
    <groupId>org.zalando.paradox</groupId>
    <artifactId>paradox-nakadi-consumer-sqs-error-handler</artifactId>
    <version>see above</version>
</dependency>
```

### Choose your Partition Coordinator Provider  

    - simple     SimplePartitionCoordinator           # no persistence, no coordination
    - zk         ZKLeaderConsumerPartitionCoordinator # persistence in Zookeeper, leader election,
                                                        only one consumer from consumer group pro partition topic
    - zk-simple  ZKSimpleConsumerPartitionCoordinator # offset persistence in Zookeeper, no partition coordination

### application.yaml

Provide your unique `defaultConsumerName` e.g. ApplicationID.
Offset tracking and topic partition leader election is done pro consumer group name.

```yaml
paradox:
  nakadi:
    errorhandler:
      sqs:
        enabled: true
        queueName: test-queue-name
        region: eu-central-1
        createQueueIfNotExists: true
        messageVisibilityTimeout: 120
        messageRetentionPeriod: 1209600
    defaults:
      nakadiUrl: https://nakadi.example.com
      nakadiTokenId: nakadi-event-stream-read
      zookeeperBrokers: exhibitor.example.com:2181
      defaultConsumerName: yourConsumerGroupName
      oauth2Enabled : true
      partitionCoordinatorProvider: zk
      exhibitorAddresses: exhibitor.example.com
      exhibitorPort: 8181
      eventsStreamTimeoutSeconds: 900
      eventsBatchLimit: 1

    # batch settings can be overriden per consumer
    consumers:
      yourConsumerGroupName-handlerSuffix:
        eventsBatchLimit: 100
        eventsBatchTimeoutSeconds: 3

tokens:
  enableMock: false
  startAfterCreation: true
  accessTokenUri: https://token.example.com

  token-configuration-list:
    - tokenId:  nakadi-event-stream-read
      scopes: your scopes
```


### Implementation
#### Model your event

```java
@JsonIgnoreProperties(ignoreUnknown = true)
public class Metadata {

    @JsonProperty("eid")
    private UUID eid;

    @JsonProperty("occurred_at")
    private Date occurredAt;

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("received_at")
    private Date receivedAt;

    @JsonProperty("flow_id")
    private String flowId;
}

@JsonIgnoreProperties(ignoreUnknown = true)
public class OrderReceived {

    @JsonProperty("metadata")
    private Metadata metadata;

    @JsonProperty("order_number")
    private String orderNumber;
}

```

#### Add event handler

```java
@Component
@NakadiHandler(eventName = "order.ORDER_RECEIVED")
public class OrderReceivedHandler implements BatchEventsHandler<OrderReceived> {

    @Override
    public void onEvent(final EventTypeCursor cursor, final OrderReceived orderReceived) {
        // your code to handle the OrderReceived event
    }

    @Override
    public Class<OrderReceived> getEventClass() {
        return OrderReceived.class;
    }
}
```

### Other handlers

#### RawContentHandler

JSON payload with cursor and all events. The content is not interpreted and it can contain many events (determined by `eventsBatchLimit`).
Keep alive events are not filtered and in such a case content will have only cursor object and no events.

```java
@Configuration
public class Handlers {
       @Bean
       public RawContentHandler rawContentHandler() {
           return new RawContentHandler() {

               @Override
               @NakadiHandler(eventName = EVENT_NAME, consumerName = "test-raw-content-consumer")
               public void onEvent(final EventTypeCursor cursor, final String content) {
                 // your code to handle the raw content
               }
           };
       }
}
```

Posting one or more Events

```sh
curl -v -H 'Content-Type: application/json' -XPOST http://localhost:8080/event-types/order.ORDER_RECEIVED/events -d '[
  {
    "order_number": "24873243241",
    "metadata": {
      "eid": "d765de34-09c0-4bbb-8b1e-7160a33a0791",
      "occurred_at": "2016-03-15T23:47:15+01:00"
    }
  }, {
    "order_number": "24873243242",
    "metadata": {
      "eid": "a7671c51-49d1-48e6-bb03-b50dcf14f3d3",
      "occurred_at": "2016-03-15T23:47:16+01:00"
    }
  }]'

```

RawContentHandler.onEvent(cursor,content) can be invoked once with EventTypeCursor{name=order.ORDER_RECEIVED, partition=0, offset=1} and below content.
As content contains 2 events the next cursor with events would be EventTypeCursor{name=order.ORDER_RECEIVED, partition=0, offset=3}


```json
{
   "cursor":{
      "partition":"0",
      "offset":"1"
   },
   "events":[
      {
         "metadata":{
            "occurred_at":"2016-03-15T23:47:15+01:00",
            "eid":"d765de34-09c0-4bbb-8b1e-7160a33a0791",
            "event_type":"order.ORDER_RECEIVED",
            "partition":"0",
            "received_at":"2016-07-16T21:11:07.555Z",
            "flow_id":"Hj92NHC38oWJpXU2CiWyBqmR"
         },
         "order_number":"24873243241"
      },
      {
         "metadata":{
            "occurred_at":"2016-03-15T23:47:16+01:00",
            "eid":"a7671c51-49d1-48e6-bb03-b50dcf14f3d3",
            "event_type":"order.ORDER_RECEIVED",
            "partition":"0",
            "received_at":"2016-07-16T21:11:07.555Z",
            "flow_id":"Hj92NHC38oWJpXU2CiWyBqmR"
         },
         "order_number":"24873243242"
      }
   ]
}
```

RawContentHandler.onEvent(cursor,content) invoked with keep-alive content

```json
{
   "cursor":{
      "partition":"0",
      "offset":"1"
   }
}
```


#### RawEventHandler
JSON payload of the event. Keep alive events are filtered and the handler is not invoked

```java
@Component
@NakadiHandler(eventName = EVENT_NAME)
public static class MyRawEventHandler implements RawEventHandler {

    @Override
    public void onEvent(final EventTypeCursor cursor, final String content) {
      // your code to handle the raw event  
    }
}
```

Posting one or more Events

```sh
curl -v -H 'Content-Type: application/json' -XPOST http://localhost:8080/event-types/order.ORDER_RECEIVED/events -d '[
  {
    "order_number": "24873243241",
    "metadata": {
      "eid": "d765de34-09c0-4bbb-8b1e-7160a33a0791",
      "occurred_at": "2016-03-15T23:47:15+01:00"
    }
  }, {
    "order_number": "24873243242",
    "metadata": {
      "eid": "a7671c51-49d1-48e6-bb03-b50dcf14f3d3",
      "occurred_at": "2016-03-15T23:47:16+01:00"
    }
  }]'

```

First invocation with EventTypeCursor{name=order.ORDER_RECEIVED, partition=0, offset=5} and content


```json
 {
    "metadata":{
       "occurred_at":"2016-03-15T23:47:15+01:00",
       "eid":"d765de34-09c0-4bbb-8b1e-7160a33a0791",
       "event_type":"order.ORDER_RECEIVED",
       "partition":"0",
       "received_at":"2016-07-16T21:21:57.996Z",
       "flow_id":"QwiSkopv8V3L8AkgpE0cnAIh"
    },
    "order_number":"24873243241"
 }
```    


Second invocation with EventTypeCursor{name=order.ORDER_RECEIVED, partition=0, offset=6} and content

```json
{
   "metadata":{
      "occurred_at":"2016-03-15T23:47:16+01:00",
      "eid":"a7671c51-49d1-48e6-bb03-b50dcf14f3d3",
      "event_type":"order.ORDER_RECEIVED",
      "partition":"0",
      "received_at":"2016-07-16T21:21:57.996Z",
      "flow_id":"QwiSkopv8V3L8AkgpE0cnAIh"
   },
   "order_number":"24873243242"
}
```      

#### JsonEventHandler
Analog to RawEventHandler but provides JsonNode object


```java
@Component
@NakadiHandler(eventName = EVENT_NAME)
public static class MyJsonEventHandler implements JsonEventHandler {

    @Override
    public void onEvent(final EventTypeCursor cursor, final JsonNode jsonNode) {
      // your code to handle the jsonNode event
    }
}
```


### Bulk handlers BatchEventsBulkHandler, RawEventResponseBulkHandler and JsonEventResponseBulkHandler

JSON payload with cursor and list of events. The cursor represents the last element and it is passed
as commit object to the offset manager (only one commit is performed).
List length is determined by `eventsBatchLimit`.


```java
@Configuration
public class Handlers {
    @Bean
    public BatchEventsBulkHandler<OrderReceived> batchEventsBulkHandler() {
        return new BatchEventsBulkHandler<OrderReceived>() {

            @Override
            @NakadiHandler(eventName = EVENT_NAME)
            public void onEvent(EventTypeCursor cursor, List<OrderReceived> list) {
                // your code to handle the event list
            }

            @Override
            public Class<OrderReceived> getEventClass() {
                return OrderReceived.class;
            }
        };
    }
}
```


#### NakadiBatchEventsHandler, NakadiRawContentHandler, NakadiRawEventHandler, NakadiJsonEventHandler (bulk NakadiBatchEventsBulkHandler, NakadiRawEventBulkHandler, NakadiJsonEventBulkHandler)

One handler class for different events and consumer groups

```java

@Bean
public NakadiEventConsumers testNakadiEventConsumers() {
    return new NakadiEventConsumers(ImmutableSet.of(NakadiEventConsumer.of(EVENT_NAME_1, "test-multi1"),
                NakadiEventConsumer.of(EVENT_NAME_2, "test-multi2")));
}

@Bean
public RawContentHandler createHandlers(final NakadiEventConsumers testNakadiEventConsumers) {
    return new NakadiRawContentHandler() {

        @Override
        public NakadiEventConsumers getNakadiEventConsumers() {
            return testNakadiEventConsumers;
        }

        @Override
        public void onEvent(final EventTypeCursor cursor, final String content) {
            // your code
        }
    };
}
```

### Exception handling in handler

As single exception should not stop the whole stream processing, `java.lang.Exception` thrown in `onEvent` handler
is suppressed and logged only . If required, a method implementor should take care of replaying separate failed events from the stream.
If Zookeeper Partition Coordinator is used, `java.lang.Error` and any subclass of
`de.zalando.paradox.nakadi.consumer.core.exceptions.UnrecoverableException` will disconnect the the topic-partition processor
from the Nakadi server and resume from the last committed offset. 

There is also EventErrorHandler interface. Initially there is no implementation for this interface.

```java
@FunctionalInterface
public interface EventErrorHandler {

    /**
     * This callback method will be called when an exception occurred. The Exception can be many things such as a broken
     * event, unparsable event body, database connection timeout etc.
     *
     * @param  consumerName        Event consumer name
     * @param  t                   Thrown exception itself
     * @param  eventTypePartition  EventTypePartition contains eventType and partition information
     * @param  offset              Current offset
     * @param  rawEvent            Raw event body itself
     */
    void onError(String consumerName, Throwable t, EventTypePartition eventTypePartition, @Nullable String offset, String rawEvent);
}
```

Each project should specify what to do with the failed events. They can be logged to the database, log files, elastic search etc.

The example usage can be found below.

```java
@Service
public class LoggingEventErrorHandler implements EventErrorHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingEventErrorHandler.class);

    @Override
    public void onError(final String consumerName, final Throwable t, final EventTypePartition eventTypePartition, @Nullable final String offset,
            final String rawEvent) {
        LOGGER.error(
            "Failed Event // Consumer Name = [{}] , Event Partition = [{}] , Event Type = [{}] , Event Offset = [{}] , Raw Event = [{}] //",
            consumerName, eventTypePartition.getPartition(), eventTypePartition.getEventType(), offset, rawEvent, t);
    }
}
```

If the SQS error handler is enabled, it will store the failed events to the SQS queue automatically.

Here are the configuration explanations 

`paradox.nakadi.errorhandler.sqs.enabled`: If it is true, the handler is bound to context automatically.

`paradox.nakadi.errorhandler.sqs.queueName`: The place where the failed events are stored.

`paradox.nakadi.errorhandler.sqs.region`: See available AWS regions in [AWS Documentation](http://docs.aws.amazon.com/AWSJavaSDK/latest/javadoc/com/amazonaws/regions/Regions.html)

`paradox.nakadi.errorhandler.sqs.createQueueIfNotExists`: If the parameter is true, initially, it checks the queue name in amazon then creates it if it does not exist.

`paradox.nakadi.errorhandler.sqs.messageVisibilityTimeout`: See VisibilityTimeout parameter in [AWS Documentation](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SetQueueAttributes.html)

`paradox.nakadi.errorhandler.sqs.messageRetentionPeriod`: See MessageRetentionPeriod parameter in [AWS Documentation](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_SetQueueAttributes.html)
     
### Spring boot support endpoints

#### Stop and restart event receivers

```sh
curl -XPOST http://host:port/nakadi/event-receivers/stop   
curl -XPOST http://host:port/nakadi/event-receivers/restart
```

#### Replay and restore events
1. Get current event consumer handlers
  
    ```sh
        curl -X GET 'http://host:port/nakadi/event-handlers'
    ```    
    
2. Replay event from Nakadi
    * all consumer handlers receiving `order.ORDER_RECEIVED`
    
    ```sh
        curl -X POST 'http://host:port/nakadi/event-handlers/event_types/order.ORDER_RECEIVED/partitions/0/offsets/0/replays'
    ``` 
    
    * consumer `test-raw-event-consumer` receiving `order.ORDER_RECEIVED`
    
    ```sh
        curl -X POST 'http://host:port/nakadi/event-handlers/event_type/order.ORDER_RECEIVED/partitions/0/offsets/0/replays?consumer_name=test-raw-event-consumer&verbose=true'
    ``` 

3. Restore events from file


    ```sh
       curl --data-binary @0-16516140.json -H "Content-Type: application/json" -X POST 'http://localhost:8082/nakadi/event-handlers/event_types/order.ORDER_RECEIVED/partitions/0/restores'
    ```    



