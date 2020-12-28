# Connect Schema ID Rewrite Converter


### Background on Confluent Replicator's use of converters

Confluent Replicator calls the following methods for a replicated message:

1. `toConnectData` of the converter specified by `src.value.converter` with the original topic name,
1. `fromConnectData` of the converter specified by `value.converter` with 'new' topic name (e.g., `topic.replica). 

### Design decisions

Since we need to register the schema to a subject on the destination cluster, the transformation logic is best placed into the `fromConnecData` method.
In this way, `toConnectData` becomes basically a no-op.

### Installation

The following two packages need to be compiled locally for the integration tests in the `src/intTest` folder to work:

* https://github.com/christophschubert/cp-testcontainers
* https://github.com/christophschubert/kafka-connect-java-client

### Known limitations

* the only supported `SubjectNamingStrategy` is `TopicNamingStrategy`


### Todos/features to consider
* how can other SubjectNamingStrategies be implemented


### Remarks

Just dropping a docker-compose file is not enough. We can produce and use AdminClient, but we cannot consume.
Should double-check with https://www.confluent.io/blog/kafka-listeners-explained/