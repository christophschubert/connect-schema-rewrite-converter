# Demo scenarios for the schema rewrite converter

These demos use docker compose, see `docker-compose.yaml`.

The docker environment created consists of
- source Kafka broker, listening on `localhost:9091`,  with schema registry listing on `http://localhost:8081`,
- destination Kafka broker, listening on `localhost:9092`, with schema registry listening on `http://localhost:8082` and Connect listening on port `http://localhost:8083`.

## Scenario 1

In this scenario we configure a replication from all topics.

Start the demo environment:
```shell
docker compose up -d
```
In order to show that schema ids are actually rewritten, we first register a schema in the destination cluster:
```shell
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"namespace\": \"clients.avro\", \"type\": \"record\",\"name\": \"Order\", \"fields\": [{\"name\": \"product_name\", \"type\": \"string\" }]}"}' \
  http://localhost:8082/subjects/orders-value/versions
```

Now produce data to a topic in the source cluster using the `kafka-avro-console-consumer`:
```shell
cat user-data.jsonl | kafka-avro-console-producer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --property value.schema="$(cat user.avsc)" --topic user
```
Check that a schema was registered in the source schema registry:
```shell
curl http://localhost:8081/subjects/user-value/versions/1
```
and that data was actually written to the topic:
```shell
kafka-avro-console-consumer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --topic user --from-beginning --max-messages 3
```

Start replicator with schema rewrite converter:
```shell
curl  -X POST -H "Content-Type: application/json" --data @replicator-scenario-1.json http://localhost:8083/connectors
```
Check that data is actually replicated using `kafka-avro-console-consumer` on the destination cluster:
```shell
kafka-avro-console-consumer --bootstrap-server localhost:9092 --property schema.registry.url=http://localhost:8082 --topic user.replica --from-beginning
```

Bonus: check that we have registered a schema automatically using `curl`:
```shell
curl http://localhost:8082/schemas
```

Bonus 2: use `kcat` (formerly known as `kafkacat`) to look at the binary data in the destination topic:
```shell
kafkacat -b localhost:9092 -C -t user.replica -e |  hexdump -C
```
Observe that the first byte is 0, followed by bytes 0, 0, 0, 2, which is the ID of the schema automatically registered by the converter.

Checking the topic in the source cluster, we see that the messages in the topic differ only by the schema ID:
```shell
kafkacat -b localhost:9091 -C -t user -e |  hexdump -C
```

### Teardown
Use
```shell
docker compose down -v
```
to delete the containers and volumes used in this scenario.

## Scenario 2

In this scenario, we will replicate four topics, one with Avro, ProtoBuf, and JSON schema each, and another one with plain JSON data (without using a schema registry).
We will then create a single replicator instance with the schema rewrite converter which will auto-register schemas for the topics involving the schema registry and just plainly copy over the data in the JSON topic.
We this, we will need to create a blacklist including the JSON topic.

We will write `user` data in into four different topics: `user.avro`, `user.protobuf`, `user.jsonsr`, and `user.raw`.

Start the demo environment:
```shell
docker compose up -d
```
As before, we start by registering an un-related schema in the destination schema registry:
```shell
curl -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema": "{\"namespace\": \"clients.avro\", \"type\": \"record\",\"name\": \"Order\", \"fields\": [{\"name\": \"product_name\", \"type\": \"string\" }]}"}' \
  http://localhost:8082/subjects/orders-value/versions
```

Produce data in Avro format:
```shell
cat user-data.jsonl | kafka-avro-console-producer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --property value.schema="$(cat user.avsc)" --topic user.avro
```

Produce data in Protobuf format:
```shell
cat user-data.jsonl | kafka-protobuf-console-producer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --property value.schema="$(cat user.proto)" --topic user.protobuf
```

Produce data using a JSON schema:
```shell
cat user-data.jsonl | kafka-json-schema-console-producer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --property value.schema="$(cat user.json_schema)" --topic user.jsonsr
```

Finally, produce 'raw' JSON encoded data (without using the schema registry):
```shell
cat user-data.jsonl | kafka-console-producer --bootstrap-server localhost:9091 --topic user.raw
```
Bonus: compare the serialized messages in the `user.jsonsr` and `user.raw` topics and observe that they mainly differ in the presence of the schema ID in front of every message:
```shell
kcat -b localhost:9091 -C -t user.jsonsr -e |  hexdump -C
kcat -b localhost:9091 -C -t user.raw -e |  hexdump -C
```

Let's have a look at the schemas registered by the various console producers:
```shell
curl http://localhost:8081/schemas | jq
```

Start the replicator:
```shell
curl  -X POST -H "Content-Type: application/json" --data @replicator-scenario-2.json http://localhost:8083/connectors
```
We configured the replicator to replicate all topics whose name starts with the prefix `user.` topic to a blacklist 

## Scenario 3

In this scenario, we will evolve the schema of the source topic and observe that the converter registers these schemas in the destination cluster: 

Start the demo environment:
```shell
docker compose up -d
```

Register un-related schema in destination schema registry:
```shell
curl -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d @order-wrapped.json \
  http://localhost:8082/subjects/orders-value/versions
```
Produce data using first version of the user schema (as above):
```shell
cat user-data.jsonl | kafka-avro-console-producer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --property value.schema="$(cat user.avsc)" --topic user
```
Start the replicator:
```shell
curl -H "Content-Type: application/json" -d @replicator-scenario-1.json http://localhost:8083/connectors
```
Produce a record with the schema version 2 to the source topic:
```shell
echo '{"user":"dave", "id": 15, "email":{"string":"david@example.com"}}' | kafka-avro-console-producer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --property value.schema="$(cat user-v2.avsc)" --topic user
```
Produce a record with schema version 3 to the source topic:
```shell
echo '{"user":"eduard", "id": 16, "email":{"string": "ed@example.com"}, "vip":true}' | kafka-avro-console-producer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --property value.schema="$(cat user-v3.avsc)" --topic user
```

Check that data is actually replicated using `kafka-avro-console-consumer` on the destination cluster:
```shell
kafka-avro-console-consumer --bootstrap-server localhost:9092 --property schema.registry.url=http://localhost:8082 --topic user.replica --from-beginning
```
## Scenario 4

In order to demonstrate the use of the schema rewrite converter with Confluent Cloud, first create a cluster together with API keys and secrets to access a topic `user` and a schema registry and expose them as environment variables:
```shell
export CCLOUD_BOOTSTRAP=...
export CCLOUD_KAFKA_API_KEY=...
export CCLOUD_KAFKA_API_SECRET=...
export CCLOUD_SR_URL=...
export CCLOUD_SR_API_KEY=...
export CCLOUD_SR_API_SECRET=...
```

Ensure that a topic called `user` has been created in Confluent Cloud and start producing Avro encoded data to it:
```shell
cat user-data.jsonl | kafka-avro-console-producer --bootstrap-server "$CCLOUD_BOOTSTRAP" --producer.config ccloud-producer-config --producer-property sasl.jaas.config="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$CCLOUD_KAFKA_API_KEY\" password=\"$CCLOUD_KAFKA_API_SECRET\";" --property schema.registry.url="$CCLOUD_SR_URL" --property schema.registry.basic.auth.credentials.source=USER_INFO --property schema.registry.basic.auth.user.info="$CCLOUD_SR_API_KEY:$CCLOUD_SR_API_SECRET" --property value.schema="$(cat user.avsc)" --topic user
```

In this scenario, we need to pre-create the destination topic and disable automatic topic config syncronization in the replicator config.
Otherwise, replicator would try to produce to the destination cluster using `min.insync.replicas` of 2 and thus could not produce data.
```shell
kafka-topics --create --topic user.replica --bootstrap-server localhost:9092
```
Edit the replicator config `replicator-scenario-4.json` to change the setting for `src.kafka.bootstrap.servers`, `src.kafka.sasl.jaas.config`, `value.converter.source.schema.registry.url`, and `value.converter.source.schema.registry.basic.auth.user.info` to match your Confluent Cloud cluster.

Start the replicator:
```shell
curl -H "Content-Type: application/json" -d @replicator-scenario-4.json http://localhost:8083/connectors
```
Finally, observe that data has been replicator to our local cluster:
```shell
kafka-avro-console-consumer --bootstrap-server localhost:9092 --property schema.registry.url=http://localhost:8082 --topic user.replica --from-beginning
```