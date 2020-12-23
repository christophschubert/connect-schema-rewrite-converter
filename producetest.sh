curl  -X POST -H "Content-Type: application/json" --data @replicator_config.json http://localhost:8083/connectors

export my_schema='{"namespace": "clients.avro", "type": "record","name": "PositionValue", "fields": [{"name": "user", "type": "string" },{"name": "password", "type": "string" }]}'

kafka-avro-console-producer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --property value.schema="$my_schema" --topic data.position

{"user": "alice", "password": "geheim"}
{"user": "barnie", "password": "dontknow"}
{"user": "charlie", "password": "1337"}
{"user": "peter", "password": "secret"}

kafka-avro-console-consumer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --topic data.position --from-beginning


kafka-avro-console-producer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --property value.schema="$my_schema" --topic data.position


export my_schema2='{"namespace": "clients.avro", "type": "record","name": "PositionValue", "fields": [{"name": "user", "type": "string" },{"name": "g", "type":"string"},{"name": "password", "type": "string" }]}'
kafka-avro-console-producer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --property value.schema="$my_schema2" --topic another topic
{"user": "peter", "password": "secret", "g": "gg"}

export my_schema3='{"namespace": "clients.avro", "type": "record","name": "PositionValue", "fields": [{"name": "user", "type": "string" },{"name": "g", "type":"string"},{"name": "h", "type":"string"},{"name": "password", "type": "string" }]}'
kafka-avro-console-producer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --property value.schema="$my_schema3" --topic  data.tryout

{"user": "peter", "password": "secret", "g": "gg", "h": "jhksdfshfjsdhfjsh"}
