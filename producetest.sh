curl  -X POST -H "Content-Type: application/json" --data @replicator_config.json http://localhost:8083/connectors

export my_schema='{"namespace": "clients.avro", "type": "record","name": "PositionValue", "fields": [{"name": "user", "type": "string" },{"name": "password", "type": "string" }]}'

kafka-avro-console-producer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --property value.schema="$my_schema" --topic data.position

{"user": "alice", "password": "geheim"}
{"user": "barnie", "password": "dontknow"}
{"user": "charlie", "password": "1337"}
{"user": "peter", "password": "secret"}

kafka-avro-console-consumer --bootstrap-server localhost:9091 --property schema.registry.url=http://localhost:8081 --topic data.position --from-beginning
