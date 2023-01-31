# Kafka Connect HTTP Sink Connector

This HTTP Sink connector was modified to support Carol Platform integration and the original author is [asaintsever](https://github.com/asaintsever/kafka-connect-http-sink). Al documentation regarging the base connector is available in the original repo.

## Added features

1. New connector property parameters carol.authorization and carol.connectorid.
2. New event formatters CarolMSSQLSERVEREventFormatter and CarolPGSQLEventFormatter

## Modified Behaviour

The connector now receives the Carol org URL as the HTTP endpoint: `https://myorgdomain.carol.ai` and will batch events of each topic subscribed using a hashmap, so multiple events can be stored in multiple topics in the batching process and after `event.batch.maxsize` is reached, it will send the messages from given topic to Carol intake API.

The connector will always call `https://myorgdomain.carol.ai/api/v3/staging/intake/CarolStaging?connectorId=myconnectorid`, being:

| Property | Description |
|---|---|
| myorgdomain | your Carol organization name |
| CarolStaging | the staging to send data to (more detail below) |
| myconnectorid | the connector id of the staging table |

The connector needs a Carol Token to be authenticated (carol.authorization property). The token can be obtained in the admin panel of the tenant you wish to send data.

Important: currently the staging table has to have the exact name of the topic, so the connector can build the correct URL to send data to. It will always format the topic name removing dots and using toLowerCase() function. Examples:

| Topic Name  | Carol Staging Table Name |
|---|---|
| postgres.public.Users | postgrespublicusers |
| some.topic | sometopic |
| ANOTHERTOPIC | anothertopic |

Sample configuration file can be found [here](/src/main/resources/connector_HttpSinkConnector_config.json). Use the release package on your Kafka Cluster.
Prefer using io.confluent.connect.avro.AvroConverter for message conversion on source connectors and this sink connector to ensure that the HTTP payload will satisfy Carol API intake format.