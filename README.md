_THIS PROJECT IS WIP_

# Kafka Connect JSONSChema

Uses JSONSchema (Draft 4) to convert from JSON messages in Kafka to Kafka ConnectRecords.

Schemas are resolved from URIs expected to be in each of your JSON messages.
These schemas can be resolved over http://, or any protocol supported by java.net.URI.
This allows for integration between schemaed JSON data in Kafka with various
Kafka connectors.

# Usage

In connect.properties:

```
value.converter=org.wikimedia.kafka.connect.jsonschema.JsonSchemaConverter
value.converter.schema.uri.prefix=http://schema.service.org/v1/schemas/
value.converter.schema.uri.field=/meta/schema_uri

```

With this config, each Kafka messages value will look for a nested `meta.schema_uri` field,
append it to the `schema.uri.prefix` `http://schema.service.org/v1/schemas/`, and then
get the JSONSchema for the message.  The returned JSONSchema will be parsed
and converted into a ConnectSchema.  The value JSON data will be converted into
a Java connect value.

