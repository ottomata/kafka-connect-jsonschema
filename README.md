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



# Configuration Options

| Name                       | Description                                                                                                                                                                              | Default                    |
|----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------|
| `schema.uri.field`         | JsonPointer path to schema URI field in JSON record. This will be used to extract the JSONSchema URI for the JSON record.                                                                | `/meta/schema_uri`         |
| `schema.uri.prefix`        | Prefix added to every `schema.uri.field`. If your schema URIs are relative, you can use this to prefix them to build a fully qualified URI.                                              | ``                         |
| `schema.uri.suffix`        | Suffix added to every `schema.uri.field`. If your schema URIs don't include a required suffix (e.g. a file extension), you can use this to append suffix to build a fully qualified URI. | ``                         |
| `schema.uri.version.regex` | This regex is used to extract the schema version from the schema URI. There should be a named capture group for 'version'.                                                               | `([\w\\-\\./:@]+)/(?\\d+)` |
| `schemas.cache.size`       | The maximum number of schemas that can be cached in this converter instance.                                                                                                             | 1000                       |