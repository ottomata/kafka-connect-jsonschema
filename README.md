# Kafka Connect JSONSChema

Uses JSONSchema to convert from JSON messages in Kafka to Kafka ConnectRecords.

Schemas are resolved from URIs expected to be in each of your JSON messages.
These schemas can be resolved over http://, or any protocol supported by java.net.URI.
This allows for integration between schemaed JSON data in Kafka with various
Kafka connectors.

# Usage

In connect.properties:

```
# Connect record values will be converted from JSON by JsonSchemaConverter.
value.converter=org.wikimedia.kafka.connect.jsonschema.JsonSchemaConverter

# Each JSON record is expected to have this field set to its JSON Schema.
# This value would expect it to exist in the meta.schema_uri field, e.g.
#   { "meta": { "schema_uri": user/create/1 }, ... }
# Note the schema version at the end of the example schema_uri. This
# will be extracted by the default value of schema.uri.version.regex.
value.converter.schema.uri.field=/meta/schema_uri

# Prefix all extracted schema_uris with this value before attempting to
# request the JSONSchema.  This can be a remote http service, or a local
# file:// hierarchy.
value.converter.schema.uri.prefix=http://schema.service.org/v1/schemas/

# If using a local file hierarchy, it is likely that your files all end in some
# file format extension. If your schema_uris are extensionless, you could use
# this to append the extension before the JSONSchema will be requested.
#value.converter.schema.uri.suffix=".yaml"

```

With this config, each Kafka messages value will look for a nested `meta.schema_uri` field,
append it to the `schema.uri.prefix` `http://schema.service.org/v1/schemas/`, and then
get the JSONSchema for the message.  The returned JSONSchema will be parsed
and converted into a ConnectSchema.  The value JSON data will be converted into
a Java connect value.



# Configuration Options

| Name                       | Description                                                                                                                                                                              | Default                    |
|----------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------|
| `schema.uri.field`         | JsonPointer path to schema URI field in JSON record. This will be used to extract the JSONSchema URI for the JSON record.                                                                | `/meta/schema_uri`               |
| `schema.uri.prefix`        | Prefix added to every `schema.uri.field`. If your schema URIs are relative, you can use this to prefix them to build a fully qualified URI.                                              | `""`                             |
| `schema.uri.suffix`        | Suffix added to every `schema.uri.field`. If your schema URIs don't include a required suffix (e.g. a file extension), you can use this to append suffix to build a fully qualified URI. | `""`                             |
| `schema.uri.version.regex` | This regex is used to extract the schema version from the schema URI. There should be a named capture group for 'version'.                                                               | `([\w\-\./:@]+)/(?<version>\d+)` |
| `schemas.cache.size`       | The maximum number of schemas that can be cached in this converter instance.                                                                                                             | 1000                             |



NOTE: JsonSchemaConverter extends from Apache Kafka Connect JsonConverter in
order to leverage its implementation to convert from Connect records
back to JSON bytes.  It overrides methods that convert from JSON bytes
to Connect records in order to do so via JSONSchemas.
