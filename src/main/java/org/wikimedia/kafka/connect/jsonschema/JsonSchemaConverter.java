package org.wikimedia.kafka.connect.jsonschema;


import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import com.github.fge.jsonschema.core.load.SchemaLoader;

import com.fasterxml.jackson.dataformat.yaml.YAMLParser;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.StringConverterConfig;



/**
 * Uses a schemaURI extracted from a JsonNode value to find the JSONSchema
 * for the JsonNode value.  This JSONSchema is then converted into a Connect Schema,
 * which is then used to convert the JsonNode value into a Connect value Java Object.
 *
 * This class extends from JsonConverter to take advantage of its implemented
 * fromConnectData() method(s).  This class copy/pastes the convertToConnect logic
 * from the parent JsonConverter, since those methods are private there.
 */
public class JsonSchemaConverter extends JsonConverter {
    /**
     * Used to extract the schemaURI from each JSON value.
     */
    private JsonPointer schemaURIPointer = JsonPointer.compile(
        JsonSchemaConverterConfig.SCHEMA_URI_FIELD_DEFAULT
    );

    /**
     * This will be prefixed to every URI extracted from each JSON value to
     * build a fully qualified URI.
     */
    private String schemaURIPrefix = JsonSchemaConverterConfig.SCHEMA_URI_PREFIX_DEFAULT;
    private String schemaURISuffix = JsonSchemaConverterConfig.SCHEMA_URI_SUFFIX_DEFAULT;


    /**
     * Pattern regex used to extract the schema version from the schema URI.
     */
    private Pattern schemaURIVersionPattern = Pattern.compile(
        JsonSchemaConverterConfig.SCHEMA_URI_VERSION_REGEX_DEFAULT
    );

    private int cacheSize = JsonSchemaConverterConfig.SCHEMAS_CACHE_SIZE_DEFAULT;

    private final JsonSerializer serializer = new JsonSerializer();
    private final JsonDeserializer deserializer = new JsonDeserializer();

    private final SchemaLoader schemaLoader = new SchemaLoader();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final YAMLFactory  yamlFactory  = new YAMLFactory();


    // This cache will be used to cache Scheams by schemaURIs.
    // The parent JsonConverter toConnectSchemaCache will not be used.
    // However, the parent JsonConverter fromConnectSchemaCache will be.
    private Cache<String, Schema> toConnectSchemaCache;

    // JSONSchema field names used to convert the JSONSchema to Connect Schema.
    protected static final String typeField          = "type";
    protected static final String itemsField         = "items";
    protected static final String propertiesField    = "properties";
    protected static final String requiredField      = "required";
    protected static final String titleField         = "title";
    protected static final String descriptionField   = "description";
    protected static final String defaultField       = "default";

    @Override
    public void configure(Map<String, ?> configs) {
        JsonSchemaConverterConfig config = new JsonSchemaConverterConfig(configs);
        schemaURIPointer        = JsonPointer.compile(config.schemaURIField());
        schemaURIPrefix         = config.schemaURIPrefix();
        schemaURISuffix         = config.schemaURISuffix();
        schemaURIVersionPattern = config.schemaURIVersionRegex();
        cacheSize               = config.schemaCacheSize();

        boolean isKey = config.type() == ConverterType.KEY;
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);

        toConnectSchemaCache = new SynchronizedCache<>(new LRUCache<String, Schema>(cacheSize));

        // Configure the parent JsonConverter so it can use
        // its fromConnectData() method.
        Map jsonConverterConfigs  = new HashMap<String, String>();
        jsonConverterConfigs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false");
        jsonConverterConfigs.put(JsonConverterConfig.SCHEMAS_CACHE_SIZE_CONFIG, cacheSize);
        jsonConverterConfigs.put(ConverterConfig.TYPE_CONFIG, config.type().getName());
        super.configure(jsonConverterConfigs);
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> conf = new HashMap<>(configs);
        conf.put(StringConverterConfig.TYPE_CONFIG, isKey ? ConverterType.KEY.getName() : ConverterType.VALUE.getName());
        configure(conf);
    }

    /**
     * Convert a native object to a Kafka Connect data object.
     * @param topic the topic associated with the data
     * @param value the value to convert
     * @return an object containing the {@link Schema} and the converted value
     */
    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        // Convert value to JsonNode.
        JsonNode jsonValue;
        try {
            jsonValue = deserializer.deserialize(topic, value);

            if (jsonValue == null) {
                return SchemaAndValue.NULL;
            }
        } catch (SerializationException e) {
            throw new DataException(
                "Converting byte[] to Kafka Connect data failed due to serialization error: ", e
            );
        }

        Schema connectSchema = null;
        Object connectValue = null;
        try {
            connectSchema = asConnectSchemaFromJsonValue(topic, jsonValue);
            connectValue  = convertToConnect(connectSchema, jsonValue);
            // TODO: do we want to (configurably) validate jsonValue
            // using JsonSchema with JsonSchemaFactory???
        }
        catch (Exception e) {
            throw new DataException(
                "Caught Exception while converting to connect:\n" +
                connectSchema + "\n" + connectValue,
                e
            );
        }

        return new SchemaAndValue(connectSchema, connectValue);
    }

    @Override
    /**
     * Converts the provided jsonSchema into a Connect Schema.
     * This assumes that the schema is versionless.
     */
    public Schema asConnectSchema(JsonNode jsonSchema) {
        return asConnectSchema(jsonSchema, null, true, null);
    }

    /**
     * Converts the provided jsonSchema into a Connect Schema with the given schema version.
     *
     * @param jsonSchema
     * @param version
     * @return
     */
    public Schema asConnectSchema(JsonNode jsonSchema, Integer version) {
        return asConnectSchema(jsonSchema, null, true, version);
    }

    /**
     * Converts the given jsonSchema with name fieldName and schema version to a Connect Schema.
     *
     * @param jsonSchema
     * @param fieldName
     * @param required
     * @param version
     * @return
     */
    public Schema asConnectSchema(
        JsonNode jsonSchema,
        String fieldName,
        Boolean required,
        Integer version
    ) {
        if (jsonSchema.isNull())
            return null;

        if (fieldName == null && jsonSchema.hasNonNull(titleField)) {
            // TODO: make a sanitizeFieldName method?
            fieldName = jsonSchema.get(titleField).textValue();
        }
        fieldName = sanitizeFieldName(fieldName);

        JsonNode schemaTypeNode = jsonSchema.get(typeField);
        if (schemaTypeNode == null || !schemaTypeNode.isTextual())
            throw new DataException("Schema must contain 'type' field");


        final SchemaBuilder builder;
        switch (schemaTypeNode.textValue()) {

            case "boolean":
                builder = SchemaBuilder.bool();
                if (jsonSchema.hasNonNull(defaultField))
                    builder.defaultValue(jsonSchema.get(defaultField).booleanValue());
                break;

            case "integer":
                builder = SchemaBuilder.int64();
                if (jsonSchema.hasNonNull(defaultField))
                    builder.defaultValue(jsonSchema.get(defaultField).longValue());
                break;

            case "number":
                builder = SchemaBuilder.float64();
                if (jsonSchema.hasNonNull(defaultField))
                    builder.defaultValue(jsonSchema.get(defaultField).doubleValue());
                break;

            case "string":
                builder = SchemaBuilder.string();
                if (jsonSchema.hasNonNull(defaultField))
                    builder.defaultValue(jsonSchema.get(defaultField).textValue());
                break;

            case "array":
                JsonNode itemsSchema = jsonSchema.get(itemsField);

                // Arrays must specify the type of their elements.
                if (itemsSchema == null || itemsSchema.isNull())
                    throw new DataException(fieldName + " array schema did not specify the items type");

                // Arrays must only use a single type, not tuple validation.
                if (!itemsSchema.isObject() || !itemsSchema.has("type"))
                    throw new DataException(fieldName + " array schema must specify the items type for field, e.g. \"items\": { \"type\": \"string\"");

                builder = SchemaBuilder.array(asConnectSchema(itemsSchema));
                if (jsonSchema.hasNonNull(defaultField))
                    builder.defaultValue(jsonSchema.get(defaultField).longValue());
                break;

            case "object":
                builder = SchemaBuilder.struct();
                if (fieldName != null) {
                    builder.name(fieldName);
                }

                JsonNode properties = jsonSchema.get(propertiesField);

                if (properties == null || !properties.isObject())
                    throw new DataException(fieldName + " struct schema's \"properties\" is not an object.");

                JsonNode requiredFieldList = jsonSchema.get(requiredField);
                if (requiredFieldList != null && !requiredFieldList.isArray()) {
                    throw new DataException(fieldName + " struct schema's \"required\" is not an array.");
                }

                Iterator<Map.Entry<String,JsonNode>> fields = properties.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> field = fields.next();
                    String subFieldName = field.getKey();

                    // TODO find a better way to do this than brute force checking the list every time?
                    // If we have a JSONSchema list of 'required' field names,
                    // check if any of these fields are required.  This is Draft 4 JSONSChema.
                    boolean subFieldRequired = arrayNodeContainsTextValue(
                        (ArrayNode)requiredFieldList, subFieldName
                    );
                    builder.field(
                        subFieldName,
                        asConnectSchema(field.getValue(), subFieldName, subFieldRequired, null)
                    );
                }
                break;

            default:
                throw new DataException("Unknown schema type: " + schemaTypeNode.textValue());
        }

        if (fieldName != null) {
            builder.name(fieldName);
        }

        if (version != null) {
            builder.version(version);
        }

        // Fields from JSON schema are default optional.
        if (required) {
            builder.required();
        }
        else {
            builder.optional();
        }

        if (jsonSchema.hasNonNull(descriptionField)) {
            builder.doc(jsonSchema.get(descriptionField).textValue());
        }

        // TODO: If 'additionalProperties' is present, should we infer the rest of the schema
        // from other fields in the data?
        // TODO Do we want to validate using factory?  Should this be configurable?


        return builder.build();

    }



    /**
     * Extracts the json value's JSONSchema URI from the schemaURIPointer json pointer.
     *
     * @param topic
     * @param value
     * @return
     * @throws DataException
     */
    public URI getSchemaURI(String topic, JsonNode value) throws DataException {
        try {
            return new URI(schemaURIPrefix + value.at(schemaURIPointer).textValue() + schemaURISuffix);
        }
        catch (java.net.URISyntaxException e) {
            throw new DataException("Could not extract JSONSchema URI in field " + schemaURIPointer + " json value with prefix " + schemaURIPrefix, e);
        }
    }

    /**
     * Given a schemaURI, this will request the JSON or YAML content at that URI and
     * parse it into a JsonNode.  $refs will be resolved.
     *
     * @param schemaURI
     * @return
     * @throws DataException
     */
    public JsonNode getJsonSchema(URI schemaURI) throws DataException {
        YAMLParser yamlParser = null;
        try {
            yamlParser = yamlFactory.createParser(schemaURI.toURL());
        }
        catch (IOException e) {
            throw new DataException("Failed parsing json schema returned from " + schemaURI, e);
        }

        // Use SchemaLoader so we resolve any JsonRefs in the JSONSchema.
        try {
            // TODO get fancy andy use URITranslator to resolve relative $refs?
            return schemaLoader.load(objectMapper.readTree(yamlParser)).getBaseNode();
        }
        catch (IOException e) {
            throw new DataException("Failed reading json schema returned from " + schemaURI, e);
        }
    }

    /**
     * Given the a jsonValue, this will request the JSONSchema referred to by the configured
     * schemaURI in the jsonValue and convert it to a Connect Schema.  If the schema version
     * is present in the schemaURI, the Connect Schema for the schemaURI will be cached.
     * If it is versionless, then it will not be cached.
     *
     * @param topic
     * @param jsonValue
     * @return
     * @throws IOException
     * @throws com.github.fge.jsonschema.core.exceptions.ProcessingException
     */
    public Schema asConnectSchemaFromJsonValue(String topic, JsonNode jsonValue) throws IOException, com.github.fge.jsonschema.core.exceptions.ProcessingException {
        URI schemaURI = getSchemaURI(topic, jsonValue);
        String schemaURIString = schemaURI.toString();

        Schema cachedConnectSchema = toConnectSchemaCache.get(schemaURIString);
        if (cachedConnectSchema != null)
            return cachedConnectSchema;

        Integer schemaVersion = getSchemaVersion(topic, jsonValue);

        Schema connectSchema = asConnectSchema(getJsonSchema(schemaURI), schemaVersion);

        // Only cache this schema if the schema has a version.
        if (schemaVersion != null) {
            toConnectSchemaCache.put(schemaURIString, connectSchema);
        }

        return connectSchema;
    }

    public Integer getSchemaVersion(String topic, JsonNode value){
        return getSchemaVersion(topic, getSchemaURI(topic, value));
    }


    /**
     * Extracts the schema version from the schemaURI using the schemaURIVersionRegex.
     *
     * @param topic
     * @param schemaURI
     * @return
     */
    public Integer getSchemaVersion(String topic, URI schemaURI) throws DataException {
        return getSchemaVersion(topic, schemaURI.toString());
    }

    /**
     * Extracts the schema version from the schemaURIString using the schemaURIVersionRegex.
     *
     * @param topic
     * @param schemaURIString
     * @return
     */
    public Integer getSchemaVersion(String topic, String schemaURIString) throws DataException {
        Matcher versionMatcher = schemaURIVersionPattern.matcher(schemaURIString);

        Integer version = null;

        // If we matched a schema version,
        // then extract it from the match and parse it as an Integer.
        if (versionMatcher.find()) {
            String versionString = versionMatcher.group("version");
            try {
                version = Integer.parseInt(versionString);
            }
            catch (NumberFormatException e) {
                throw new DataException("Failed parsing schema version " + versionString + " as an Integer.", e);
            }
        }

        return version;
    }


    /**
     * Given a Jackson ArrayNode, returns true if it contains the String value.
     *
     * @param list
     * @param value
     * @return
     */
    private static boolean arrayNodeContainsTextValue(ArrayNode list, String value) {
        if (list == null) {
            return false;
        }

        for (JsonNode element : list) {
            if (element.asText().equals(value)) {
                return true;
            }
        }

        return false;
    }


    /**
     * Replaces characters in fieldName that are not suitable for
     * field names with underscores.  I.e. [/.-\s]will be replaced.
     *
     * @param fieldName
     * @return
     */
    public String sanitizeFieldName(String fieldName) {
        if (fieldName == null)
            return fieldName;
        else {
            return fieldName.replaceAll("[/\\.\\s-]", "_");
        }
    }


    //
    // NOTE: Code below is copy/pasted from parent JsonConverter class, since these variables
    //       and methods are private there.
    //

    private static final Map<Schema.Type, JsonToConnectTypeConverter> TO_CONNECT_CONVERTERS = new EnumMap<>(Schema.Type.class);

    static {
        TO_CONNECT_CONVERTERS.put(Schema.Type.BOOLEAN, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return value.booleanValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT8, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return (byte) value.intValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT16, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return (short) value.intValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT32, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return value.intValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.INT64, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return value.longValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT32, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return value.floatValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.FLOAT64, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return value.doubleValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.BYTES, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                try {
                    return value.binaryValue();
                } catch (IOException e) {
                    throw new DataException("Invalid bytes field", e);
                }
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRING, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                return value.textValue();
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.ARRAY, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                Schema elemSchema = schema == null ? null : schema.valueSchema();
                ArrayList<Object> result = new ArrayList<>();
                for (JsonNode elem : value) {
                    result.add(convertToConnect(elemSchema, elem));
                }
                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.MAP, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                Schema keySchema = schema == null ? null : schema.keySchema();
                Schema valueSchema = schema == null ? null : schema.valueSchema();

                // If the map uses strings for keys, it should be encoded in the natural JSON format. If it uses other
                // primitive types or a complex type as a key, it will be encoded as a list of pairs. If we don't have a
                // schema, we default to encoding in a Map.
                Map<Object, Object> result = new HashMap<>();
                if (schema == null || keySchema.type() == Schema.Type.STRING) {
                    if (!value.isObject())
                        throw new DataException("Maps with string fields should be encoded as JSON objects, but found " + value.getNodeType());
                    Iterator<Map.Entry<String, JsonNode>> fieldIt = value.fields();
                    while (fieldIt.hasNext()) {
                        Map.Entry<String, JsonNode> entry = fieldIt.next();
                        result.put(entry.getKey(), convertToConnect(valueSchema, entry.getValue()));
                    }
                } else {
                    if (!value.isArray())
                        throw new DataException("Maps with non-string fields should be encoded as JSON array of tuples, but found " + value.getNodeType());
                    for (JsonNode entry : value) {
                        if (!entry.isArray())
                            throw new DataException("Found invalid map entry instead of array tuple: " + entry.getNodeType());
                        if (entry.size() != 2)
                            throw new DataException("Found invalid map entry, expected length 2 but found :" + entry.size());
                        result.put(convertToConnect(keySchema, entry.get(0)),
                                convertToConnect(valueSchema, entry.get(1)));
                    }
                }
                return result;
            }
        });
        TO_CONNECT_CONVERTERS.put(Schema.Type.STRUCT, new JsonToConnectTypeConverter() {
            @Override
            public Object convert(Schema schema, JsonNode value) {
                if (!value.isObject())
                    throw new DataException("Structs should be encoded as JSON objects, but found " + value.getNodeType());

                // We only have ISchema here but need Schema, so we need to materialize the actual schema. Using ISchema
                // avoids having to materialize the schema for non-Struct types but it cannot be avoided for Structs since
                // they require a schema to be provided at construction. However, the schema is only a SchemaBuilder during
                // translation of schemas to JSON; during the more common translation of data to JSON, the call to schema.schema()
                // just returns the schema Object and has no overhead.
                Struct result = new Struct(schema.schema());
                for (Field field : schema.fields())
                    result.put(field, convertToConnect(field.schema(), value.get(field.name())));

                return result;
            }
        });
    }

    // Convert values in Kafka Connect form into their logical types. These logical converters are discovered by logical type
    // names specified in the field
    private static final HashMap<String, LogicalTypeConverter> TO_CONNECT_LOGICAL_CONVERTERS = new HashMap<>();
    static {
        TO_CONNECT_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof byte[]))
                    throw new DataException("Invalid type for Decimal, underlying representation should be bytes but was " + value.getClass());
                return Decimal.toLogical(schema, (byte[]) value);
            }
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof Integer))
                    throw new DataException("Invalid type for Date, underlying representation should be int32 but was " + value.getClass());
                return Date.toLogical(schema, (int) value);
            }
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof Integer))
                    throw new DataException("Invalid type for Time, underlying representation should be int32 but was " + value.getClass());
                return Time.toLogical(schema, (int) value);
            }
        });

        TO_CONNECT_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof Long))
                    throw new DataException("Invalid type for Timestamp, underlying representation should be int64 but was " + value.getClass());
                return Timestamp.toLogical(schema, (long) value);
            }
        });
    }

    private static Object convertToConnect(Schema schema, JsonNode jsonValue) {
        final Schema.Type schemaType;
        if (schema != null) {
            schemaType = schema.type();
            if (jsonValue == null || jsonValue.isNull()) {
                if (schema.defaultValue() != null)
                    return schema.defaultValue(); // any logical type conversions should already have been applied
                if (schema.isOptional())
                    return null;
                throw new DataException("Invalid null value for required " + schemaType +  " field " + schema.name() + "\n" + schema.toString());
            }
        } else {
            switch (jsonValue.getNodeType()) {
                case NULL:
                    // Special case. With no schema
                    return null;
                case BOOLEAN:
                    schemaType = Schema.Type.BOOLEAN;
                    break;
                case NUMBER:
                    if (jsonValue.isIntegralNumber())
                        schemaType = Schema.Type.INT64;
                    else
                        schemaType = Schema.Type.FLOAT64;
                    break;
                case ARRAY:
                    schemaType = Schema.Type.ARRAY;
                    break;
                // All JSON objects need to be Structs.  JSON does not differentiate
                // between maps and objects.
                case OBJECT:
                    schemaType = Schema.Type.STRUCT;
                    break;
                case STRING:
                    schemaType = Schema.Type.STRING;
                    break;

                case BINARY:
                case MISSING:
                case POJO:
                default:
                    schemaType = null;
                    break;
            }
        }

        final JsonToConnectTypeConverter typeConverter = TO_CONNECT_CONVERTERS.get(schemaType);
        if (typeConverter == null)
            throw new DataException("Unknown schema type: " + String.valueOf(schemaType));

        Object converted = typeConverter.convert(schema, jsonValue);
        if (schema != null && schema.name() != null) {
            LogicalTypeConverter logicalConverter = TO_CONNECT_LOGICAL_CONVERTERS.get(schema.name());
            if (logicalConverter != null)
                converted = logicalConverter.convert(schema, converted);
        }
        return converted;
    }

    private interface JsonToConnectTypeConverter {
        Object convert(Schema schema, JsonNode value);
    }

    private interface LogicalTypeConverter {
        Object convert(Schema schema, Object value);
    }

}
