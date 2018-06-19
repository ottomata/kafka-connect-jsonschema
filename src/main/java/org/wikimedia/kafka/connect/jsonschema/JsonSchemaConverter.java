package org.wikimedia.kafka.connect.jsonschema;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StringConverterConfig;


import java.net.URL;
import java.net.URI;

import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.github.fge.jsonschema.core.tree.SchemaTree;
import com.github.fge.jsonschema.core.util.ValueHolder;
import com.github.fge.jsonschema.core.load.SchemaLoader;


import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


public class JsonSchemaConverter extends JsonConverter {
    // TODO: save this a  a JsonPointer
    private String schemaURIField = JsonSchemaConverterConfig.SCHEMA_URI_FIELD_DEFAULT;
    private String schemaURIPrefix = JsonSchemaConverterConfig.SCHEMA_URI_PREFIX_DEFAULT;
    private int cacheSize = JsonSchemaConverterConfig.SCHEMAS_CACHE_SIZE_DEFAULT;

    private final JsonSerializer serializer = new JsonSerializer();
    private final JsonDeserializer deserializer = new JsonDeserializer();

    private final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();

//        final JsonNode jsonSchema = new ObjectMapper().readTree(new URL(schemaURI));
    private final SchemaLoader schemaLoader = new SchemaLoader();

    protected Cache<URI, Schema> toConnectSchemaCache;

    // TODO: Do we want to enable conversion from Connect Schema to connect 'JsonConverter'
    // custom envelop schema?
    //private Cache<Schema, ObjectNode> fromConnectSchemaCache;


//    @Override
//    public ConfigDef config() {
//        return new JsonSchemaConverterConfig.configDef();
//    }

    @Override
    public void configure(Map<String, ?> configs) {
        JsonSchemaConverterConfig config = new JsonSchemaConverterConfig(configs);
        schemaURIField  = config.schemaURIField();
        schemaURIPrefix = config.schemaURIPrefix();
        cacheSize       = config.schemaCacheSize();

        boolean isKey = config.type() == ConverterType.KEY;
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);

        toConnectSchemaCache = new SynchronizedCache<>(new LRUCache<URI, Schema>(cacheSize));
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Map<String, Object> conf = new HashMap<>(configs);
        conf.put(StringConverterConfig.TYPE_CONFIG, isKey ? ConverterType.KEY.getName() : ConverterType.VALUE.getName());
        configure(conf);
    }


//    /**
//     * Convert a Kafka Connect data object to a native object for serialization.
//     * @param topic the topic associated with the data
//     * @param schema the schema for the value
//     * @param value the value to convert
//     * @return the serialized value
//     */
//    public byte[] fromConnectData(String topic, Schema schema, Object value) {
//        return new byte['0'];
//    }

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
            throw new DataException("Converting byte[] to Kafka Connect data failed due to serialization error: ", e);
        }

        Schema connectSchema = null;
        Object connectValue = null;
        try {
            // TODO change this to call method on configured implemented interface type
            connectSchema = connectSchemaFromJsonValue(jsonValue);
            connectValue  = convertToConnect(connectSchema, jsonValue);
        }
        catch (Exception e) {
            throw new DataException("Caught Exception while converting to connect:\n" + connectSchema + "\n" + connectValue, e);
        }

        return new SchemaAndValue(connectSchema, connectValue);
    }

    // TODO:
    // getJsonSchema
    // getJsonSchemaVersion
    // getJsonSchemaName
    // infer additional properties not in schema??


    // TODO this is the implementable interface.  This should move to a URISchemaResolver class
    public Schema connectSchemaFromJsonValue(JsonNode jsonValue) throws java.net.URISyntaxException, IOException, com.github.fge.jsonschema.core.exceptions.ProcessingException {
        return asConnectSchema(getSchemaURI(jsonValue), getJsonSchemaVersion(jsonValue));
    }

    public Integer getJsonSchemaVersion(JsonNode jsonValue) throws java.net.URISyntaxException {
        String[] uriComponents = getSchemaURI(jsonValue).toString().split("/");
        return Integer.parseInt(uriComponents[uriComponents.length - 1]);
    }

    /**
     *
     * @param value
     *
     * @return
     */
    public URI getSchemaURI(JsonNode value) throws java.net.URISyntaxException {
        return new URI(schemaURIPrefix + value.at(schemaURIField).asText());
    }

    // TODO: better exception handling.
    public JsonNode getJsonSchema(URI schemaURI) throws java.net.URISyntaxException, IOException, com.github.fge.jsonschema.core.exceptions.ProcessingException {

        // TODO make this call a configurable plugin class
//        // Get schema URI from value.
//        String schemaURI= "/.../";
//
////        SchemaLoader loader = new SchemaLoader();
//
//
//
//        // curl schema URI and instantiate JsonSchema
////        final JsonNode jsonSchema = new ObjectMapper().readTree(new URL(schemaURI));
//        /// OR
////        SchemaTree tree = loader.get(new URI(schemaURI));
////        JsonNode treeNode = tree.getBaseNode()
//        // this is probably only useful for validating
////        JsonSchema jsonSchema = JsonSchemaFactory.byDefault().getJsonSchema(treeNode);
//
//
//
////        JsonSchema schema = factory.getJsonSchema(schemaURI);
//
//
////        val schemaGenerator = new SchemaGenerator()


        // Use SchemaLoader so we resolve any JsonRefs.
        SchemaTree schemaTree = schemaLoader.get(schemaURI);
        return schemaTree.getBaseNode();

//        JsonNode jsonSchema = new ObjectMapper().readTree(schemaURI.toURL());

    }



    public Schema asConnectSchema(URI schemaURI, Integer version) throws java.net.URISyntaxException, IOException, com.github.fge.jsonschema.core.exceptions.ProcessingException {
        Schema cachedConnectSchema = toConnectSchemaCache.get(schemaURI);
        if (cachedConnectSchema != null)
            return cachedConnectSchema;


        Schema connectSchema = asConnectSchema(getJsonSchema(schemaURI), version);


        // TODO: if schemaURI is versionless, should we expire?  Should we infer and set the version?
        // Should we just put an expiry time on items in the cache?
        toConnectSchemaCache.put(schemaURI, connectSchema);
        return connectSchema;
    }

    @Override
    public Schema asConnectSchema(JsonNode jsonSchema) {
        return asConnectSchema(jsonSchema, null, true, null);
    }

    public Schema asConnectSchema(JsonNode jsonSchema, Integer version) {
        return asConnectSchema(jsonSchema, null, true, version);
    }

    public Schema asConnectSchema(JsonNode jsonSchema, String fieldName, Boolean required, Integer version) {
        final String typeField = "type";
        final String itemsField = "items";
        final String propertiesField = "properties";
        final String requiredField = "required";
        final String titleField = "title";
        final String descriptionField = "description";
        final String defaultField = "default";


        if (jsonSchema.isNull())
            return null;


        if (fieldName == null && jsonSchema.hasNonNull(titleField)) {
            // TODO: make a sanitizeFieldName method?
            fieldName = jsonSchema.get(titleField).textValue().replace('/', '_');
        }

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


                // TODO: Do we need to handle things like patternProperites, anyOf, etc?
                // I hope not!

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
                    // If we have a JSONSChema list of 'required' field names,
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


//        JsonNode schemaVersionNode = jsonSchema.get(JsonSchema.SCHEMA_VERSION_FIELD_NAME);
//        if (schemaVersionNode != null && schemaVersionNode.isIntegralNumber()) {
//            builder.version(schemaVersionNode.intValue());
//        }

//        JsonNode schemaDefaultNode = jsonSchema.get(JsonSchema.SCHEMA_DEFAULT_FIELD_NAME);
//        if (schemaDefaultNode != null)
//            builder.defaultValue(convertToConnect(builder, schemaDefaultNode));

        return builder.build();

    }

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

    private static final HashMap<String, LogicalTypeConverter> TO_JSON_LOGICAL_CONVERTERS = new HashMap<>();
    static {
        TO_JSON_LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof BigDecimal))
                    throw new DataException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());
                return Decimal.fromLogical(schema, (BigDecimal) value);
            }
        });

        TO_JSON_LOGICAL_CONVERTERS.put(Date.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof java.util.Date))
                    throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
                return Date.fromLogical(schema, (java.util.Date) value);
            }
        });

        TO_JSON_LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof java.util.Date))
                    throw new DataException("Invalid type for Time, expected Date but was " + value.getClass());
                return Time.fromLogical(schema, (java.util.Date) value);
            }
        });

        TO_JSON_LOGICAL_CONVERTERS.put(Timestamp.LOGICAL_NAME, new LogicalTypeConverter() {
            @Override
            public Object convert(Schema schema, Object value) {
                if (!(value instanceof java.util.Date))
                    throw new DataException("Invalid type for Timestamp, expected Date but was " + value.getClass());
                return Timestamp.fromLogical(schema, (java.util.Date) value);
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



//    private static List<T> jsonNodeAsArray(JsonNode jsonList, Schema.Type type) {
//
//
//
//        for (JsonNode element : jsonList) {
//
//        }
//    }


}