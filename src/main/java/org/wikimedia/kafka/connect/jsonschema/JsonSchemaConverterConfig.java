package org.wikimedia.kafka.connect.jsonschema;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.storage.ConverterConfig;

import org.apache.kafka.connect.json.JsonConverterConfig;

import java.util.Map;

public class JsonSchemaConverterConfig extends ConverterConfig {
    public static  final String SCHEMA_URI_FIELD_CONFIG  = "schema.uri.field";
    public static  final String SCHEMA_URI_FIELD_DEFAULT = "/meta/schema_uri";
    private static final String SCHEMA_URI_FIELD_DOC     = "Dotted name of schema uri field in JSON message";
    private static final String SCHEMA_URI_FIELD_DISPLAY = "Schema URI Field Name";

    public static  final String SCHEMA_URI_PREFIX_CONFIG   = "schema.uri.prefix";
    public static  final String SCHEMA_URI_PREFIX_DEFAULT = "";
    private static final String SCHEMA_URI_PREFIX_DOC     = "Prefix added to every schema.uri.field";
    private static final String SCHEMA_URI_PREFIX_DISPLAY = "Schema URI Prefix";

    public static final String SCHEMAS_CACHE_SIZE_CONFIG = "schemas.cache.size";
    public static final int SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
    private static final String SCHEMAS_CACHE_SIZE_DOC = "The maximum number of schemas that can be cached in this converter instance.";
    private static final String SCHEMAS_CACHE_SIZE_DISPLAY = "Schema Cache Size";


    private final static ConfigDef CONFIG;

    static {
        String group = "Schemas";
        int orderInGroup = 0;
        CONFIG = ConverterConfig.newConfigDef();
        CONFIG.define(SCHEMA_URI_FIELD_CONFIG, Type.STRING, SCHEMA_URI_FIELD_DEFAULT,
            Importance.HIGH, SCHEMA_URI_FIELD_DOC, group,
            orderInGroup++, Width.MEDIUM, SCHEMA_URI_FIELD_DISPLAY
        );

        CONFIG.define(SCHEMA_URI_PREFIX_CONFIG, Type.STRING, SCHEMA_URI_PREFIX_DEFAULT,
            Importance.HIGH, SCHEMA_URI_PREFIX_DOC, group,
            orderInGroup++, Width.MEDIUM, SCHEMA_URI_PREFIX_DISPLAY
        );

        CONFIG.define(SCHEMAS_CACHE_SIZE_CONFIG, Type.INT, SCHEMAS_CACHE_SIZE_DEFAULT,
            Importance.HIGH, SCHEMAS_CACHE_SIZE_DOC, group,
            orderInGroup++, Width.MEDIUM, SCHEMAS_CACHE_SIZE_DISPLAY
        );
    }

    public static ConfigDef configDef() {
        return CONFIG;
    }

    public JsonSchemaConverterConfig(Map<String, ?> props) {
        super(CONFIG, props);
    }

    public String schemaURIField() {
        return getString(SCHEMA_URI_FIELD_CONFIG);
    }

    public String schemaURIPrefix() {
        return getString(SCHEMA_URI_PREFIX_CONFIG);
    }


    /**
     * Get the cache size.
     *
     * @return the cache size
     */
    public int schemaCacheSize() {
        return getInt(SCHEMAS_CACHE_SIZE_CONFIG);
    }
}