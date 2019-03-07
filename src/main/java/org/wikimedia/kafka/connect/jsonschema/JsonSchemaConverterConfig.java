package org.wikimedia.kafka.connect.jsonschema;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.storage.ConverterConfig;

import java.util.Map;
import java.util.regex.Pattern;

public class JsonSchemaConverterConfig extends ConverterConfig {
    public static  final String SCHEMA_URI_FIELD_CONFIG  = "schema.uri.field";
    public static  final String SCHEMA_URI_FIELD_DEFAULT = "/meta/schema_uri";
    private static final String SCHEMA_URI_FIELD_DOC     = "JsonPointer path to schema URI field in JSON value";
    private static final String SCHEMA_URI_FIELD_DISPLAY = "Schema URI Field Name";

    public static  final String SCHEMA_URI_PREFIX_CONFIG  = "schema.uri.prefix";
    public static  final String SCHEMA_URI_PREFIX_DEFAULT = "";
    private static final String SCHEMA_URI_PREFIX_DOC     = "Prefix added to every schema.uri.field";
    private static final String SCHEMA_URI_PREFIX_DISPLAY = "Schema URI Prefix";

    public static  final String SCHEMA_URI_SUFFIX_CONFIG  = "schema.uri.suffix";
    public static  final String SCHEMA_URI_SUFFIX_DEFAULT = "";
    private static final String SCHEMA_URI_SUFFIX_DOC     = "Suffix added to every schema.uri.field";
    private static final String SCHEMA_URI_SUFFIX_DISPLAY = "Schema URI Suffix";

    public static  final String SCHEMA_URI_VERSION_REGEX_CONFIG  = "schema.uri.version.regex";
    public static  final String SCHEMA_URI_VERSION_REGEX_DEFAULT = "([\\w\\-\\./:@]+)/(?<version>\\d+)";
    private static final String SCHEMA_URI_VERSION_REGEX_DOC     = "This regex is used to extract the schema version from the schema URI. There should be a named capture group for 'version'.";
    private static final String SCHEMA_URI_VERSION_REGEX_DISPLAY = "Schema URI Version Regex";

    public static  final String SCHEMAS_CACHE_SIZE_CONFIG  = "schemas.cache.size";
    public static  final int    SCHEMAS_CACHE_SIZE_DEFAULT = 1000;
    private static final String SCHEMAS_CACHE_SIZE_DOC     = "The maximum number of schemas that can be cached in this converter instance.";
    private static final String SCHEMAS_CACHE_SIZE_DISPLAY = "Schema Cache Size";

    public static  final String  SANITIZE_FIELD_NAMES_CONFIG  = "sanitize.field.names";
    public static  final boolean SANITIZE_FIELD_NAMES_DEFAULT = true;
    private static final String  SANITIZE_FIELD_NAMES_DOC     = "If true, bad field name characters (. / etc.) will be replaced with underscores.";
    private static final String  SANITIZE_FIELD_NAMES_DISPLAY = "Sanitize Field Names";

    public static  final String SCHEMA_URI_FALLBACK_CONFIG  = "schema.uri.fallback";
    public static  final String SCHEMA_URI_FALLBACK_DEFAULT = "";
    private static final String SCHEMA_URI_FALLBACK_DOC     = "Static schema URI to fallback to if missing from message";
    private static final String SCHEMA_URI_FALLBACK_DISPLAY = "Schema URI Fallback Value";

    // always false.  Here 'schemas.enable' refers to Kafka Connect's custom envelope
    // schema, which JsonSchemaConverter does not use.  However, JsonSchemaConverter
    // extends from JsonConverter in order to not reimplement some methods that work
    // as is if the Connect Schema is provided.
    private static final String SCHEMAS_ENABLE_CONFIG = "schemas.enable";

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

        CONFIG.define(SCHEMA_URI_SUFFIX_CONFIG, Type.STRING, SCHEMA_URI_SUFFIX_DEFAULT,
                Importance.HIGH, SCHEMA_URI_SUFFIX_DOC, group,
                orderInGroup++, Width.MEDIUM, SCHEMA_URI_SUFFIX_DISPLAY
        );

        CONFIG.define(SCHEMA_URI_VERSION_REGEX_CONFIG, Type.STRING, SCHEMA_URI_VERSION_REGEX_DEFAULT,
                Importance.HIGH, SCHEMA_URI_VERSION_REGEX_DOC, group,
                orderInGroup++, Width.MEDIUM, SCHEMA_URI_VERSION_REGEX_DISPLAY
        );

        // Hardcoded to false
        CONFIG.define(SANITIZE_FIELD_NAMES_CONFIG, Type.BOOLEAN, SANITIZE_FIELD_NAMES_DEFAULT,
            Importance.HIGH, SANITIZE_FIELD_NAMES_DOC, group,
            orderInGroup++, Width.MEDIUM, SANITIZE_FIELD_NAMES_DISPLAY
        );

        CONFIG.define(SCHEMAS_CACHE_SIZE_CONFIG, Type.INT, SCHEMAS_CACHE_SIZE_DEFAULT,
                Importance.HIGH, SCHEMAS_CACHE_SIZE_DOC, group,
                orderInGroup++, Width.MEDIUM, SCHEMAS_CACHE_SIZE_DISPLAY
        );

        CONFIG.define(SCHEMA_URI_FALLBACK_CONFIG, Type.STRING, SCHEMA_URI_FALLBACK_DEFAULT,
                Importance.HIGH, SCHEMA_URI_FALLBACK_DOC, group,
                orderInGroup++, Width.MEDIUM, SCHEMA_URI_FALLBACK_DISPLAY
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

    public String schemaURISuffix() {
        return getString(SCHEMA_URI_SUFFIX_CONFIG);
    }

    public Pattern schemaURIVersionRegex() {
        return Pattern.compile(getString(SCHEMA_URI_VERSION_REGEX_CONFIG));
    }

    public boolean shouldSanitizeFieldNames() {
        return getBoolean(SANITIZE_FIELD_NAMES_CONFIG);
    }

    public int schemaCacheSize() {
        return getInt(SCHEMAS_CACHE_SIZE_CONFIG);
    }

    public String schemaURIFallback() {
        return getString(SCHEMA_URI_FALLBACK_CONFIG);
    }
}
