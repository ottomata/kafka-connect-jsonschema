package org.wikimedia.kafka.connect.jsonschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.kafka.connect.data.*;

import java.nio.file.Files;
import java.io.File;
import java.io.IOException;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

public class JsonSchemaConverterTest {

    private static final String TOPIC = "topic";

    JsonSchemaConverter converter = new JsonSchemaConverter();
    JsonSchemaConverter unsanitizedConverter = new JsonSchemaConverter();


    File resourcesDirectory = new File("src/test/resources");
    String schemaURIPrefix = "file://" + resourcesDirectory.getAbsolutePath();

    File schemaFileJson = new File("src/test/resources/schema.json");
    File recordFileWithJsonSchema = new File("src/test/resources/record.json");

    File schemaFileYaml = new File("src/test/resources/schema.yaml");
    // The record in this file has its schema_uri set to a .yaml file.
    File recordFileWithYamlSchema = new File("src/test/resources/record_with_schema_yaml.json");

    File recordFileWithoutSchema = new File("src/test/resources/record_without_schema.json");

    byte[]   recordBytes;
    JsonNode recordWithJsonSchema;
    JsonNode recordWithYamlSchema;
    JsonNode recordWithoutSchema;
    JsonNode schemaJson;
    JsonNode schemaYaml;

    static Schema expectedSchema;
    static Struct expectedValue;

    static Schema expectedUnsanitizedSchema;
    static Struct expectedUnsanitizedValue;

    static {
        Schema complexListElementSchema = SchemaBuilder.struct().field("elem1", SchemaBuilder.string().name("elem1").optional());

        expectedSchema = SchemaBuilder.struct()
            .name("mediawiki_revision_create")
            .field("meta", SchemaBuilder.struct()
                .name("meta").required()
                .field("id", SchemaBuilder.string().name("id").required())
                .field("dt", SchemaBuilder.string().name("dt").required())
                .field("schema_uri", SchemaBuilder.string().name("schema_uri").required()).build()
            )
            .field("page_id", SchemaBuilder.int64().name("page_id").optional())
            .field("rev_content_model", SchemaBuilder.string().name("rev_content_model").optional())
            .field("list_field", SchemaBuilder.array(Schema.STRING_SCHEMA).name("list_field").optional())
            .field("complex_list_field", SchemaBuilder.array(complexListElementSchema).name("complex_list_field").optional())
            .field("bad_field_name", SchemaBuilder.string().name("bad_field_name").optional())
            .build();

        expectedValue = new Struct(expectedSchema.schema());

        Struct metaField = new Struct(expectedSchema.field("meta").schema());
        metaField.put("id", "123");
        metaField.put("dt", "2018-05-22T18:23:16+00:00");
        metaField.put("schema_uri", "/schema.json");

        expectedValue.put("meta", metaField);
        expectedValue.put("page_id", 1L);
        expectedValue.put("rev_content_model", "text");

        ArrayList<String> listField = new ArrayList<>();
        listField.add("hi");
        listField.add("there");
        expectedValue.put("list_field", listField);

        ArrayList<Struct> complexListField = new ArrayList<>();
        Struct complexListFieldElement = new Struct(complexListElementSchema);
        complexListFieldElement.put("elem1", "complicated");
        complexListField.add(complexListFieldElement);
        expectedValue.put("complex_list_field", complexListField);

        expectedValue.put("bad_field_name", "bad");


        expectedUnsanitizedSchema = SchemaBuilder.struct()
                .name("mediawiki/revision/create")
                .field("meta", SchemaBuilder.struct()
                        .name("meta").required()
                        .field("id", SchemaBuilder.string().name("id").required())
                        .field("dt", SchemaBuilder.string().name("dt").required())
                        .field("schema_uri", SchemaBuilder.string().name("schema_uri").required()).build()
                )
                .field("page_id", SchemaBuilder.int64().name("page_id").optional())
                .field("rev_content_model", SchemaBuilder.string().name("rev_content_model").optional())
                .field("list_field", SchemaBuilder.array(Schema.STRING_SCHEMA).name("list_field").optional())
                .field("complex_list_field", SchemaBuilder.array(complexListElementSchema).name("complex_list_field").optional())
                .field("bad.field name", SchemaBuilder.string().name("bad.field name").optional())
                .build();

        expectedUnsanitizedValue = new Struct(expectedUnsanitizedSchema.schema());
        Struct metaUnsanitizedField = new Struct(expectedUnsanitizedSchema.field("meta").schema());
        metaUnsanitizedField.put("id", "123");
        metaUnsanitizedField.put("dt", "2018-05-22T18:23:16+00:00");
        metaUnsanitizedField.put("schema_uri", "/schema.json");
        expectedUnsanitizedValue.put("meta", metaUnsanitizedField);
        expectedUnsanitizedValue.put("page_id", 1L);
        expectedUnsanitizedValue.put("rev_content_model", "text");
        // listField and complexListField are already defined, we can reuse
        expectedUnsanitizedValue.put("list_field", listField);
        expectedUnsanitizedValue.put("complex_list_field", complexListField);
        expectedUnsanitizedValue.put("bad.field name", "bad");
    }

    static String topic = "mediawiki.revision-create";



    public void assertSchemasEqual(Schema expected, Schema actual) {
        assertEquals(expected.type(),           actual.type(),          "type should match for field " + expected.name());
        assertEquals(expected.isOptional(),     actual.isOptional(),    "isOptional should match for field " + expected.name());
        assertEquals(expected.defaultValue(),   actual.defaultValue(),  "defaultValue should match for field " + expected.name());
        assertEquals(expected.version(),        actual.version(),       "version should match for field " + expected.name());
        assertEquals(expected.parameters(),     actual.parameters(),    "parameters should match for field " + expected.name());
        assertEquals(expected.name(), actual.name());

        // If this is a nested struct schema, then check all nested fields too.
        if (expected.type() == Schema.Type.STRUCT) {
            assertEquals(
                expected.fields().size(), actual.fields().size(),
                "struct schema " + expected.name() + " had " + actual.fields().size() + " fields, expected " + expected.fields().size()
            );

            for (Field expectedField: expected.fields()) {
                Field actualField = actual.field(expectedField.name());
                assertNotNull(
                    actualField,
                    "Field " + expectedField.name()  + " was expected but is not present"
                );
                assertSchemasEqual(actualField.schema(), expectedField.schema());
            }
        }
    }

    @BeforeEach
    public void setUp() throws Exception {


        Map<String, Object> conf = new HashMap<>();
        conf.put(JsonSchemaConverterConfig.SCHEMA_URI_PREFIX_CONFIG, schemaURIPrefix);
        conf.put(JsonSchemaConverterConfig.SCHEMA_URI_FALLBACK_CONFIG, "/fallback/${topic}.json");

        converter.configure(conf, false);

        conf.put(JsonSchemaConverterConfig.SANITIZE_FIELD_NAMES_CONFIG, "false");
        unsanitizedConverter.configure(conf, false);

        ObjectMapper objectMapper = new ObjectMapper();

        schemaJson  = objectMapper.readTree(schemaFileJson);
        recordBytes = Files.readAllBytes(recordFileWithJsonSchema.toPath());
        recordWithJsonSchema = objectMapper.readTree(recordFileWithJsonSchema);

        schemaYaml  = objectMapper.readTree(new YAMLFactory().createParser(schemaFileYaml));
        recordWithYamlSchema = objectMapper.readTree(recordFileWithYamlSchema);

        recordWithoutSchema = objectMapper.readTree(recordFileWithoutSchema);
    }


    @Test
    public void booleanAsConnectSchema() throws IOException {
        assertEquals(
            SchemaBuilder.bool().build(),
            converter.asConnectSchema(new ObjectMapper().readTree("{ \"type\": \"boolean\" }"))
        );
    }

    @Test
    public void getSchemaURI() throws Exception {
        assertEquals(
            schemaURIPrefix + "/schema.json",
            converter.getSchemaURI(topic, recordWithJsonSchema).toString()
        );
        assertEquals(
            schemaURIPrefix + "/schema.yaml",
            converter.getSchemaURI(topic, recordWithYamlSchema).toString()
        );
        assertEquals(
            schemaURIPrefix + "/fallback/" + topic + ".json",
            converter.getSchemaURI(topic, recordWithoutSchema).toString()
        );
    }

    @Test
    public void getSchemaVersion() throws Exception {
        assertNull(
            converter.getSchemaVersion(topic, schemaURIPrefix + "/schema.json")
        );
        assertEquals(
            new Integer(1),
            converter.getSchemaVersion(topic, schemaURIPrefix + "/schema/1.json")
        );
        assertEquals(
            new Integer(1),
            converter.getSchemaVersion(topic, schemaURIPrefix + "/schema/1.yaml")
        );
        assertEquals(
            new Integer(1),
            converter.getSchemaVersion(topic, schemaURIPrefix + "/schema/1")
        );
    }

    @Test
    public void getJsonSchema() throws Exception {
       assertEquals(
           schemaJson,
           converter.getJsonSchema(converter.getSchemaURI(topic, recordWithJsonSchema))
       );
    }

    @Test
    public void getJsonSchemaFromYaml() throws Exception {

        assertEquals(
            schemaYaml,
            converter.getJsonSchema(converter.getSchemaURI(topic, recordWithYamlSchema))
        );
    }

    @Test
    public void asConnectSchema() throws Exception {
        Schema connectSchema = converter.asConnectSchema(schemaJson);
        assertSchemasEqual(expectedSchema, connectSchema);
    }

    @Test
    public void toConnectData() throws Exception {
        SchemaAndValue connectData = converter.toConnectData(topic, recordBytes);

        // TODO test that fields are normalized/sanitized.

        // Struct's toString uses the types and values to build a string.
        // Assert that the returned Strings are equals, to avoid object instance comparision.
        assertEquals(expectedValue.toString(), connectData.value().toString(), "toConnectData should return exactly this value");
        assertSchemasEqual(expectedSchema, connectData.schema());
        // TODO compare data?
    }

    @Test
    public void fromConnectData() throws Exception {
        SchemaAndValue connectData = converter.toConnectData(topic, recordBytes);

        byte[] jsonBytes = converter.fromConnectData(
            topic,
            connectData.schema(),
            connectData.value()
        );

        // Assert that the json record from the original file is the same
        // as the one that Kafka Connect converted to Connect format and then back
        // to json string bytes.  This should be handled by the parent
        // JsonConverter in Kafka Connect.

        // Since JsonSchemaConverter sanitizes field names by default, we need to sanitize our input
        // JSON to assert it matches the converted data.
        String expectedJsonString = recordWithJsonSchema.toString().replaceFirst("bad\\.field name", "bad_field_name");
        assertEquals(expectedJsonString, new String(jsonBytes));
    }


    /**
     * Test that a converter configured to NOT sanitize field names will
     * convert to and from keeping the field names exactly the same.
     * @throws Exception
     */
    @Test
    public void convertUnsanitized() throws Exception {

        SchemaAndValue connectData = unsanitizedConverter.toConnectData(topic, recordBytes);

        assertEquals(expectedUnsanitizedValue.toString(), connectData.value().toString(), "toConnectData should return exactly this value");
        assertSchemasEqual(expectedUnsanitizedSchema, connectData.schema());


        byte[] jsonBytes = unsanitizedConverter.fromConnectData(
                topic,
                connectData.schema(),
                connectData.value()
        );

        // Assert that the json record from the original file is the same
        // as the one that Kafka Connect converted to Connect format and then back
        // to json string bytes.  This should be handled by the parent
        // JsonConverter in Kafka Connect.
        assertEquals(recordWithJsonSchema.toString(), new String(jsonBytes));
    }
}
