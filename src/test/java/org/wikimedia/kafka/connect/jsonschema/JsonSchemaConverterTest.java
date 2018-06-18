package org.wikimedia.kafka.connect.jsonschema;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.*;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.connect.errors.DataException;

//import org.powermock.reflect.Whitebox;

import java.nio.file.Files;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

public class JsonSchemaConverterTest {

    private static final String TOPIC = "topic";

    JsonSchemaConverter converter = new JsonSchemaConverter();

    File resourcesDirectory = new File("src/test/resources");
    String schemaURIPrefix = "file://" + resourcesDirectory.getAbsolutePath();

    File schemaFile = new File("src/test/resources/schema.json");
    File recordFile = new File("src/test/resources/record.json");

    byte[]   recordBytes;
    JsonNode record;
    JsonNode schema;

    static Schema expectedSchema;
    static Struct expectedValue;

    static {
        expectedSchema = SchemaBuilder.struct()
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
    }


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

        converter.configure(conf, false);

        schema      = new ObjectMapper().readTree(schemaFile);
        recordBytes = Files.readAllBytes(recordFile.toPath());
        record      = new ObjectMapper().readTree(recordFile);
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
                converter.getSchemaURI(record).toString()
        );
    }

    @Test
    public void getJsonSchema() throws Exception {
       assertEquals(
           new ObjectMapper().readTree(schemaFile),
           converter.getJsonSchema(converter.getSchemaURI(record))
       );
    }

    @Test
    public void asConnectSchema() throws Exception {
        Schema connectSchema = converter.asConnectSchema(schema);
        assertSchemasEqual(expectedSchema, connectSchema);
    }

    @Test
    public void toConnectData() throws Exception {
        SchemaAndValue connectData = converter.toConnectData("test", recordBytes);

        // Struct's toString uses the types and values to build a string.
        //  Assert that the returned Strings are equals, to avoid object instance comparision.
        assertEquals(expectedValue.toString(), connectData.value().toString(), "toConnectData should return exactly this value");
        assertSchemasEqual(expectedSchema, connectData.schema());
    }

}
