package com.bpcyber.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RecordExpanderTest {

    private RecordExpander<SourceRecord> createTransformer(boolean serializeAsJson) {
        RecordExpander<SourceRecord> transformer = new RecordExpander<>();
        Map<String, Object> config = new HashMap<>();
        config.put("includeKey", true);
        config.put("includeHeaders", true);
        config.put("includeMetadata", true);
        config.put("serializeValueAsJson", serializeAsJson);
        transformer.configure(config);
        return transformer;
    }

    @Test
    void shouldExpandRecordWithKeyValueAndHeaders() {
        RecordExpander<SourceRecord> transformer = createTransformer(false);

        // Setup schema and value
        Schema valueSchema = SchemaBuilder.struct().name("LoginEvent")
                .field("event", Schema.STRING_SCHEMA)
                .field("timestamp", Schema.STRING_SCHEMA)
                .build();

        Struct valueStruct = new Struct(valueSchema)
                .put("event", "login")
                .put("timestamp", "2025-06-10T12:00:00Z");

        ConnectHeaders headers = new ConnectHeaders();
        headers.add("source", "web", Schema.STRING_SCHEMA);

        SourceRecord originalRecord = new SourceRecord(
                null, null, "test-topic", null,
                Schema.STRING_SCHEMA, "user-123",
                valueSchema, valueStruct,
                System.currentTimeMillis(), headers);

        SourceRecord transformed = transformer.apply(originalRecord);
        Struct resultValue = (Struct) transformed.value();

        assertEquals("user-123", resultValue.getString("key"));
        assertEquals(valueStruct, resultValue.getStruct("value"));
        Map<String, String> headerMap = (Map<String, String>) resultValue.get("headers");
        assertEquals("web", headerMap.get("source"));
        assertEquals("test-topic", resultValue.getString("topic"));
    }

    @Test
    void shouldExpandRecordWithoutSchema() {
        RecordExpander<SourceRecord> transformer = createTransformer(false);

        // Setup value without schema
        String value = "simpleValue";

        ConnectHeaders headers = new ConnectHeaders();
        headers.add("source", "web", Schema.STRING_SCHEMA);

        SourceRecord originalRecord = new SourceRecord(
                null, null, "test-topic", null,
                Schema.STRING_SCHEMA, "user-123",
                null, value,
                System.currentTimeMillis(), headers);

        SourceRecord transformed = transformer.apply(originalRecord);

        HashMap<String, Object> resultValue = (HashMap<String, Object>) transformed.value();

        assertEquals("user-123", resultValue.get("key"));
        assertEquals(value, resultValue.get("value"));
        Map<String, String> headerMap = (Map<String, String>) resultValue.get("headers");
        assertEquals("web", headerMap.get("source"));
        assertEquals("test-topic", resultValue.get("topic"));
    }

    @Test
    void shouldExpandRecordWithJsonValue() {
        RecordExpander<SourceRecord> transformer = createTransformer(false);

        // Setup JSON value
        HashMap<String, Object> inputJSON = new HashMap<String, Object>() {
            {
                put("firstName", "example");
                put("lastName", "user");
            }
        };

        ConnectHeaders headers = new ConnectHeaders();
        headers.add("source", "web", Schema.STRING_SCHEMA);

        SourceRecord originalRecord = new SourceRecord(
                null, null, "test-topic", null,
                Schema.STRING_SCHEMA, "user-123",
                null, inputJSON,
                System.currentTimeMillis(), headers);

        SourceRecord transformed = transformer.apply(originalRecord);
        HashMap<String, Object> resultValue = (HashMap<String, Object>) transformed.value();

        assertEquals("user-123", resultValue.get("key"));
        assertEquals(inputJSON, resultValue.get("value"));
        Map<String, String> headerMap = (Map<String, String>) resultValue.get("headers");
        assertEquals("web", headerMap.get("source"));
        assertEquals("test-topic", resultValue.get("topic"));
    }

    @Test
    public void testWithJsonSerialization() {
        RecordExpander<SourceRecord> transformer = createTransformer(true);

        // Setup JSON value
        HashMap<String, Object> inputJSON = new HashMap<String, Object>() {
            {
                put("firstName", "example");
                put("lastName", "user");
            }
        };

        ConnectHeaders headers = new ConnectHeaders();
        headers.add("source", "web", Schema.STRING_SCHEMA);

        SourceRecord original = new SourceRecord(
                null, null, "test-topic", null,
                Schema.STRING_SCHEMA, "user-123",
                null, inputJSON,
                System.currentTimeMillis(), headers);

        // ObjectMapper mapper = new ObjectMapper();

        SourceRecord result = transformer.apply(original);
        Map<String, Object> value = (Map<String, Object>) result.value();

        assertEquals("user-123", value.get("key"));
        assertTrue(value.get("value") instanceof String);

        String actualJson = (String) value.get("value");

        // compare actualJson with expected jsonValue

        assertEquals("{\"firstName\":\"example\",\"lastName\":\"user\"}", actualJson);

    }
}
