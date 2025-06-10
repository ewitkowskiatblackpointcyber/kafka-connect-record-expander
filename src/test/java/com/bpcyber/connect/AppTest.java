package com.bpcyber.connect;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RecordExpanderTest {

    private RecordExpander<SourceRecord> transformer;

    @BeforeEach
    void setup() {
        transformer = new RecordExpander<>();
        Map<String, String> config = new HashMap<>();
        config.put("includeKey", "true");
        config.put("includeHeaders", "true");
        config.put("keyFieldName", "originalKey");
        config.put("valueFieldName", "originalValue");
        config.put("headersFieldName", "originalHeaders");
        transformer.configure(config);
    }

    @Test
    void shouldExpandRecordWithKeyValueAndHeaders() {
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

        assertEquals("user-123", resultValue.getString("originalKey"));
        assertEquals(valueStruct, resultValue.getStruct("originalValue"));
        Map<String, String> headerMap = (Map<String, String>) resultValue.get("originalHeaders");
        assertEquals("web", headerMap.get("source"));
    }

    @Test
    void shouldExpandRecordWithoutSchema() {
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

        assertEquals("user-123", resultValue.get("originalKey"));
        assertEquals(value, resultValue.get("originalValue"));
        Map<String, String> headerMap = (Map<String, String>) resultValue.get("originalHeaders");
        assertEquals("web", headerMap.get("source"));
    }

    @Test
    void shouldExpandRecordWithJsonValue() {
        // Setup JSON value
        String jsonValue = "{\"event\":\"login\",\"timestamp\":\"2025-06-10T12:00:00Z\"}";

        ConnectHeaders headers = new ConnectHeaders();
        headers.add("source", "web", Schema.STRING_SCHEMA);

        SourceRecord originalRecord = new SourceRecord(
                null, null, "test-topic", null,
                Schema.STRING_SCHEMA, "user-123",
                null, jsonValue,
                System.currentTimeMillis(), headers);

        SourceRecord transformed = transformer.apply(originalRecord);
        HashMap<String, Object> resultValue = (HashMap<String, Object>) transformed.value();

        assertEquals("user-123", resultValue.get("originalKey"));
        assertEquals(jsonValue, resultValue.get("originalValue"));
        Map<String, String> headerMap = (Map<String, String>) resultValue.get("originalHeaders");
        assertEquals("web", headerMap.get("source"));
    }
}
