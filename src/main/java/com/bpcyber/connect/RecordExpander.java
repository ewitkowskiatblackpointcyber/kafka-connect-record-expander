package com.bpcyber.connect;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RecordExpander<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final Logger log = LoggerFactory.getLogger(RecordExpander.class);

    public static final String INCLUDE_KEY_CONFIG = "includeKey";
    public static final String INCLUDE_HEADERS_CONFIG = "includeHeaders";
    public static final String INCLUDE_METADATA_CONFIG = "includeMetadata";
    public static final String SERIALIZE_VALUE_CONFIG = "serializeValueAsJson";

    public static final String KEY_FIELD_NAME_CONFIG = "keyFieldName";
    public static final String VALUE_FIELD_NAME_CONFIG = "valueFieldName";
    public static final String HEADERS_FIELD_NAME_CONFIG = "headersFieldName";

    public static final String TOPIC_FIELD_NAME = "topic";
    public static final String PARTITION_FIELD_NAME = "partition";
    public static final String TIMESTAMP_FIELD_NAME = "timestamp";

    private boolean includeKey;
    private boolean includeHeaders;
    private boolean includeMetadata;
    private boolean serializeValueAsJson;

    private String keyFieldName;
    private String valueFieldName;
    private String headersFieldName;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        includeKey = config.getBoolean(INCLUDE_KEY_CONFIG);
        includeHeaders = config.getBoolean(INCLUDE_HEADERS_CONFIG);
        includeMetadata = config.getBoolean(INCLUDE_METADATA_CONFIG);
        serializeValueAsJson = config.getBoolean(SERIALIZE_VALUE_CONFIG);

        keyFieldName = config.getString(KEY_FIELD_NAME_CONFIG);
        valueFieldName = config.getString(VALUE_FIELD_NAME_CONFIG);
        headersFieldName = config.getString(HEADERS_FIELD_NAME_CONFIG);
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(INCLUDE_KEY_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM, "Include record key")
            .define(INCLUDE_HEADERS_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                    "Include record headers")
            .define(INCLUDE_METADATA_CONFIG, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.MEDIUM,
                    "Include Kafka metadata: topic, partition, timestamp")
            .define(SERIALIZE_VALUE_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.MEDIUM,
                    "Serialize 'value' field as JSON string")
            .define(KEY_FIELD_NAME_CONFIG, ConfigDef.Type.STRING, "key", ConfigDef.Importance.MEDIUM,
                    "Key field name")
            .define(VALUE_FIELD_NAME_CONFIG, ConfigDef.Type.STRING, "value", ConfigDef.Importance.MEDIUM,
                    "Value field name")
            .define(HEADERS_FIELD_NAME_CONFIG, ConfigDef.Type.STRING, "headers", ConfigDef.Importance.MEDIUM,
                    "Headers field name");

    @Override
    public R apply(R record) {
        if (record.value() == null)
            return record;

        final Object originalValue = record.value();
        final Schema originalSchema = record.valueSchema();
        final boolean isSchemaless = originalSchema == null;

        log.trace("Original value: {}", originalValue);

        Object newValue;
        Schema newSchema = null;

        if (isSchemaless) {
            Map<String, Object> newJsonValue = new HashMap<>();

            if (includeKey) {
                newJsonValue.put(keyFieldName, record.key() != null ? record.key().toString() : null);
            }

            Object transformedValue = serializeValueAsJson
                    ? toJsonString(originalValue)
                    : originalValue;

            newJsonValue.put(valueFieldName, transformedValue);

            if (includeHeaders) {
                Map<String, String> headersMap = new HashMap<>();
                for (Header header : record.headers()) {
                    if (header.value() != null) {
                        headersMap.put(header.key(), header.value().toString());
                    }
                }
                newJsonValue.put(headersFieldName, headersMap);
            }

            if (includeMetadata) {
                newJsonValue.put(TOPIC_FIELD_NAME, record.topic());
                newJsonValue.put(PARTITION_FIELD_NAME, record.kafkaPartition());
                newJsonValue.put(TIMESTAMP_FIELD_NAME, record.timestamp());
            }

            newValue = newJsonValue;
            log.trace("Generated schemaless new value: {}", newJsonValue);
        } else {
            SchemaBuilder builder = SchemaBuilder.struct().name("ExpandedRecord");

            if (includeKey) {
                builder.field(keyFieldName, Schema.OPTIONAL_STRING_SCHEMA);
            }

            builder.field(valueFieldName, serializeValueAsJson ? Schema.STRING_SCHEMA : originalSchema);

            if (includeHeaders) {
                builder.field(headersFieldName,
                        SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build());
            }

            if (includeMetadata) {
                builder.field(TOPIC_FIELD_NAME, Schema.OPTIONAL_STRING_SCHEMA);
                builder.field(PARTITION_FIELD_NAME, Schema.OPTIONAL_INT32_SCHEMA);
                builder.field(TIMESTAMP_FIELD_NAME, Schema.OPTIONAL_INT64_SCHEMA);
            }

            newSchema = builder.build();
            Struct newStructValue = new Struct(newSchema);

            if (includeKey) {
                newStructValue.put(keyFieldName, record.key() != null ? record.key().toString() : null);
            }

            Object transformedValue = serializeValueAsJson
                    ? toJsonString(originalValue)
                    : originalValue;

            newStructValue.put(valueFieldName, transformedValue);

            if (includeHeaders) {
                Map<String, String> headersMap = new HashMap<>();
                for (Header header : record.headers()) {
                    if (header.value() != null) {
                        headersMap.put(header.key(), header.value().toString());
                    }
                }
                newStructValue.put(headersFieldName, headersMap);
            }

            if (includeMetadata) {
                newStructValue.put(TOPIC_FIELD_NAME, record.topic());
                newStructValue.put(PARTITION_FIELD_NAME, record.kafkaPartition());
                newStructValue.put(TIMESTAMP_FIELD_NAME, record.timestamp());
            }

            newValue = newStructValue;
            log.trace("Generated schema-based new value: {}", newStructValue);
        }

        return record.newRecord(
                record.topic(),
                record.kafkaPartition(),
                record.keySchema(),
                record.key(),
                newSchema,
                newValue,
                record.timestamp());
    }

    private String toJsonString(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize value to JSON", e);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // No resources to close
    }
}
