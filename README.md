# kafka-connect-record-expander

**kafka-connect-record-expander** is a custom [Kafka Connect](https://kafka.apache.org/documentation/#connect) [Single Message Transform (SMT)](https://docs.confluent.io/platform/current/connect/transforms/index.html) that enriches Kafka records by expanding their structure. It adds the original **key**, **value**, and **headers** into the record's **value**, making downstream processing easier and more consistent — especially for sinks that expect a unified record structure.

---

## 🔧 Use Case

This transform is useful when:

* You want to persist both key and value in a single structured payload.
* Headers need to be visible within the value for auditing or downstream consumption.
* You're writing to data sinks like S3, Elasticsearch, or BigQuery, which benefit from having all record data in one JSON object.

---

## 🧩 Features

* Injects the Kafka record’s `key`, `value`, and `headers` into the output `value`.
* Supports Avro, JSON, and String formats.
* Can be configured to include only selected fields (`key`, `headers`, etc.).

---

## 🚀 Example

### Input Kafka Record

```json
{
  "key": "user-123",
  "value": {
    "event": "login",
    "timestamp": "2025-06-10T12:00:00Z"
  },
  "headers": {
    "source": "web"
  }
}
```

### Output Kafka Record (value)

```json
{
  "originalKey": "user-123",
  "originalValue": {
    "event": "login",
    "timestamp": "2025-06-10T12:00:00Z"
  },
  "originalHeaders": {
    "source": "web"
  }
}
```

---

## 🛠 Configuration

Add this SMT to your Kafka Connect connector configuration:

```json
"transforms": "expand",
"transforms.expand.type": "com.bpcyber.connect.RecordExpander",
"transforms.expand.includeKey": "true",
"transforms.expand.includeHeaders": "true"
"transforms.expand.includeMetadata": "true"
```

| Config Option      | Type    | Description                                    | Default           |
| ------------------ | ------- | -----------------------------------------------| ----------------- |
| `includeKey`       | boolean | Whether to include the key                     | true              |
| `includeHeaders`   | boolean | Whether to include the headers                 | true              |
| `includeMetadata`  | boolean | Whether to include timestamp, topic, partition | true              |
| `keyFieldName`     | string  | Field name to store the key                    | `originalKey`     |
| `valueFieldName`   | string  | Field name to store the value                  | `originalValue`   |
| `headersFieldName` | string  | Field name to store the headers                | `originalHeaders` |

---

## 📦 Installation

1. Build the JAR:

```bash
mvn clean package
```

2. Copy the JAR to your Kafka Connect `plugins` directory:

```bash
cp target/kafka-connect-record-expander*.jar /path/to/connect/plugins/
```

3. Restart your Kafka Connect worker.

---

## 🧪 Testing

To run unit tests:

```bash
mvn test
```

---

## 📄 License

MIT License. See [LICENSE](LICENSE) for details.

---

## 👥 Contributing

Contributions, issues, and feature requests are welcome! Feel free to open a pull request or issue.
