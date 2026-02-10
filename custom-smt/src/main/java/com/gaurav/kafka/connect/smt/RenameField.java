package com.gaurav.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Field;

import java.util.Map;

public class RenameField<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String SOURCE_FIELD_CONFIG = "source.field";
    public static final String TARGET_FIELD_CONFIG = "target.field";

    private String sourceField;
    private String targetField;

    // Kafka Connect calls this once when the connector starts
    @Override
    public void configure(Map<String, ?> configs) {
        this.sourceField = (String) configs.get(SOURCE_FIELD_CONFIG);
        this.targetField = (String) configs.get(TARGET_FIELD_CONFIG);
    }

    // apply() – Transform each record
    @Override
    public R apply(R record) {
        if (record.value() == null) {
            return record;
        }

        // Handle Avro / Struct records
        if (record.value() instanceof Struct) {
            Struct value = (Struct) record.value();
            Schema schema = value.schema();

            SchemaBuilder builder = SchemaBuilder.struct().name(schema.name());
            for (Field field : schema.fields()) {
                if (!field.name().equals(sourceField)) {
                    builder.field(field.name(), field.schema());
                }
            }

            // Preserve optionality: if original field was optional, keep it optional
            Schema targetSchema = Schema.OPTIONAL_STRING_SCHEMA;
            Field srcField = schema.field(sourceField);
            if (srcField != null && srcField.schema().type() != null) {
                targetSchema = srcField.schema().isOptional()
                        ? Schema.OPTIONAL_STRING_SCHEMA
                        : Schema.STRING_SCHEMA;
            }

            builder.field(targetField, targetSchema);

            Schema updatedSchema = builder.build();
            Struct updatedValue = new Struct(updatedSchema);

            for (Field field : schema.fields()) {
                if (!field.name().equals(sourceField)) {
                    updatedValue.put(field.name(), value.get(field));
                }
            }

            updatedValue.put(targetField, value.get(sourceField));

            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    updatedSchema,
                    updatedValue,
                    record.timestamp()
            );
        }

        return record;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define(SOURCE_FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Field to rename")
                .define(TARGET_FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "New field name");
    }

    @Override
    public void close() {
        // no-op
    }
}
