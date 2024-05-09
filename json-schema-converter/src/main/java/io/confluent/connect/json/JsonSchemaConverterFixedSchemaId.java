/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchemaProvider;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe;
import io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.AbstractKafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.json.JsonSchemaAndValue;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializerConfig;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

public class JsonSchemaConverterFixedSchemaId extends AbstractKafkaSchemaSerDe implements
    Converter {

  private SchemaRegistryClient schemaRegistry;

  private Serializer serializer;

  private Deserializer deserializer;

  private boolean isKey;

  private JsonSchemaData jsonSchemaData;

  private Integer idSchema;

  public JsonSchemaConverterFixedSchemaId() {
  }

  @VisibleForTesting
  public JsonSchemaConverterFixedSchemaId(SchemaRegistryClient client) {
    this.schemaRegistry = client;
  }

  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
    this.idSchema = Integer.valueOf((String) configs.get("fixed.schema.id"));
    JsonSchemaConverterConfig jsonSchemaConverterConfig = new JsonSchemaConverterConfig(configs);
    if (this.schemaRegistry == null) {
      this

          .schemaRegistry = new CachedSchemaRegistryClient(
          jsonSchemaConverterConfig.getSchemaRegistryUrls(),
          jsonSchemaConverterConfig.getMaxSchemasPerSubject(),
          Collections.singletonList(new JsonSchemaProvider()), configs,
          jsonSchemaConverterConfig.requestHeaders());
    }
    this.serializer = new Serializer(configs, this.schemaRegistry);
    this.deserializer = new Deserializer(configs, this.schemaRegistry);
    this.jsonSchemaData = new JsonSchemaData(new JsonSchemaDataConfig(configs));
  }

  public byte[] fromConnectData(String topic, Schema schema, Object value) {
    if (schema == null && value == null) {
      return null;
    }
    JsonNode jsonValue = this.jsonSchemaData.fromConnectData(schema, value);
    try {
      return this.serializer.serialize(jsonValue, this.idSchema);
    } catch (SerializationException e) {
      throw new DataException(String.format(
          "Converting Kafka Connect data to byte[] failed due to serialization error of topic %s: ",
          topic), e);
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access JSON Schema data from topic %s : %s",
              topic, e.getMessage()));
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return toConnectData(topic, null, value);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
    try {
      JsonSchemaAndValue deserialized = this.deserializer.deserialize(topic, this.isKey, headers,
          value);
      if (deserialized == null || deserialized.getValue() == null) {
        return SchemaAndValue.NULL;
      }
      JsonSchema jsonSchema = deserialized.getSchema();
      Schema schema = this.jsonSchemaData.toConnectSchema(jsonSchema);
      return new SchemaAndValue(schema,
          jsonSchemaData.toConnectData(schema, (JsonNode) deserialized.getValue()));
    } catch (SerializationException e) {
      throw new DataException(String.format(
          "Converting byte[] to Kafka Connect data failed due to serialization error of topic %s: ",
          topic), e);
    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access JSON Schema data from topic %s : %s",
              topic, e.getMessage()));
    }
  }

  private static class Serializer extends AbstractKafkaJsonSchemaSerializer {

    public Serializer(SchemaRegistryClient client, boolean autoRegisterSchema) {
      this.schemaRegistry = client;
      this.autoRegisterSchema = autoRegisterSchema;
    }

    public Serializer(Map<String, ?> configs, SchemaRegistryClient client) {
      this(client, false);
      configure(new KafkaJsonSchemaSerializerConfig(configs));
    }

    public byte[] serialize(Object value, Integer idSchema) {
      if (value == null) {
        return null;
      }
      return serializeSimplifiedImpl(value, idSchema);
    }

    public byte[] serializeSimplifiedImpl(Object object, Integer idSchema)
        throws SerializationException, InvalidConfigurationException {
      if (this.schemaRegistry == null) {
        throw new InvalidConfigurationException(
            "SchemaRegistryClient not found. You need to configure the serializer or use"
                + " serializer constructor with SchemaRegistryClient.");
      }
      if (object == null) {
        return null;
      }
      try {
        int id = idSchema;
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0);
        out.write(ByteBuffer.allocate(4).putInt(id).array());
        out.write(objectMapper.writeValueAsBytes(object));
        byte[] bytes = out.toByteArray();
        out.close();
        return bytes;
      } catch (IOException | RuntimeException e) {
        throw new SerializationException("Error serializing JSON message", e);
      }
    }
  }

  private static class Deserializer extends AbstractKafkaJsonSchemaDeserializer {

    public Deserializer(SchemaRegistryClient client) {
      this.schemaRegistry = client;
    }

    public Deserializer(Map<String, ?> configs, SchemaRegistryClient client) {
      this(client);
      configure(new KafkaJsonSchemaDeserializerConfig(configs), null);
    }

    public JsonSchemaAndValue deserialize(String topic, boolean isKey, Headers headers,
        byte[] payload) {
      return deserializeWithSchemaAndVersion(topic, isKey, headers, payload);
    }
  }
}