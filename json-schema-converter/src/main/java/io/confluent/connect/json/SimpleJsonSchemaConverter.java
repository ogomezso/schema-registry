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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SimpleJsonSchemaConverter extends AbstractKafkaSchemaSerDe implements Converter {

  private static final Logger log = LoggerFactory.getLogger(SimpleJsonSchemaConverter.class);
  private SchemaRegistryClient schemaRegistry;
  private Serializer serializer;
  private Deserializer deserializer;
  private boolean isKey;
  private JsonSchemaData jsonSchemaData;

  public SimpleJsonSchemaConverter() {
  }

  @VisibleForTesting
  public SimpleJsonSchemaConverter(SchemaRegistryClient client) {
    this.schemaRegistry = client;
  }


  public void configure(Map<String, ?> configs, boolean isKey) {
    this.isKey = isKey;
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
    return fromConnectData(topic, null, schema, value);
  }

  @Override
  public byte[] fromConnectData(String topic, Headers headers, Schema schema, Object value) {
    if (schema == null && value == null) {
      return null;
    }
    JsonSchema jsonSchema = this.jsonSchemaData.fromConnectSchema(schema);
    JsonNode jsonValue = this.jsonSchemaData.fromConnectData(schema, value);
    try {
      return this.serializer.serialize(topic, this.isKey, jsonValue, jsonSchema);
    } catch (SerializationException e) {
      throw new DataException(String.format(
          "Converting Kafka Connect data to byte[] failed due to serialization error of topic %s: ",
          topic), e);


    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access JSON Schema data from topic %s : %s", topic,
              e.getMessage()));
    }
  }

  @Override
  public SchemaAndValue toConnectData(String topic, byte[] value) {
    return toConnectData(topic,null,value);
  }

  @Override
  public SchemaAndValue toConnectData(String topic, Headers headers, byte[] value) {
    try {
      Object deserialized = this.deserializer.simpleDeserialize(value);

      if (deserialized == null) {
        return SchemaAndValue.NULL;
      }

      log.debug("[SCHEMA CONVERTER RESULT MESSAGE]:" + deserialized);

      return new SchemaAndValue(null, deserialized);
    } catch (SerializationException e) {
      throw new DataException(String.format(
          "Converting byte[] to Kafka Connect data failed due to serialization error of topic %s: ",
          topic), e);


    } catch (InvalidConfigurationException e) {
      throw new ConfigException(
          String.format("Failed to access JSON Schema data from topic %s : %s", topic,
              e.getMessage()));
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

    public byte[] serialize(String topic, boolean isKey, Object value, JsonSchema schema) {
      if (value == null) {
        return null;
      }
      return serializeImpl(getSubjectName(topic, isKey, value, schema), value, schema);
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

    public Object simpleDeserialize(byte[] payload) {
      return deserialize(payload);
    }
  }
}
