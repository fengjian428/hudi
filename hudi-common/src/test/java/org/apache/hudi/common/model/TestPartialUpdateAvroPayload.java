/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.common.model;

import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests {@link TestPartialUpdateAvroPayload}.
 */
public class TestPartialUpdateAvroPayload {
  private Schema schema;

  String jsonSchema = "{\n" +
          "  \"type\": \"record\",\n" +
          "  \"name\": \"partialRecord\", \"namespace\":\"org.apache.hudi\",\n" +
          "  \"fields\": [\n" +
          "    {\"name\": \"id\", \"type\": [\"null\", \"string\"]},\n" +
          "    {\"name\": \"partition\", \"type\": [\"null\", \"string\"]},\n" +
          "    {\"name\": \"ts\", \"type\": [\"null\", \"long\"]},\n" +
          "    {\"name\": \"_hoodie_is_deleted\", \"type\": [\"null\", \"boolean\"], \"default\":false},\n" +
          "    {\"name\": \"city\", \"type\": [\"null\", \"string\"]},\n" +
          "    {\"name\": \"child\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}]}\n" +
          "  ]\n" +
          "}";
  @BeforeEach
  public void setUp() throws Exception {
    schema = new Schema.Parser().parse(jsonSchema);
  }

  @Test
  public void testActiveRecords() throws IOException {
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition1");
    record1.put("ts", null);
    record1.put("_hoodie_is_deleted", false);
    record1.put("city", "NY0");
    record1.put("child", Arrays.asList("A"));

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "1");
    record2.put("partition", "partition1");
    record2.put("ts", 0L);
    record2.put("_hoodie_is_deleted", false);
    record2.put("city", null);
    record2.put("child", Arrays.asList("B"));

    GenericRecord record3 = new GenericData.Record(schema);
    record3.put("id", "1");
    record3.put("partition", "partition1");
    record3.put("ts", 0L);
    record3.put("_hoodie_is_deleted", false);
    record3.put("city", "NY0");
    record3.put("child", Arrays.asList("A"));

    GenericRecord record4 = new GenericData.Record(schema);
    record4.put("id", "1");
    record4.put("partition", "partition1");
    record4.put("ts", 0L);
    record4.put("_hoodie_is_deleted", false);
    record4.put("city", "NY0");
    record4.put("child", Arrays.asList("B"));


    PartialUpdateAvroPayload payload1 = new PartialUpdateAvroPayload(record1, 1);
    PartialUpdateAvroPayload payload2 = new PartialUpdateAvroPayload(record2, 2);
    assertArrayEquals(payload1.preCombine(payload2, schema).recordBytes, new PartialUpdateAvroPayload(record3,1).recordBytes);
    assertArrayEquals(payload2.preCombine(payload1, schema).recordBytes, new PartialUpdateAvroPayload(record4,2).recordBytes);

    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertEquals(record2, payload2.getInsertValue(schema).get());

    assertEquals(payload1.combineAndGetUpdateValue(record2, schema).get(), record3);
    assertEquals(payload2.combineAndGetUpdateValue(record1, schema).get(), record4);
  }

  @Test
  public void testDeletedRecord() throws IOException {
    GenericRecord record1 = new GenericData.Record(schema);
    record1.put("id", "1");
    record1.put("partition", "partition0");
    record1.put("ts", 0L);
    record1.put("_hoodie_is_deleted", false);
    record1.put("city", "NY0");
    record1.put("child", Collections.emptyList());

    GenericRecord delRecord1 = new GenericData.Record(schema);
    delRecord1.put("id", "2");
    delRecord1.put("partition", "partition1");
    delRecord1.put("ts", 1L);
    delRecord1.put("_hoodie_is_deleted", true);
    delRecord1.put("city", "NY0");
    delRecord1.put("child", Collections.emptyList());

    GenericRecord record2 = new GenericData.Record(schema);
    record2.put("id", "1");
    record2.put("partition", "partition0");
    record2.put("ts", 0L);
    record2.put("_hoodie_is_deleted", true);
    record2.put("city", "NY0");
    record2.put("child", Collections.emptyList());

    PartialUpdateAvroPayload payload1 = new PartialUpdateAvroPayload(record1, 1);
    PartialUpdateAvroPayload payload2 = new PartialUpdateAvroPayload(delRecord1, 2);

    assertEquals(payload1.preCombine(payload2), payload2);
    assertEquals(payload2.preCombine(payload1), payload2);

    assertEquals(record1, payload1.getInsertValue(schema).get());
    assertFalse(payload2.getInsertValue(schema).isPresent());

    assertEquals(payload1.combineAndGetUpdateValue(delRecord1, schema).get(), record1);
    assertFalse(payload2.combineAndGetUpdateValue(record1, schema).isPresent());
  }

  @Test
  public void testNullColumn() throws IOException {
    Schema avroSchema = Schema.createRecord(Arrays.asList(
            new Schema.Field("id", Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)), "", JsonProperties.NULL_VALUE),
            new Schema.Field("name", Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)), "", JsonProperties.NULL_VALUE),
            new Schema.Field("age", Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)), "", JsonProperties.NULL_VALUE),
            new Schema.Field("job", Schema.createUnion(Schema.create(Schema.Type.STRING), Schema.create(Schema.Type.NULL)), "", JsonProperties.NULL_VALUE)
    ));
    GenericRecord record1 = new GenericData.Record(avroSchema);
    record1.put("id", "1");
    record1.put("name", "aa");
    record1.put("age", "1");
    record1.put("job", "1");

    GenericRecord record2 = new GenericData.Record(avroSchema);
    record2.put("id", "1");
    record2.put("name", "bb");
    record2.put("age", "2");
    record2.put("job", null);

    GenericRecord record3 = new GenericData.Record(avroSchema);
    record3.put("id", "1");
    record3.put("name", "bb");
    record3.put("age", "2");
    record3.put("job", "1");

    OverwriteNonDefaultsWithLatestAvroPayload payload2 = new OverwriteNonDefaultsWithLatestAvroPayload(record2, 1);
    assertEquals(payload2.combineAndGetUpdateValue(record1, avroSchema).get(), record3);
  }
}
