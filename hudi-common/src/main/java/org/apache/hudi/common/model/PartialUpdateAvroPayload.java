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
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * subclass of OverwriteWithLatestAvroPayload used for delta streamer.
 *
 * <ol>
 * <li>preCombine - Picks the latest delta record for a key, based on an ordering field;
 * <li>combineAndGetUpdateValue/getInsertValue - overwrite storage for specified fields
 * that doesn't equal defaultValue.
 * </ol>
 */
public class PartialUpdateAvroPayload extends OverwriteWithLatestAvroPayload {

  public PartialUpdateAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public PartialUpdateAvroPayload(Option<GenericRecord> record) {
    super(record); // natural order
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {

    Option<IndexedRecord> recordOption = getInsertValue(schema);
    if (!recordOption.isPresent()) {
      return Option.empty();
    }

    GenericRecord insertRecord = (GenericRecord) recordOption.get();
    GenericRecord currentRecord = (GenericRecord) currentValue;

    if (isDeleteRecord(insertRecord)) {
      return Option.empty();
    } else {
      List<Schema.Field> fields = schema.getFields();
      fields.forEach(field -> {
        Object value = insertRecord.get(field.name());
        value = field.schema().getType().equals(Schema.Type.STRING) && value != null ? value.toString() : value;
        Object defaultValue = field.defaultVal();
        if (!overwriteField(value, defaultValue)) {
////          if(field.schema().getType().equals(Schema.Type.ARRAY)){
////            List<Object> oldArray = (List<Object>) currentRecord.get(field.name());
////            List<Object> insertArray = (List<Object>) value;
////            List<Object> mergeList =new ArrayList<>();
////            mergeList.addAll(oldArray);
////            mergeList.addAll(insertArray);
////            currentRecord.put(field.name(), mergeList);
////          }
////          else{
            currentRecord.put(field.name(), value);
//          }
        }
      });
      return Option.of(currentRecord);
    }
  }

  /**
   * Return true if value equals defaultValue otherwise false.
   */
  public Boolean overwriteField(Object value, Object defaultValue) {
     return value == null;
  }
}
