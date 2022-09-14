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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * subclass of OverwriteNonDefaultsWithLatestAvroPayload used for delta streamer.
 *
 * <ol>
 * <li>preCombine - Picks the latest delta record for a key, based on an ordering field;
 * <li>combineAndGetUpdateValue/getInsertValue - overwrite storage for specified fields
 * that doesn't equal defaultValue.
 * </ol>
 */
public class PartialUpdateAvroPayload extends OverwriteNonDefaultsWithLatestAvroPayload {

  public static ConcurrentHashMap<String, Schema> schemaRepo = new ConcurrentHashMap<>();
  private boolean isPreCombining = false;

  public PartialUpdateAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public PartialUpdateAvroPayload(Option<GenericRecord> record) {
    super(record); // natural order
  }

  @Override
  public PartialUpdateAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue, Properties properties) {
    isPreCombining = true;
    String schemaStringIn = properties.getProperty("schema");
    Schema schemaInstance;
    if (!schemaRepo.containsKey(schemaStringIn)) {
      schemaInstance = new Schema.Parser().parse(schemaStringIn);
      schemaRepo.put(schemaStringIn, schemaInstance);
    } else {
      schemaInstance = schemaRepo.get(schemaStringIn);
    }
    if (oldValue.recordBytes.length == 0) {
      // use natural order for delete record
      return this;
    }

    try {
      GenericRecord indexedOldValue = HoodieAvroUtils.bytesToAvro(oldValue.recordBytes, schemaInstance);
      Option<IndexedRecord> optValue = combineAndGetUpdateValue(indexedOldValue, schemaInstance, this.orderingVal.toString());
      if (optValue.isPresent()) {
        return new PartialUpdateAvroPayload((GenericRecord) optValue.get(), this.orderingVal.toString());
      }
    } catch (Exception ex) {
      return this;
    } finally {
      isPreCombining = false;
    }
    return this;
  }

  public Option<IndexedRecord> combineAndGetUpdateValue(
      IndexedRecord currentValue, Schema schema, String multipleOrderingFieldsWithCols) throws IOException {
    Option<IndexedRecord> incomingRecord = getInsertValue(schema);
    if (!incomingRecord.isPresent()) {
      return Option.empty();
    }

    // Perform a deserialization again to prevent resultRecord from sharing the same reference as recordOption
    GenericRecord resultRecord = (GenericRecord) getInsertValue(schema).get();

    Map<String, Schema.Field> name2Field = schema.getFields().stream().collect(Collectors.toMap(Schema.Field::name, item -> item));
    // multipleOrderingFieldsWithCols = _ts1:name1,price1=999;_ts2:name2,price2=;

    MultipleOrderingVal2ColsInfo multipleOrderingVal2ColsInfo = new MultipleOrderingVal2ColsInfo(multipleOrderingFieldsWithCols, (GenericRecord) incomingRecord.get());
    final Boolean[] deleteFlag = new Boolean[1];
    deleteFlag[0] = false;
    multipleOrderingVal2ColsInfo.getOrderingVal2ColsInfoList().stream().forEach(orderingVal2ColsInfo -> {
      Comparable persistOrderingVal = (Comparable) HoodieAvroUtils.getNestedFieldVal(
          (GenericRecord) currentValue, orderingVal2ColsInfo.getOrderingField(), true, false);

      // No update required
      if (persistOrderingVal == null && orderingVal2ColsInfo.getOrderingField().isEmpty()) {
        return;
      }

      // Pick the payload with greatest ordering value as insert record
      boolean needUpdatePersistData = false;
      if (persistOrderingVal == null || (orderingVal2ColsInfo.getOrderingValue() != null && persistOrderingVal.compareTo(orderingVal2ColsInfo.getOrderingValue()) <= 0)) {
        needUpdatePersistData = true;
      }

      // Initialise the fields of the sub-tables
      GenericRecord insertRecord;
      if (!needUpdatePersistData) {
        insertRecord = (GenericRecord) currentValue;
        // resultRecord is already assigned as recordOption
        orderingVal2ColsInfo.getColumnNames().stream()
            .filter(fieldName -> name2Field.containsKey(fieldName))
            .forEach(fieldName -> resultRecord.put(fieldName, insertRecord.get(fieldName)));
        resultRecord.put(orderingVal2ColsInfo.getOrderingField(), persistOrderingVal);
      } else {
        insertRecord = (GenericRecord) incomingRecord.get();
        orderingVal2ColsInfo.getColumnNames().stream()
            .filter(fieldName -> name2Field.containsKey(fieldName))
            .forEach(fieldName -> resultRecord.put(fieldName, insertRecord.get(fieldName)));
      }

      // If any of the sub-table records is flagged for deletion, delete entire row
      if (isDeleteRecord(insertRecord)) {
        deleteFlag[0] = true;
      }
    });

    if (deleteFlag[0]) {
      resultRecord.put("_hoodie_is_deleted", true);
      if (!isPreCombining) {
        return Option.empty();
      }
    }
    return Option.of(resultRecord);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    return this.combineAndGetUpdateValue(currentValue, schema, this.orderingVal.toString());
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties prop) throws IOException {
    Option<IndexedRecord> recordOption = getInsertValue(schema);
    if (!recordOption.isPresent()) {
      return Option.empty();
    }
    return combineAndGetUpdateValue(currentValue, schema, this.orderingVal.toString());
  }

  private static String getOrderingValForOneOrderingField(String newOrderingValWithMapping) {
    String[] newOrderingValWithMappingArr = newOrderingValWithMapping.split(":.*=");
    return newOrderingValWithMappingArr.length > 1 ? newOrderingValWithMappingArr[1] : "";
  }

  /**
   * Return true if value equals defaultValue otherwise false.
   */
  public Boolean overwriteField(Object value, Object defaultValue) {
    return value == null;
  }
}
