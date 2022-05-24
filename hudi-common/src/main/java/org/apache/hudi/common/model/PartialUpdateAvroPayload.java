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
import org.apache.hudi.common.util.StringUtils;

import java.io.IOException;
import java.util.Properties;

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

    public PartialUpdateAvroPayload(GenericRecord record, Comparable orderingVal) {
        super(record, orderingVal);
    }

    public PartialUpdateAvroPayload(Option<GenericRecord> record) {
        super(record); // natural order
    }

    @Override
    public PartialUpdateAvroPayload preCombine(OverwriteWithLatestAvroPayload oldValue, Schema schema) {
        if (oldValue.recordBytes.length == 0) {
            // use natural order for delete record
            return this;
        }
        boolean isBaseRecord = false;
        if (oldValue.orderingVal.compareTo(orderingVal) > 0) {
            // pick the payload with greatest ordering value as insert record
            isBaseRecord = true;
        }
        try {
            GenericRecord indexedOldValue = (GenericRecord) oldValue.getInsertValue(schema).get();
            Option<IndexedRecord> optValue = combineAndGetUpdateValue(indexedOldValue, schema, isBaseRecord);
            if (optValue.isPresent()) {
                return new PartialUpdateAvroPayload((GenericRecord) optValue.get(),
                        isBaseRecord ? oldValue.orderingVal : this.orderingVal);
            }
        } catch (Exception ex) {
            return this;
        }
        return this;
    }

    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, boolean isBaseRecord) throws IOException {
        Option<IndexedRecord> recordOption = getInsertValue(schema);

        if (!recordOption.isPresent()) {
            return Option.empty();
        }

        GenericRecord insertRecord;
        GenericRecord currentRecord;
        if (isBaseRecord) {
            insertRecord = (GenericRecord) currentValue;
            currentRecord = (GenericRecord) recordOption.get();
        } else {
            insertRecord = (GenericRecord) recordOption.get();
            currentRecord = (GenericRecord) currentValue;
        }

        return getMergedIndexedRecordOption(schema, insertRecord, currentRecord);
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
        return this.combineAndGetUpdateValue(currentValue,schema,false);
    }

    @Override
    public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema, Properties prop) throws IOException {
        String orderingField = prop.getProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY);
        boolean isBaseRecord = false;

        if (!StringUtils.isNullOrEmpty(orderingField)) {
            String oldOrderingVal = HoodieAvroUtils.getNestedFieldValAsString(
                    (GenericRecord) currentValue, orderingField, false, false);
            if (oldOrderingVal.compareTo(orderingVal.toString()) > 0) {
                // pick the payload with greatest ordering value as insert record
                isBaseRecord = true;
            }
        }
        return combineAndGetUpdateValue(currentValue, schema, isBaseRecord);
    }

    /**
     * Return true if value equals defaultValue otherwise false.
     */
    public Boolean overwriteField(Object value, Object defaultValue) {
        return value == null;
    }
}
