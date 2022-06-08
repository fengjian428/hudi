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

package org.apache.hudi.index.hbase;

import org.apache.hudi.config.HoodieWriteConfig;

/**
 * Extends SparkHoodieHbaseIndex, add random prefix to key for avoiding data skew issue in hbase regions
 */
public class BalanceSparkHoodieHBaseIndex extends SparkHoodieHBaseIndex  {

  public BalanceSparkHoodieHBaseIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  protected String transformToHbaseKey(String originalKey) {
    int bucket = Math.abs(originalKey.hashCode()) % config.getHBaseIndexRegionCount();
    String bucketStr = String.format("%0" + String.valueOf(config.getHBaseIndexRegionCount() - 1).length() + "d", bucket);
    return bucketStr + originalKey;
  }
}
