/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.openTSDB;

import com.google.common.base.Splitter;

import java.util.Map;

public class Util {

  /**
   * Parse FROM parameters to Map representation
   *
   * @param rowData with this syntax (metric=warp.speed.test)
   * @return Map with params key: metric, value: warp.speed.test
   */
  static Map<String, String> parseFROMRowData(String rowData) {
    String FROMRowData = rowData.replaceAll("[()]", "");
    return Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator("=").split(FROMRowData);
  }

  /**
   * @param name Metric name
   * @return Valid metric name
   */
  public static String getValidTableName(String name) {
    if (!isTableNameValid(name)) {
      name = parseFROMRowData(name).get("metric");
    }
    return name;
  }

  /**
   * @param name Metric name
   * @return true if name is valid
   */
  static boolean isTableNameValid(String name) {
    return !name.contains("=");
  }
}
