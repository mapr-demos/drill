/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.openTSDB.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.drill.exec.store.openTSDB.dto.ColumnDTO;
import org.apache.drill.exec.store.openTSDB.dto.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.drill.exec.store.openTSDB.OpenTSDBRecordReader.DEFAULT_TIME;
import static org.apache.drill.exec.store.openTSDB.OpenTSDBRecordReader.SUM_AGGREGATOR;

// TODO: Refactor this class
@Slf4j
public class Schema {

  private final List<ColumnDTO> columns = new ArrayList<>();
  private final OpenTSDB client;
  private final String metricName;

  public Schema(OpenTSDB client, String metricName) {
    this.client = client;
    this.metricName = metricName;
    setupStructure();
  }

  // TODO: refactor this
  public List<ColumnDTO> getColumns() throws IOException {
    return Collections.unmodifiableList(columns);
  }

  private void setupStructure() {
    columns.add(new ColumnDTO("metric", OpenTSDBTypes.STRING));
    columns.add(new ColumnDTO("aggregate tags", OpenTSDBTypes.STRING));
    columns.add(new ColumnDTO("timestamp", OpenTSDBTypes.TIMESTAMP));
    columns.add(new ColumnDTO("aggregated value", OpenTSDBTypes.DOUBLE));
    List<Table> res = null;
    try {
      res = client.getTable(DEFAULT_TIME, SUM_AGGREGATOR + ":" + metricName).execute().body();
    } catch (IOException e) {
      e.printStackTrace();
    }
    if (res != null) {
      for (Table table : res) {
        for (String tag : table.getTags().keySet()) {
          columns.add(new ColumnDTO(tag, OpenTSDBTypes.STRING));
        }
      }
    }
  }

  public int getColumnCount() {
    return columns.size();
  }

  public ColumnDTO getColumnByIndex(int columnIndex) {
    return columns.get(columnIndex);
  }
}
