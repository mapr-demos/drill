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
import org.apache.drill.exec.store.openTSDB.client.query.BaseQuery;
import org.apache.drill.exec.store.openTSDB.client.query.Query;
import org.apache.drill.exec.store.openTSDB.dto.ColumnDTO;
import org.apache.drill.exec.store.openTSDB.dto.Table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.drill.exec.store.openTSDB.OpenTSDBRecordReader.DEFAULT_TIME;
import static org.apache.drill.exec.store.openTSDB.OpenTSDBRecordReader.SUM_AGGREGATOR;
import static org.apache.drill.exec.store.openTSDB.client.Schema.DefaultColumns.AGGREGATED_VALUE;
import static org.apache.drill.exec.store.openTSDB.client.Schema.DefaultColumns.AGGREGATE_TAGS;
import static org.apache.drill.exec.store.openTSDB.client.Schema.DefaultColumns.METRIC;
import static org.apache.drill.exec.store.openTSDB.client.Schema.DefaultColumns.TIMESTAMP;

// TODO: Refactor this class
@Slf4j
public class Schema {

  enum DefaultColumns {

    METRIC("metric"),
    TIMESTAMP("timestamp"),
    AGGREGATE_TAGS("aggregate tags"),
    AGGREGATED_VALUE("aggregated value");

    private String columnName;

    DefaultColumns(String name) {
      this.columnName = name;
    }

    @Override
    public String toString() {
      return columnName;
    }
  }

  private final List<ColumnDTO> columns = new ArrayList<>();
  private final OpenTSDB client;
  private final String metricName;

  public Schema(OpenTSDB client, String metricName) throws IOException {
    this.client = client;
    this.metricName = metricName;
    setupStructure();
  }

  public List<ColumnDTO> getColumns() throws IOException {
    return Collections.unmodifiableList(columns);
  }

  private void setupStructure() throws IOException {
    columns.add(new ColumnDTO(METRIC.toString(), OpenTSDBTypes.STRING));
    columns.add(new ColumnDTO(AGGREGATE_TAGS.toString(), OpenTSDBTypes.STRING));
    columns.add(new ColumnDTO(TIMESTAMP.toString(), OpenTSDBTypes.TIMESTAMP));
    columns.add(new ColumnDTO(AGGREGATED_VALUE.toString(), OpenTSDBTypes.DOUBLE));

    columns.addAll(getUnfixedColumns());
  }

  private List<ColumnDTO> getUnfixedColumns() throws IOException {
    Set<Table> tables = getBasicTable();

    BaseQuery base = new BaseQuery();

    Set<Query> queries = new HashSet<>();
    Query query = new Query();

    Set<String> extractedTags = new HashSet<>();
    Map<String, String> tags = new HashMap<>();

    base.setStart(DEFAULT_TIME);
    query.setAggregator(SUM_AGGREGATOR);
    query.setMetric(metricName);

    query.setTags(tags);
    queries.add(query);

    base.setQueries(queries);

    extractTags(tables, extractedTags);
    tables.clear();

    getAllMetrics(tables, base, extractedTags, tags);

    for (Table table : tables) {
      for (String tag : table.getTags().keySet()) {
        ColumnDTO tmp = new ColumnDTO(tag, OpenTSDBTypes.STRING);
        if (!columns.contains(tmp)) {
          columns.add(tmp);
        }
      }
    }

    return new LinkedList<>();
  }

  private void getAllMetrics(Set<Table> tables, BaseQuery base, Set<String> taguni, Map<String, String> tags) throws IOException {
    for (String value : taguni) {
      tags.clear();
      tags.put(value, "*");
      tables.addAll(client.getTables(base).execute().body());
    }
  }

  private void extractTags(Set<Table> tables, Set<String> taguni) {
    for (Table table : tables) {
      taguni.addAll(table.getAggregateTags());
      taguni.addAll(table.getTags().keySet());
    }
  }

  private Set<Table> getBasicTable() throws IOException {
    return client.getTable(DEFAULT_TIME, SUM_AGGREGATOR + ":" + metricName).execute().body();
  }

  public int getColumnCount() {
    return columns.size();
  }

  public ColumnDTO getColumnByIndex(int columnIndex) {
    return columns.get(columnIndex);
  }
}
