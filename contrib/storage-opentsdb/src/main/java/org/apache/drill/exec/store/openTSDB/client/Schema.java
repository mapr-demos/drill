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

import org.apache.drill.exec.store.openTSDB.client.query.DBQuery;
import org.apache.drill.exec.store.openTSDB.client.query.Query;
import org.apache.drill.exec.store.openTSDB.dto.ColumnDTO;
import org.apache.drill.exec.store.openTSDB.dto.MetricDTO;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.drill.exec.store.openTSDB.OpenTSDBRecordReader.DEFAULT_TIME;
import static org.apache.drill.exec.store.openTSDB.OpenTSDBRecordReader.SUM_AGGREGATOR;
import static org.apache.drill.exec.store.openTSDB.client.Schema.DefaultColumns.AGGREGATED_VALUE;
import static org.apache.drill.exec.store.openTSDB.client.Schema.DefaultColumns.AGGREGATE_TAGS;
import static org.apache.drill.exec.store.openTSDB.client.Schema.DefaultColumns.METRIC;
import static org.apache.drill.exec.store.openTSDB.client.Schema.DefaultColumns.TIMESTAMP;

/**
 * Abstraction for representing structure of openTSDB table
 */
public class Schema {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Schema.class);
  private final List<ColumnDTO> columns = new ArrayList<>();
  private final OpenTSDB client;
  private final String metricName;

  public Schema(OpenTSDB client, String metricName) {
    this.client = client;
    this.metricName = metricName;
    setupStructure();
  }

  private void setupStructure() {
    try {
      columns.add(new ColumnDTO(METRIC.toString(), OpenTSDBTypes.STRING));
      columns.add(new ColumnDTO(AGGREGATE_TAGS.toString(), OpenTSDBTypes.STRING));
      columns.add(new ColumnDTO(TIMESTAMP.toString(), OpenTSDBTypes.TIMESTAMP));
      columns.add(new ColumnDTO(AGGREGATED_VALUE.toString(), OpenTSDBTypes.DOUBLE));
      addUnfixedColumnsToSchema();
    } catch (IOException ie) {
      log.warn("A problem occurred when talking to the server", ie);
    }
  }

  /**
   * Return list with all columns names and its types
   *
   * @return List<ColumnDTO>
   */
  public List<ColumnDTO> getColumns() {
    return Collections.unmodifiableList(columns);
  }

  /**
   * Number of columns in table
   *
   * @return number of table columns
   */
  public int getColumnCount() {
    return columns.size();
  }

  /**
   * @param columnIndex index of required column in table
   * @return ColumnDTO
   */
  public ColumnDTO getColumnByIndex(int columnIndex) {
    return columns.get(columnIndex);
  }

  private void addUnfixedColumnsToSchema() throws IOException {
    Set<MetricDTO> tables = getBasicTable();
    findAllUnfixedColumns(tables);

    for (MetricDTO table : tables) {
      for (String tag : table.getTags().keySet()) {
        ColumnDTO tmp = new ColumnDTO(tag, OpenTSDBTypes.STRING);
        if (!columns.contains(tmp)) {
          columns.add(tmp);
        }
      }
    }
  }

  //void methods must not do actions with parameters using object link
  //to find such problems make all DTOs immutable
  private void findAllUnfixedColumns(Set<MetricDTO> tables) throws IOException {
    DBQuery base = new DBQuery();
    Query subQuery = new Query();

    Set<Query> queries = new HashSet<>();

    Set<String> extractedTags = new HashSet<>();
    Map<String, String> tags = new HashMap<>();

    base.setStart(DEFAULT_TIME);
    setupSubQuery(subQuery, tags);
    queries.add(subQuery);

    base.setQueries(queries);

    extractTags(tables, extractedTags);
    tables.clear();

    getAllMetrics(tables, base, extractedTags, tags);
  }

  private void setupSubQuery(Query subQuery, Map<String, String> tags) {
    subQuery.setAggregator(SUM_AGGREGATOR);
    subQuery.setMetric(metricName);
    subQuery.setTags(tags);
  }

  private void getAllMetrics(Set<MetricDTO> tables, DBQuery base,
                             Set<String> tagNames, Map<String, String> tags) throws IOException {
    for (String value : tagNames) {
      tags.clear();
      tags.put(value, "*");
      tables.addAll(client.getTables(base).execute().body());
    }
  }

  private void extractTags(Set<MetricDTO> tables, Set<String> taguni) {
    for (MetricDTO table : tables) {
      taguni.addAll(table.getAggregateTags());
      taguni.addAll(table.getTags().keySet());
    }
  }

  private Set<MetricDTO> getBasicTable() throws IOException {
    return client.getTables(DEFAULT_TIME, SUM_AGGREGATOR + ":" + metricName).execute().body();
  }

  /**
   * Structure with constant openTSDB columns
   */
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
}
