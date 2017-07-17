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
package org.apache.drill.exec.store.openTSDB.client.services;

import org.apache.drill.exec.store.openTSDB.client.OpenTSDB;
import org.apache.drill.exec.store.openTSDB.client.OpenTSDBTypes;
import org.apache.drill.exec.store.openTSDB.client.Service;
import org.apache.drill.exec.store.openTSDB.client.query.DBQuery;
import org.apache.drill.exec.store.openTSDB.client.query.Query;
import org.apache.drill.exec.store.openTSDB.dto.ColumnDTO;
import org.apache.drill.exec.store.openTSDB.dto.MetricDTO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.drill.exec.store.openTSDB.Constants.AGGREGATOR;
import static org.apache.drill.exec.store.openTSDB.Constants.DOWNSAMPLE;
import static org.apache.drill.exec.store.openTSDB.Constants.METRIC;
import static org.apache.drill.exec.store.openTSDB.Util.isTableNameValid;
import static org.apache.drill.exec.store.openTSDB.Util.parseFROMRowData;

public class ServiceImpl implements Service {

  private static final Logger log =
      LoggerFactory.getLogger(ServiceImpl.class);

  private OpenTSDB client;
  private Map<String, String> queryParameters;

  public ServiceImpl(String connectionURL) {
    this.client = new Retrofit.Builder()
        .baseUrl(connectionURL)
        .addConverterFactory(JacksonConverterFactory.create())
        .build()
        .create(OpenTSDB.class);
  }

  @Override
  public Set<MetricDTO> getAllMetrics() {
    return getAllMetricsByTags();
  }

  @Override
  public Set<String> getAllMetricNames() {
    return getTableNames();
  }

  @Override
  public List<ColumnDTO> getUnfixedColumns() {
    Set<MetricDTO> metrics = getAllMetricsByTags();
    List<ColumnDTO> unfixedColumns = new ArrayList<>();

    for (MetricDTO metric : metrics) {
      for (String tag : metric.getTags().keySet()) {
        ColumnDTO tmp = new ColumnDTO(tag, OpenTSDBTypes.STRING);
        if (!unfixedColumns.contains(tmp)) {
          unfixedColumns.add(tmp);
        }
      }
    }
    return unfixedColumns;
  }

  @Override
  public void setupQueryParameters(String rowData) {
    if (!isTableNameValid(rowData)) {
      this.queryParameters = parseFROMRowData(rowData);
    } else {
      Map<String, String> params = new HashMap<>();
      params.put(METRIC, rowData);
      this.queryParameters = params;
    }
  }

  private Set<MetricDTO> getAllMetricsByTags() {
    try {
      return getAllMetricsFromDBByTags();
    } catch (IOException e) {
      logIOException(e);
      return Collections.emptySet();
    }
  }

  private Set<String> getTableNames() {
    try {
      return client.getAllTablesName().execute().body();
    } catch (IOException e) {
      e.printStackTrace();
      return Collections.emptySet();
    }
  }

  private Set<MetricDTO> getMetricsByTags(DBQuery base) throws IOException {
    return client.getTables(base).execute().body();
  }

  private Set<MetricDTO> getAllMetricsFromDBByTags() throws IOException {
    Map<String, String> tags = new HashMap<>();
    DBQuery baseQuery = getConfiguredDbQuery(tags);

    Set<MetricDTO> metrics = getBaseMetric(baseQuery);
    Set<String> extractedTags = getTagsFromMetrics(metrics);

    return getMetricsByTags(extractedTags);
  }

  private Set<MetricDTO> getMetricsByTags(Set<String> extractedTags) throws IOException {
    Set<MetricDTO> metrics = new HashSet<>();
    for (String value : extractedTags) {
      metrics.addAll(getMetricsByTags(getConfiguredDbQuery(getTransformedTag(value))));
    }
    return metrics;
  }

  private DBQuery getConfiguredDbQuery(Map<String, String> tags) {
    Query subQuery = new Query.Builder(queryParameters.get(METRIC))
        .setAggregator(queryParameters.get(AGGREGATOR))
        .setDownsample(queryParameters.get(DOWNSAMPLE))
        .setTags(tags).build();

    Set<Query> queries = new HashSet<>();
    queries.add(subQuery);

    return new DBQuery.Builder()
        .setQueries(queries)
        .build();
  }

  private Set<MetricDTO> getBaseMetric(DBQuery base) throws IOException {
    return getMetricsByTags(base);
  }

  private Set<String> getTagsFromMetrics(Set<MetricDTO> metrics) {
    Set<String> extractedTags = new HashSet<>();

    for (MetricDTO table : metrics) {
      extractedTags.addAll(table.getAggregateTags());
      extractedTags.addAll(table.getTags().keySet());
    }

    return extractedTags;
  }

  private Map<String, String> getTransformedTag(String tag) {
    Map<String, String> tags = new HashMap<>();
    tags.put(tag, "*");
    return tags;
  }

  private void logIOException(IOException e) {
    log.warn("A problem occurred when talking to the server", e);
  }

}
