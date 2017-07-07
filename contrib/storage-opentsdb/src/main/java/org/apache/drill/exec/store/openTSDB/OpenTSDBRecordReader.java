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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.logical.ValidationError;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.openTSDB.client.OpenTSDB;
import org.apache.drill.exec.store.openTSDB.client.OpenTSDBTypes;
import org.apache.drill.exec.store.openTSDB.client.Schema;
import org.apache.drill.exec.store.openTSDB.client.query.DBQuery;
import org.apache.drill.exec.store.openTSDB.client.query.Query;
import org.apache.drill.exec.store.openTSDB.dto.ColumnDTO;
import org.apache.drill.exec.store.openTSDB.dto.MetricDTO;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.drill.exec.store.openTSDB.Constants.AGGREGATOR;
import static org.apache.drill.exec.store.openTSDB.Constants.DEFAULT_TIME;
import static org.apache.drill.exec.store.openTSDB.Constants.DOWNSAMPLE;
import static org.apache.drill.exec.store.openTSDB.Constants.METRIC;
import static org.apache.drill.exec.store.openTSDB.Constants.SUM_AGGREGATOR;
import static org.apache.drill.exec.store.openTSDB.Constants.TIME;
import static org.apache.drill.exec.store.openTSDB.Util.isTableNameValid;
import static org.apache.drill.exec.store.openTSDB.Util.parseFROMRowData;

public class OpenTSDBRecordReader extends AbstractRecordReader {

  private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(OpenTSDBRecordReader.class);

  private static final Map<OpenTSDBTypes, MinorType> TYPES;

  private final OpenTSDB client;

  private Iterator<MetricDTO> tableIterator;
  private OutputMutator output;
  private ImmutableList<ProjectedColumnInfo> projectedCols;
  private Map<String, String> queryParameters;

  OpenTSDBRecordReader(OpenTSDB client, OpenTSDBSubScan.OpenTSDBSubScanSpec subScanSpec,
                       List<SchemaPath> projectedColumns) throws IOException {
    setColumns(projectedColumns);
    this.client = client;
    queryParameters = setupQueryParam(subScanSpec.getTableName());
    log.debug("Scan spec: {}", subScanSpec);
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.output = output;

    Set<MetricDTO> tables = getTablesFromDB();
    if (tables == null || tables.isEmpty()) {
      throw new ValidationError(String.format("Table '%s' not found or it's empty", queryParameters.get(METRIC)));
    }
    this.tableIterator = tables.iterator();
  }

  @Override
  public int next() {
    try {
      return processOpenTSDBTablesData();
    } catch (SchemaChangeException e) {
      return 0;
    }
  }

  @Override
  protected boolean isSkipQuery() {
    return super.isSkipQuery();
  }

  @Override
  public void close() throws Exception {
  }

  static {
    TYPES = ImmutableMap.<OpenTSDBTypes, MinorType>builder()
        .put(OpenTSDBTypes.STRING, MinorType.VARCHAR)
        .put(OpenTSDBTypes.DOUBLE, MinorType.FLOAT8)
        .put(OpenTSDBTypes.TIMESTAMP, MinorType.TIMESTAMP)
        .build();
  }

  private static class ProjectedColumnInfo {
    ValueVector vv;
    ColumnDTO openTSDBColumn;
  }

  private Map<String, String> setupQueryParam(String data) {
    if (!isTableNameValid(data)) {
      return parseFROMRowData(data);
    } else {
      Map<String, String> params = new HashMap<>();
      params.put(METRIC, data);
      return params;
    }
  }

  private Set<MetricDTO> getTablesFromDB() {
    try {
      return getAllMetricsFromDBByTags();
    } catch (IOException e) {
      log.warn("A problem occurred when talking to the server", e);
      return Collections.emptySet();
    }
  }

  private Set<MetricDTO> getAllMetricsFromDBByTags() throws IOException {
    Map<String, String> tags = new HashMap<>();
    DBQuery baseQuery = new DBQuery();
    Query subQuery = new Query();

    setupBaseQuery(baseQuery, subQuery, tags);

    Set<MetricDTO> tables =
        getBaseTables(baseQuery);

    Set<String> extractedTags =
        getTagsFromTables(tables);

    tables.clear();

    for (String value : extractedTags) {
      transformTagsForRequest(tags, value);
      tables.addAll(getAllTablesWithSpecialTag(baseQuery));
    }
    return tables;
  }

  private Set<MetricDTO> getAllTablesWithSpecialTag(DBQuery base) throws IOException {
    return client.getTables(base).execute().body();
  }

  private Set<MetricDTO> getBaseTables(DBQuery base) throws IOException {
    return getAllTablesWithSpecialTag(base);
  }

  private void transformTagsForRequest(Map<String, String> tags, String value) {
    tags.clear();
    tags.put(value, "*");
  }

  private void setupBaseQuery(DBQuery base, Query query, Map<String, String> tags) {
    setStartTimeInDBBaseQuery(base);
    setupSubQuery(tags, query);
    addSubQueryToBaseQuery(base, query);
  }

  private void setupSubQuery(Map<String, String> tags, Query subQuery) {
    setAggregatorInQuery(subQuery);
    setMetricNameInQuery(subQuery);
    setDownsampleInQuery(subQuery);
    setEmptyTagMapInQuery(subQuery, tags);
  }

  private void addSubQueryToBaseQuery(DBQuery base, Query subQuery) {
    Set<Query> queries = new HashSet<>();
    queries.add(subQuery);

    base.setQueries(queries);
  }

  private void setEmptyTagMapInQuery(Query subQuery, Map<String, String> tags) {
    subQuery.setTags(tags);
  }

  private void setDownsampleInQuery(Query subQuery) {
    if (queryParameters.containsKey(DOWNSAMPLE)) {
      subQuery.setDownsample(queryParameters.get(DOWNSAMPLE));
    }
  }

  private void setMetricNameInQuery(Query subQuery) {
    subQuery.setMetric(queryParameters.get(METRIC));
  }

  private void setAggregatorInQuery(Query subQuery) {
    subQuery.setAggregator(getProperty(AGGREGATOR, SUM_AGGREGATOR));
  }

  private void setStartTimeInDBBaseQuery(DBQuery base) {
    base.setStart(getProperty(TIME, DEFAULT_TIME));
  }

  private Set<String> getTagsFromTables(Set<MetricDTO> tables) {
    Set<String> extractedTags = new HashSet<>();

    for (MetricDTO table : tables) {
      extractedTags.addAll(table.getAggregateTags());
      extractedTags.addAll(table.getTags().keySet());
    }

    return extractedTags;
  }

  private String getProperty(String propertyName, String defaultValue) {
    return queryParameters.containsKey(propertyName) ?
        queryParameters.get(propertyName) : defaultValue;
  }

  private int processOpenTSDBTablesData() throws SchemaChangeException {
    int rowCounter = 0;
    while (tableIterator.hasNext()) {
      MetricDTO metricDTO = tableIterator.next();
      rowCounter = addRowResult(metricDTO, rowCounter);
    }
    return rowCounter;
  }

  private int addRowResult(MetricDTO table, int rowCounter) throws SchemaChangeException {
    setupProjectedColsIfItNull();
    for (String time : table.getDps().keySet()) {
      String value = table.getDps().get(time);
      setupDataToDrillTable(table, time, value, table.getTags(), rowCounter);
      rowCounter++;
    }
    return rowCounter;
  }

  private void setupProjectedColsIfItNull() throws SchemaChangeException {
    if (projectedCols == null) {
      initCols(new Schema(client, queryParameters.get("metric")));
    }
  }

  private void setupDataToDrillTable(MetricDTO table, String timestamp, String value, Map<String, String> tags, int rowCount) {
    for (ProjectedColumnInfo pci : projectedCols) {
      switch (pci.openTSDBColumn.getColumnName()) {
        case "metric":
          setStringColumnValue(table.getMetric(), pci, rowCount);
          break;
        case "aggregate tags":
          setStringColumnValue(table.getAggregateTags().toString(), pci, rowCount);
          break;
        case "timestamp":
          setTimestampColumnValue(timestamp, pci, rowCount);
          break;
        case "aggregated value":
          setDoubleColumnValue(value, pci, rowCount);
          break;
        default:
          setStringColumnValue(tags.get(pci.openTSDBColumn.getColumnName()), pci, rowCount);
      }
    }
  }

  private void setTimestampColumnValue(String timestamp, ProjectedColumnInfo pci, int rowCount) {
    setTimestampColumnValue(timestamp != null ? Long.parseLong(timestamp) : Long.parseLong("0"), pci, rowCount);
  }

  private void setDoubleColumnValue(String value, ProjectedColumnInfo pci, int rowCount) {
    setDoubleColumnValue(value != null ? Double.parseDouble(value) : 0.0, pci, rowCount);
  }

  private void setStringColumnValue(String data, ProjectedColumnInfo pci, int rowCount) {
    if (data == null) {
      data = "null";
    }
    ByteBuffer value = ByteBuffer.wrap(data.getBytes(UTF_8));
    ((NullableVarCharVector.Mutator) pci.vv.getMutator())
        .setSafe(rowCount, value, 0, value.remaining());
  }

  private void setTimestampColumnValue(Long data, ProjectedColumnInfo pci, int rowCount) {
    ((NullableTimeStampVector.Mutator) pci.vv.getMutator())
        .setSafe(rowCount, data * 1000);
  }

  private void setDoubleColumnValue(Double data, ProjectedColumnInfo pci, int rowCount) {
    ((NullableFloat8Vector.Mutator) pci.vv.getMutator())
        .setSafe(rowCount, data);
  }

  private void initCols(Schema schema) throws SchemaChangeException {
    ImmutableList.Builder<ProjectedColumnInfo> pciBuilder = ImmutableList.builder();

    for (int i = 0; i < schema.getColumnCount(); i++) {

      ColumnDTO column = schema.getColumnByIndex(i);
      final String name = column.getColumnName();
      final OpenTSDBTypes type = column.getColumnType();
      TypeProtos.MinorType minorType = TYPES.get(type);

      if (isMinorTypeNull(minorType)) {
        logExceptionMessage(name, type);
        continue;
      }

      ProjectedColumnInfo pci = getProjectedColumnInfo(column, name, minorType);
      pciBuilder.add(pci);
    }
    projectedCols = pciBuilder.build();
  }

  private boolean isMinorTypeNull(MinorType minorType) {
    return minorType == null;
  }

  private ProjectedColumnInfo getProjectedColumnInfo(ColumnDTO column, String name, MinorType minorType) throws SchemaChangeException {
    MajorType majorType = getMajorType(minorType);

    MaterializedField field =
        MaterializedField.create(name, majorType);

    ValueVector vector =
        getValueVector(minorType, majorType, field);

    return getProjectedColumnInfo(column, vector);
  }

  private void logExceptionMessage(String name, OpenTSDBTypes type) {
    log.warn("Ignoring column that is unsupported.", UserException
        .unsupportedError()
        .message(
            "A column you queried has a data type that is not currently supported by the OpenTSDB storage plugin. "
                + "The column's name was %s and its OpenTSDB data type was %s. ",
            name, type.toString())
        .addContext("column Name", name)
        .addContext("plugin", "openTSDB")
        .build(log));
  }

  private MajorType getMajorType(MinorType minorType) {
    MajorType majorType;
    majorType = Types.optional(minorType);
    return majorType;
  }

  private ValueVector getValueVector(MinorType minorType, MajorType majorType, MaterializedField field) throws SchemaChangeException {
    final Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(
        minorType, majorType.getMode());
    ValueVector vector = output.addField(field, clazz);
    vector.allocateNew();
    return vector;
  }

  private ProjectedColumnInfo getProjectedColumnInfo(ColumnDTO column, ValueVector vector) {
    ProjectedColumnInfo pci = new ProjectedColumnInfo();
    pci.vv = vector;
    pci.openTSDBColumn = column;
    return pci;
  }
}