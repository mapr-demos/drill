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
package org.apache.drill.exec.store.openTSDB;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.openTSDB.client.OpenTSDB;
import org.apache.drill.exec.store.openTSDB.client.OpenTSDBTypes;
import org.apache.drill.exec.store.openTSDB.client.Schema;
import org.apache.drill.exec.store.openTSDB.dto.ColumnDTO;
import org.apache.drill.exec.store.openTSDB.dto.Table;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.drill.exec.store.openTSDB.Util.isTableNameValid;
import static org.apache.drill.exec.store.openTSDB.Util.parseFROMRowData;

@Slf4j
public class OpenTSDBRecordReader extends AbstractRecordReader {

  /**
   * openTSDB required constants for API call
   */
  public static final String DEFAULT_TIME = "5y-ago";
  public static final String SUM_AGGREGATOR = "sum";

  private static final String TIME = "time";
  private static final String METRIC = "metric";
  private static final String AGGREGATOR = "aggregator";
  private static final String DOWNSAMPLE = "downsample";

  private static final Map<OpenTSDBTypes, MinorType> TYPES;

  private final OpenTSDB client;

  private List<Table> tables;

  private OutputMutator output;
  private OperatorContext context;

  private Iterator iterator;
  private List<String> showed = new ArrayList<>();

  private ImmutableList<ProjectedColumnInfo> projectedCols;

  private Map<String, String> queryParameters;

  private static class ProjectedColumnInfo {
    ValueVector vv;
    ColumnDTO openTSDBColumn;
  }

  static {
    TYPES = ImmutableMap.<OpenTSDBTypes, MinorType>builder()
        .put(OpenTSDBTypes.STRING, MinorType.VARCHAR)
        .put(OpenTSDBTypes.DOUBLE, MinorType.FLOAT8)
        .put(OpenTSDBTypes.TIMESTAMP, MinorType.TIMESTAMP)
        .build();
  }

  public OpenTSDBRecordReader(OpenTSDB client, OpenTSDBSubScan.OpenTSDBSubScanSpec subScanSpec,
                              List<SchemaPath> projectedColumns, FragmentContext context) {
    setColumns(projectedColumns);
    this.client = client;
    setupQueryParam(subScanSpec.getTableName());
    log.debug("Scan spec: {}", subScanSpec);
  }

  private void setupQueryParam(String data) {
    if (!isTableNameValid(data)) {
      queryParameters = parseFROMRowData(data);
    } else {
      queryParameters = new HashMap<>();
      queryParameters.put(METRIC, data);
    }
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.output = output;
    this.context = context;

    addColumnsNames();
    tables = getTablesFromDB();
    if (tables == null || tables.isEmpty()) {
      throw new ValidationError(String.format("Table '%s' not found or it's empty", queryParameters.get(METRIC)));
    }
  }

  @Override
  public int next() {
    if (isTablesListEmpty()) {
      iterator = null;
      return 0;
    }
    return processOpenTSDBTablesData();
  }

  @Override
  public void close() throws Exception {
  }

  private List<Table> getTablesFromDB() {
    try {
      String time =
          getProperty(TIME, DEFAULT_TIME);

      if (queryParameters.containsKey(DOWNSAMPLE)) {
        return client.getTableWithInterpolation(
            time,
            getAggregatorWithMetricName(),
            queryParameters.get(DOWNSAMPLE)).execute().body();
      }
      return client.getTable(time, getAggregatorWithMetricName()).execute().body();
    } catch (IOException e) {
      e.printStackTrace();
      return Collections.emptyList();
    }
  }

  private String getAggregatorWithMetricName() {
    String tableName =
        queryParameters.get(METRIC);

    String aggregator =
        getProperty(AGGREGATOR, SUM_AGGREGATOR);

    return aggregator + ":" + tableName;
  }

  private String getProperty(String propertyName, String defaultValue) {
    return queryParameters.containsKey(propertyName) ?
        queryParameters.get(propertyName) : defaultValue;
  }

  private int processOpenTSDBTablesData() {
    for (Table table : tables) {
      try {
        if (setupTimestampIterator(table)) {
          return 0;
        }

        addRowResult(table);
        iterator.next();

        setValueCountForMutator();
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
    return 1;
  }

  private boolean setupTimestampIterator(Table table) {
    while (iterator == null || !iterator.hasNext()) {
      if (iterator != null && !iterator.hasNext()) {
        iterator = null;
        return true;
      }
      context.getStats().startWait();
      iterator = table.getDps().keySet().iterator();
      context.getStats().stopWait();
    }
    return false;
  }

  private boolean isTablesListEmpty() {
    return tables.size() == 0;
  }

  private void addColumnsNames() {
    if (!isStarQuery()) {
      List<String> colNames = Lists.newArrayList();
      for (SchemaPath p : this.getColumns()) {
        colNames.add(p.getAsUnescapedPath());
      }
    }
  }

  private void setValueCountForMutator() {
    for (ProjectedColumnInfo pci : projectedCols) {
      pci.vv.getMutator().setValueCount(1);
    }
  }

  private void addRowResult(Table table) throws SchemaChangeException {
    setupProjectedColsIfItNull();

    String timestamp = null;
    String value = null;

    for (String time : table.getDps().keySet()) {
      if (!showed.contains(time)) {
        timestamp = time;
        value = table.getDps().get(time);
        showed.add(time);
        break;
      }
    }

    setupDataToDrillTable(table, timestamp, value, table.getTags());
  }

  private void setupProjectedColsIfItNull() throws SchemaChangeException {
    if (projectedCols == null) {
      initCols(new Schema(client, queryParameters.get("metric")));
    }
  }

  private void setupDataToDrillTable(Table table, String timestamp, String value, Map<String, String> tags) {
    for (ProjectedColumnInfo pci : projectedCols) {
      switch (pci.openTSDBColumn.getColumnName()) {
        case "metric":
          setStringColumnValue(table.getMetric(), pci);
          break;
        case "aggregate tags":
          setStringColumnValue(table.getAggregateTags().toString(), pci);
          break;
        case "timestamp":
          setTimestampColumnValue(Long.parseLong(timestamp), pci);
          break;
        case "aggregated value":
          setDoubleColumnValue(Double.parseDouble(value), pci);
          break;
        default:
          setStringColumnValue(tags.get(pci.openTSDBColumn.getColumnName()), pci);
      }
    }
  }

  private void setStringColumnValue(String data, ProjectedColumnInfo pci) {
    ByteBuffer value = ByteBuffer.wrap(data.getBytes(UTF_8));
    ((NullableVarCharVector.Mutator) pci.vv.getMutator())
        .setSafe(0, value, 0, value.remaining());
  }

  private void setTimestampColumnValue(Long data, ProjectedColumnInfo pci) {
    ((NullableTimeStampVector.Mutator) pci.vv.getMutator())
        .setSafe(0, data / 1000);
  }

  private void setDoubleColumnValue(Double data, ProjectedColumnInfo pci) {
    ((NullableFloat8Vector.Mutator) pci.vv.getMutator())
        .setSafe(0, data);
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