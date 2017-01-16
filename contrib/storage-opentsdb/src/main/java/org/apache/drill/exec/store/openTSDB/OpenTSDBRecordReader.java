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
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.ExecConstants;
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
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.ValueVector;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Slf4j
public class OpenTSDBRecordReader extends AbstractRecordReader {

    /**
     * openTSDB required constants for API call
     */
    private static final String TIME = "5y-ago";
    private static final String SUM_AGGREGATOR = "sum:";

    private final OpenTSDB client;
    private final OpenTSDBSubScan.OpenTSDBSubScanSpec scanSpec;

    private final boolean unionEnabled;

    private List<Table> tables;

    private OutputMutator output;
    private OperatorContext context;

    private Iterator iterator;
    private List<String> showed = new ArrayList<>();

    private static class ProjectedColumnInfo {
        int index;
        ValueVector vv;
        ColumnDTO openTSDBColumn;
    }

    private ImmutableList<ProjectedColumnInfo> projectedCols;

    public OpenTSDBRecordReader(OpenTSDB client, OpenTSDBSubScan.OpenTSDBSubScanSpec subScanSpec,
                                List<SchemaPath> projectedColumns, FragmentContext context) {
        setColumns(projectedColumns);
        this.client = client;
        scanSpec = subScanSpec;
        unionEnabled = context.getOptions().getOption(ExecConstants.ENABLE_UNION_TYPE);
        log.debug("Scan spec: {}", subScanSpec);
    }

    @Override
    public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
        this.output = output;
        this.context = context;

        if (!isStarQuery()) {
            List<String> colNames = Lists.newArrayList();
            for (SchemaPath p : this.getColumns()) {
                colNames.add(p.getAsUnescapedPath());
            }
        }
        try {
            tables = client.getTable(TIME, SUM_AGGREGATOR + scanSpec.getTableName())
                    .execute()
                    .body();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static final Map<OpenTSDBTypes, MinorType> TYPES;

    static {
        TYPES = ImmutableMap.<OpenTSDBTypes, MinorType>builder()
                .put(OpenTSDBTypes.STRING, MinorType.VARCHAR)
                .build();
    }

    @Override
    public int next() {
        if (isTablesListEmpty()) return 0;

        try {
            while (iterator == null || !iterator.hasNext()) {
                if (iterator != null && !iterator.hasNext()) {
                    iterator = null;
                    return 0;
                }
                context.getStats().startWait();
                iterator = tables.get(0).getDps().keySet().iterator();
                context.getStats().stopWait();
            }

            addRowResult(tables.get(0));
            iterator.next();

            setValueCountForMutator();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return 1;
    }

    @Override
    public void close() throws Exception {

    }

    private boolean isTablesListEmpty() {
        if (tables.size() == 0) {
            iterator = null;
            return true;
        }
        return false;
    }

    private void setValueCountForMutator() {
        for (ProjectedColumnInfo pci : projectedCols) {
            pci.vv.getMutator().setValueCount(1);
        }
    }

    private void addRowResult(Table table) throws SchemaChangeException {
        if (projectedCols == null) {
            initCols(new Schema());
        }
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

        setupDataToDrillTable(table, timestamp, value);
    }

    private void setupDataToDrillTable(Table table, String timestamp, String value) {
        for (ProjectedColumnInfo pci : projectedCols) {
            switch (pci.openTSDBColumn.getColumnName()) {
                case "metric":
                    setColumnValue(table.getMetric(), pci);
                    break;
                case "tags":
                    setColumnValue(table.getTags().toString(), pci);
                    break;
                case "aggregate tags":
                    setColumnValue(table.getAggregateTags().toString(), pci);
                    break;
                case "timestamp":
                    Calendar calendar = Calendar.getInstance();
                    calendar.setTimeInMillis(Long.parseLong(timestamp));
                    setColumnValue(new SimpleDateFormat("HH:mm:ss:SSS").format(calendar.getTime()), pci);
                    break;
                case "aggregated value":
                    setColumnValue(value, pci);
                    break;
            }
        }
    }

    private void setColumnValue(String data, ProjectedColumnInfo pci) {
        ByteBuffer value = ByteBuffer.wrap(data.getBytes());
        ((NullableVarCharVector.Mutator) pci.vv.getMutator())
                .setSafe(0, value, 0, value.remaining());
    }


    private void initCols(Schema schema) throws SchemaChangeException {
        ImmutableList.Builder<ProjectedColumnInfo> pciBuilder = ImmutableList.builder();

        for (int i = 0; i < schema.getColumnCount(); i++) {

            ColumnDTO column = schema.getColumnByIndex(i);
            final String name = column.getColumnName();
            final OpenTSDBTypes type = column.getColumnType();
            TypeProtos.MinorType minorType = TYPES.get(type);

            if (minorType == null) {
                logExceptionMessage(name, type);
                continue;
            }

            MajorType majorType = getMajorType(minorType);

            MaterializedField field =
                    MaterializedField.create(name, majorType);

            ValueVector vector =
                    getValueVector(minorType, majorType, field);

            ProjectedColumnInfo pci =
                    getProjectedColumnInfo(i, column, vector);

            pciBuilder.add(pci);
        }
        projectedCols = pciBuilder.build();
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

    private ProjectedColumnInfo getProjectedColumnInfo(int columnNumber, ColumnDTO column, ValueVector vector) {
        ProjectedColumnInfo pci = new ProjectedColumnInfo();
        pci.vv = vector;
        pci.openTSDBColumn = column;
        pci.index = columnNumber;
        return pci;
    }
}