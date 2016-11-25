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

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.ischema.Records;

import java.util.List;

enum Type {
    BINARY,
    BOOL,
    DOUBLE,
    FLOAT,
    INT16,
    INT32,
    INT64,
    INT8,
    STRING,
    TIMESTAMP
}

@Slf4j
public class DrillOpenTSDBTable extends DynamicDrillTable {

    private final Records.Schema schema;

    public DrillOpenTSDBTable(String storageEngineName, OpenTSDBStoragePlugin plugin, Records.Schema schema, OpenTSDBScanSpec scanSpec) {
        super(plugin, storageEngineName, scanSpec);
        this.schema = schema;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {

        List<String> names = Lists.newArrayList();
        List<RelDataType> types = Lists.newArrayList();
//        for (ColumnSchema column : schema.getColumns()) {
//            names.add(column.getName());
//            RelDataType type = getSqlTypeFromKuduType(typeFactory, column.getType());
//            type = typeFactory.createTypeWithNullability(type, column.isNullable());
//            types.add(type);
//        }

        return typeFactory.createStructType(types, names);
    }

    private RelDataType getSqlTypeFromKuduType(RelDataTypeFactory typeFactory, Type type) {
        switch (type) {
            case BINARY:
            case BOOL:
            case DOUBLE:
            case FLOAT:
            case INT16:
            case INT32:
            case INT64:
            case INT8:
            case STRING:
            case TIMESTAMP:
            default:
                throw new UnsupportedOperationException("Unsupported type.");
        }
    }
}