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

import lombok.extern.slf4j.Slf4j;
import org.apache.drill.exec.store.openTSDB.dto.ColumnDTO;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class Schema {

    private final List<ColumnDTO> columns = new ArrayList<>();
    private String tableName;

    public Schema(String name) {
        this.tableName = name;
        columns.add(new ColumnDTO("timestamp", OpenTSDBTypes.TIMESTAMP));
        columns.add(new ColumnDTO("value", OpenTSDBTypes.INT64));
        columns.add(new ColumnDTO("metric", OpenTSDBTypes.STRING));
    }

    // TODO: Implement API call
    public List<String> getTableColumnNames() {
        List<String> columns = new ArrayList<>();
        for (ColumnDTO column : this.columns) {
            columns.add(column.getColumnName());
        }
        return columns;
    }

    // TODO: Implement API call
    public List<ColumnDTO> getColumns() {
        return columns;
    }

}
