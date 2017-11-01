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
package org.apache.drill.exec.store.openTSDB.schema;

import com.google.common.collect.Maps;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.exec.planner.logical.DrillTable;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.drill.exec.store.AbstractSchema;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.SchemaFactory;
import org.apache.drill.exec.store.openTSDB.DrillOpenTSDBTable;
import org.apache.drill.exec.store.openTSDB.OpenTSDBScanSpec;
import org.apache.drill.exec.store.openTSDB.OpenTSDBStoragePlugin;
import org.apache.drill.exec.store.openTSDB.OpenTSDBStoragePluginConfig;
import org.apache.drill.exec.store.openTSDB.client.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.apache.drill.exec.store.openTSDB.Util.getValidTableName;

public class OpenTSDBSchemaFactory implements SchemaFactory {

  private static final Logger log = LoggerFactory.getLogger(OpenTSDBSchemaFactory.class);

  private final String schemaName;
  private final OpenTSDBStoragePlugin plugin;

  public OpenTSDBSchemaFactory(OpenTSDBStoragePlugin plugin, String schemaName) {
    this.plugin = plugin;
    this.schemaName = schemaName;
  }

  @Override
  public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
    OpenTSDBSchema schema = new OpenTSDBSchema(schemaName);
    parent.add(schemaName, schema);
  }

  class OpenTSDBSchema extends AbstractSchema {
    private final Map<String, OpenTSDBDatabaseSchema> schemaMap = Maps.newHashMap();

    OpenTSDBSchema(String name) {
      super(Collections.<String>emptyList(), name);
    }

    @Override
    public AbstractSchema getSubSchema(String name) {
      Set<String> tables;
      if (!schemaMap.containsKey(name)) {
        tables = plugin.getClient().getAllMetricNames();
        schemaMap.put(name, new OpenTSDBDatabaseSchema(tables, this, name));
      }
      return schemaMap.get(name);
    }

    @Override
    public Table getTable(String name) {
      OpenTSDBScanSpec scanSpec = new OpenTSDBScanSpec(name);
      name = getValidTableName(name);
      try {
        return new DrillOpenTSDBTable(schemaName, plugin, new Schema(plugin.getClient(), name), scanSpec);
      } catch (Exception e) {
        throw new DrillRuntimeException(String.format("Failure while retrieving openTSDB table %s", name), e);
      }
    }

    @Override
    public Set<String> getTableNames() {
      return plugin.getClient().getAllMetricNames();
    }

    @Override
    public String getTypeName() {
      return OpenTSDBStoragePluginConfig.NAME;
    }

    DrillTable getDrillTable(String tableName) {
      OpenTSDBScanSpec openTSDBScanSpec = new OpenTSDBScanSpec(tableName);
      return new DynamicDrillTable(plugin, schemaName, null, openTSDBScanSpec);
    }
  }
}