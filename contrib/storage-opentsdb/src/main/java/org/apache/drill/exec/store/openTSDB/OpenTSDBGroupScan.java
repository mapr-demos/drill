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
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.PhysicalOperatorSetupException;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.SubScan;
import org.apache.drill.exec.proto.CoordinationProtos;
import org.apache.drill.exec.store.StoragePluginRegistry;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.io.IOException;
import java.util.List;

@Slf4j
@JsonTypeName("openTSDB-scan")
public class OpenTSDBGroupScan extends AbstractGroupScan {

    private OpenTSDBStoragePluginConfig storagePluginConfig;
    private OpenTSDBScanSpec openTSDBScanSpec;
    private OpenTSDBStoragePlugin storagePlugin;

    @JsonCreator
    public OpenTSDBGroupScan(@JsonProperty("openTSDBScanSpec") OpenTSDBScanSpec openTSDBScanSpec,
                             @JsonProperty("storage") OpenTSDBStoragePluginConfig openTSDBStoragePluginConfig,
                             @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
        this((OpenTSDBStoragePlugin) pluginRegistry.getPlugin(openTSDBStoragePluginConfig), openTSDBScanSpec);
    }

    public OpenTSDBGroupScan(OpenTSDBStoragePlugin storagePlugin,
                             OpenTSDBScanSpec scanSpec) {
        super((String) null);
        this.storagePlugin = storagePlugin;
        this.storagePluginConfig = storagePlugin.getConfig();
        this.openTSDBScanSpec = scanSpec;
        init();
    }

    private void init() {
        log.info("OpenTSDBGroupScan.init()");
    }

    /**
     * Private constructor, used for cloning.
     * @param that The OpenTSDBGroupScan to clone
     */
    public OpenTSDBGroupScan(OpenTSDBGroupScan that) {
        super((String) null);
        this.storagePlugin = that.storagePlugin;
        this.storagePluginConfig = that.storagePlugin.getConfig();
        this.openTSDBScanSpec = that.openTSDBScanSpec;
        init();
    }

    @Override
    public GroupScan clone(List<SchemaPath> columns) {
        return new OpenTSDBGroupScan(this);
    }


    public OpenTSDBGroupScan(String userName) {
        super(userName);
    }

    @Override
    public void applyAssignments(List<CoordinationProtos.DrillbitEndpoint> list) throws PhysicalOperatorSetupException {

    }

    @Override
    public SubScan getSpecificScan(int i) throws ExecutionSetupException {
        return null;
    }

    @Override
    public int getMaxParallelizationWidth() {
        return 0;
    }

    @Override
    public String getDigest() {
        return null;
    }

    @Override
    public PhysicalOperator getNewWithChildren(List<PhysicalOperator> list) throws ExecutionSetupException {
        return null;
    }

    @Override
    public String toString() {
        return "OpenTSDBGroupScan [OpenTSDBScanSpec="
                + openTSDBScanSpec + "]";
    }
}
