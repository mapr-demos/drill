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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.openTSDB.OpenTSDBSubScan.OpenTSDBSubScanSpec;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonTypeName("openTSDB-scan")
public class OpenTSDBGroupScan extends AbstractGroupScan {

  private static final long DEFAULT_TABLET_SIZE = 1000;

  private OpenTSDBStoragePluginConfig storagePluginConfig;
  private OpenTSDBScanSpec openTSDBScanSpec;
  private OpenTSDBStoragePlugin storagePlugin;

  private ListMultimap<Integer, OpenTSDBWork> assignments;
  private List<SchemaPath> columns;
  private List<OpenTSDBWork> openTSDBWorkList = Lists.newArrayList();
  private List<EndpointAffinity> affinities;

  private boolean filterPushedDown = false;

  @JsonCreator
  public OpenTSDBGroupScan(@JsonProperty("openTSDBScanSpec") OpenTSDBScanSpec openTSDBScanSpec,
                           @JsonProperty("storage") OpenTSDBStoragePluginConfig openTSDBStoragePluginConfig,
                           @JsonProperty("columns") List<SchemaPath> columns,
                           @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this((OpenTSDBStoragePlugin) pluginRegistry.getPlugin(openTSDBStoragePluginConfig), openTSDBScanSpec, columns);
  }

  public OpenTSDBGroupScan(OpenTSDBStoragePlugin storagePlugin,
                           OpenTSDBScanSpec scanSpec, List<SchemaPath> columns) {
    super((String) null);
    this.storagePlugin = storagePlugin;
    this.storagePluginConfig = storagePlugin.getConfig();
    this.openTSDBScanSpec = scanSpec;
    this.columns = columns == null || columns.size() == 0 ? ALL_COLUMNS : columns;
    init();
  }

  /**
   * Private constructor, used for cloning.
   *
   * @param that The OpenTSDBGroupScan to clone
   */
  private OpenTSDBGroupScan(OpenTSDBGroupScan that) {
    super((String) null);
    this.columns = that.columns;
    this.openTSDBScanSpec = that.openTSDBScanSpec;
    this.storagePlugin = that.storagePlugin;
    this.storagePluginConfig = that.storagePluginConfig;
    this.filterPushedDown = that.filterPushedDown;
    this.openTSDBWorkList = that.openTSDBWorkList;
    this.assignments = that.assignments;
    init();
  }

  private void init() {
    Collection<DrillbitEndpoint> endpoints = storagePlugin.getContext().getBits();
    Map<String, DrillbitEndpoint> endpointMap = Maps.newHashMap();

    for (DrillbitEndpoint endpoint : endpoints) {
      endpointMap.put(endpoint.getAddress(), endpoint);
    }
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    if (affinities == null) {
      affinities = AffinityCreator.getAffinityMap(openTSDBWorkList);
    }
    return affinities;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return openTSDBWorkList.size();
  }

  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    assignments = AssignmentCreator.getMappings(incomingEndpoints, openTSDBWorkList);
  }

  @Override
  public OpenTSDBSubScan getSpecificScan(int minorFragmentId) {
    List<OpenTSDBSubScanSpec> scanSpecList = Lists.newArrayList();
    scanSpecList.add(new OpenTSDBSubScanSpec(getTableName()));
    return new OpenTSDBSubScan(storagePlugin, storagePluginConfig, scanSpecList, this.columns);
  }

  @Override
  public ScanStats getScanStats() {
    //magic number ?
    long recordCount = 100000L * openTSDBWorkList.size();
    return new ScanStats(ScanStats.GroupScanProperty.NO_EXACT_ROW_COUNT, recordCount, 1, recordCount);
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new OpenTSDBGroupScan(this);
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @JsonIgnore
  public String getTableName() {
    return getOpenTSDBScanSpec().getTableName();
  }

  @JsonProperty
  public OpenTSDBScanSpec getOpenTSDBScanSpec() {
    return openTSDBScanSpec;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    OpenTSDBGroupScan newScan = new OpenTSDBGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  @Override
  public String toString() {
    return "OpenTSDBGroupScan [OpenTSDBScanSpec="
        + openTSDBScanSpec + "]";
  }

  private static class OpenTSDBWork implements CompleteWork {

    private EndpointByteMapImpl byteMap = new EndpointByteMapImpl();

    @Override
    public long getTotalBytes() {
      return DEFAULT_TABLET_SIZE;
    }

    @Override
    public EndpointByteMap getByteMap() {
      return byteMap;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      OpenTSDBWork that = (OpenTSDBWork) o;
      return Objects.equals(byteMap, that.byteMap);
    }

    @Override
    public int hashCode() {
      return Objects.hash(byteMap);
    }

    @Override
    public int compareTo(CompleteWork o) {
      return 0;
    }
  }
}
