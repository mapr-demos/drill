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
package org.apache.drill.exec.store.openTSDB.dto;

import java.util.List;
import java.util.Map;

public class MetricDTO {

  private String metric;
  private Map<String, String> tags;
  private List<String> aggregateTags;
  private Map<String, String> dps;

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  public List<String> getAggregateTags() {
    return aggregateTags;
  }

  public void setAggregateTags(List<String> aggregateTags) {
    this.aggregateTags = aggregateTags;
  }

  public Map<String, String> getDps() {
    return dps;
  }

  public void setDps(Map<String, String> dps) {
    this.dps = dps;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    MetricDTO table = (MetricDTO) o;

    if (metric != null ? !metric.equals(table.metric) : table.metric != null) {
      return false;
    }
    if (tags != null ? !tags.equals(table.tags) : table.tags != null) {
      return false;
    }
    if (aggregateTags != null ? !aggregateTags.equals(table.aggregateTags) : table.aggregateTags != null) {
      return false;
    }
    return dps != null ? dps.equals(table.dps) : table.dps == null;
  }

  @Override
  public int hashCode() {
    int result = metric != null ? metric.hashCode() : 0;
    result = 31 * result + (tags != null ? tags.hashCode() : 0);
    result = 31 * result + (aggregateTags != null ? aggregateTags.hashCode() : 0);
    result = 31 * result + (dps != null ? dps.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Table{" +
        "metric='" + metric + '\'' +
        ", tags=" + tags +
        ", aggregateTags=" + aggregateTags +
        ", dps=" + dps +
        '}';
  }
}
