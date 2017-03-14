/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.openTSDB.client.query;

import java.util.Map;

public class Query {

  private String aggregator;
  private String metric;
  private String rate;
  private String downsample;
  private Map<String, String> tags;

  public String getAggregator() {
    return aggregator;
  }

  public void setAggregator(String aggregator) {
    this.aggregator = aggregator;
  }

  public String getMetric() {
    return metric;
  }

  public void setMetric(String metric) {
    this.metric = metric;
  }

  public String getRate() {
    return rate;
  }

  public void setRate(String rate) {
    this.rate = rate;
  }

  public String getDownsample() {
    return downsample;
  }

  public void setDownsample(String downsample) {
    this.downsample = downsample;
  }

  public Map<String, String> getTags() {
    return tags;
  }

  public void setTags(Map<String, String> tags) {
    this.tags = tags;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Query query = (Query) o;

    if (aggregator != null ? !aggregator.equals(query.aggregator) : query.aggregator != null) {
      return false;
    }
    if (metric != null ? !metric.equals(query.metric) : query.metric != null) {
      return false;
    }
    if (rate != null ? !rate.equals(query.rate) : query.rate != null) {
      return false;
    }
    if (downsample != null ? !downsample.equals(query.downsample) : query.downsample != null) {
      return false;
    }
    return tags != null ? tags.equals(query.tags) : query.tags == null;
  }

  @Override
  public int hashCode() {
    int result = aggregator != null ? aggregator.hashCode() : 0;
    result = 31 * result + (metric != null ? metric.hashCode() : 0);
    result = 31 * result + (rate != null ? rate.hashCode() : 0);
    result = 31 * result + (downsample != null ? downsample.hashCode() : 0);
    result = 31 * result + (tags != null ? tags.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "Query{" +
        "aggregator='" + aggregator + '\'' +
        ", metric='" + metric + '\'' +
        ", rate='" + rate + '\'' +
        ", downsample='" + downsample + '\'' +
        ", tags=" + tags +
        '}';
  }
}
