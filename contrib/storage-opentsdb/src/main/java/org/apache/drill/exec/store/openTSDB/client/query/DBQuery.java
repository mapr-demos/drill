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
package org.apache.drill.exec.store.openTSDB.client.query;

import org.apache.drill.common.exceptions.DrillRuntimeException;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * DBQuery is an abstraction of an openTSDB query,
 * that used for extracting data from the storage system by POST request to DB.
 * <p>
 * An OpenTSDB query requires at least one sub query,
 * a means of selecting which time series should be included in the result set.
 */
public class DBQuery {

  /**
   * The start time for the query. This can be a relative or absolute timestamp.
   */
  private String start;
  /**
   * One or more sub subQueries used to select the time series to return.
   */
  private Set<Query> queries;

  private DBQuery(Builder builder) {
    this.start = builder.start;
    this.queries = builder.queries;
  }

  public String getStart() {
    return start;
  }

  public Set<Query> getQueries() {
    return queries;
  }

  public static class Builder {

    private String start;
    private Set<Query> queries = new HashSet<>();

    public Builder() {
    }

    public Builder setStartTime(String startTime) {
      Objects.requireNonNull(startTime, "start must be specified");
      this.start = startTime;
      return this;
    }

    public Builder setQueries(Set<Query> queries) {
      if (queries.isEmpty()) {
        throw new DrillRuntimeException("Required params such as metric, aggregator weren't specified. " +
                "Add this params to the query");
      }
      this.queries = queries;
      return this;
    }

    public DBQuery build() {
      return new DBQuery(this);
    }

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DBQuery baseQuery = (DBQuery) o;

    return (start != null ? start.equals(baseQuery.start) : baseQuery.start == null) && (queries != null ? queries.equals(baseQuery.queries) : baseQuery.queries == null);
  }

  @Override
  public int hashCode() {
    int result = start != null ? start.hashCode() : 0;
    result = 31 * result + (queries != null ? queries.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "BaseQuery{" +
        "start='" + start + '\'' +
        ", queries=" + queries +
        '}';
  }
}
