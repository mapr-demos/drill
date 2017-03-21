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
package org.apache.drill.exec.store.openTSDB.client;

import org.apache.drill.exec.store.openTSDB.client.query.DBQuery;
import org.apache.drill.exec.store.openTSDB.dto.MetricDTO;
import retrofit2.Call;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;
import retrofit2.http.Query;

import java.util.Set;

/**
 * Client for API requests to openTSDB
 */
public interface OpenTSDB {

  /**
   * Used for getting all metrics names from openTSDB
   *
   * @return Set<String> with all tables names
   */
  @GET("api/suggest?type=metrics&max=999")
  Call<Set<String>> getAllTablesName();

  /**
   * Used for retrieving all tables that corresponds to params
   *
   * @param time                    The start time for the query. This can be a relative or absolute timestamp.
   * @param aggregatorWithTableName Aggregator with Metric name with such syntax <aggregator>:<table_name>
   * @return Set<Table> with metrics from openTSDB
   */
  @GET("api/query")
  Call<Set<MetricDTO>> getTables(@Query("start") String time, @Query("m") String aggregatorWithTableName);

  /**
   * Overloaded getTables for POST request to DB
   *
   * @param query Query for for selecting data
   * @return Set<Table> with metrics from openTSDB
   */
  @POST("api/query")
  Call<Set<MetricDTO>> getTables(@Body DBQuery query);
}
