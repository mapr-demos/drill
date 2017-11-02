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
package org.apache.drill.store.openTSDB;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.openTSDB.OpenTSDBStoragePlugin;
import org.apache.drill.exec.store.openTSDB.OpenTSDBStoragePluginConfig;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static org.apache.drill.store.openTSDB.TestDataHolder.DOWNSAMPLE_REQUEST_WITH_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.DOWNSAMPLE_REQUEST_WTIHOUT_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.POST_REQUEST_WITHOUT_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.POST_REQUEST_WITH_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.REQUEST_TO_NONEXISTENT_METRIC;
import static org.apache.drill.store.openTSDB.TestDataHolder.SAMPLE_DATA_FOR_GET_TABLE_NAME_REQUEST;
import static org.apache.drill.store.openTSDB.TestDataHolder.SAMPLE_DATA_FOR_GET_TABLE_REQUEST;
import static org.apache.drill.store.openTSDB.TestDataHolder.SAMPLE_DATA_FOR_POST_DOWNSAMPLE_REQUEST_WITHOUT_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.SAMPLE_DATA_FOR_POST_DOWNSAMPLE_REQUEST_WITH_TAGS;
import static org.apache.drill.store.openTSDB.TestDataHolder.SAMPLE_DATA_FOR_POST_REQUEST_WITH_TAGS;

public class TestOpenTSDBPlugin extends BaseTestQuery {
  protected static OpenTSDBStoragePlugin storagePlugin;
  protected static OpenTSDBStoragePluginConfig storagePluginConfig;

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(10000);

  private static TestBase base;

  @BeforeClass
  public static void addTestDataToDB() throws Exception {
    final StoragePluginRegistry pluginRegistry = getDrillbitContext().getStorage();
    storagePlugin = (OpenTSDBStoragePlugin) pluginRegistry.getPlugin(OpenTSDBStoragePluginConfig.NAME);
    storagePluginConfig = storagePlugin.getConfig();
    storagePluginConfig.setEnabled(true);
    pluginRegistry.createOrUpdate(OpenTSDBStoragePluginConfig.NAME, storagePluginConfig, true);
    base = new TestBase();
  }

  private void setupGETStubs() {
    stubFor(get(urlEqualTo("/api/suggest?type=metrics&max=" + Integer.MAX_VALUE))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(SAMPLE_DATA_FOR_GET_TABLE_NAME_REQUEST)));

    stubFor(get(urlEqualTo("/api/query?start=47y-ago&m=sum:warp.speed.test"))
        .willReturn(aResponse()
            .withStatus(200)
            .withBody(SAMPLE_DATA_FOR_GET_TABLE_REQUEST)
        ));
  }

  private void setupPOSTStubs() {

    stubFor(post(urlEqualTo("/api/query"))
        .withRequestBody(equalToJson(POST_REQUEST_WITHOUT_TAGS))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(SAMPLE_DATA_FOR_GET_TABLE_REQUEST)));

    stubFor(post(urlEqualTo("/api/query"))
        .withRequestBody(equalToJson(POST_REQUEST_WITH_TAGS))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(SAMPLE_DATA_FOR_POST_REQUEST_WITH_TAGS)));

    stubFor(post(urlEqualTo("/api/query"))
        .withRequestBody(equalToJson(DOWNSAMPLE_REQUEST_WTIHOUT_TAGS))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(SAMPLE_DATA_FOR_POST_DOWNSAMPLE_REQUEST_WITHOUT_TAGS)));

    stubFor(post(urlEqualTo("/api/query"))
        .withRequestBody(equalToJson(DOWNSAMPLE_REQUEST_WITH_TAGS))
        .willReturn(aResponse()
            .withStatus(200)
            .withHeader("Content-Type", "application/json")
            .withBody(SAMPLE_DATA_FOR_POST_DOWNSAMPLE_REQUEST_WITH_TAGS)));

    stubFor(post(urlEqualTo("/api/query"))
        .withRequestBody(equalToJson(REQUEST_TO_NONEXISTENT_METRIC))
        .willReturn(aResponse()
            .withStatus(400)
            .withHeader("Content-Type", "application/json")
        ));
  }

  @Test
  public void testBasicQueryFROMWithRequiredParams() throws Exception {
    setupPOSTStubs();
    setupGETStubs();
    base.runSQLVerifyCount("select * from openTSDB.`(metric=warp.speed.test, start=47y-ago, aggregator=sum)`", 18);
  }

  @Test
  public void testBasicQueryGROUPBY() throws Exception {
    setupPOSTStubs();
    setupGETStubs();
    base.runSQLVerifyCount("select `timestamp`, sum(`aggregated value`) from openTSDB.`(metric=warp.speed.test, aggregator=sum, start=47y-ago)` group by `timestamp`", 15);
  }

  @Test
  public void testBasicQueryFROMWithInterpolationParam() throws Exception {
    setupPOSTStubs();
    setupGETStubs();
    base.runSQLVerifyCount("select * from openTSDB.`(metric=warp.speed.test, aggregator=sum, start=47y-ago, downsample=5y-avg)`", 4);
  }

  @Test(expected = UserRemoteException.class)
  public void testBasicQueryWithoutTableName() throws Exception {
    test("select * from openTSDB.``;");
  }

  @Test(expected = UserRemoteException.class)
  public void testBasicQueryWithNonExistentTableName() throws Exception {
    setupPOSTStubs();
    test("select * from openTSDB.`warp.spee`");
  }

  @Test
  public void testDescribe() throws Exception {
    setupPOSTStubs();
    setupGETStubs();
    test("use openTSDB;");
    base.runSQLVerifyCount("show tables", 1);
    test("describe `warp.speed.test`");
  }
}
