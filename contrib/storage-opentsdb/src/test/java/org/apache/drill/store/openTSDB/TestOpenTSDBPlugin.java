/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to you under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
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

import org.apache.drill.BaseTestQuery;
import org.apache.drill.common.exceptions.UserRemoteException;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.drill.store.openTSDB.TestTableGenerator.setupTestData;

@Ignore("requires a remote openTSDB server to run.")
public class TestOpenTSDBPlugin extends BaseTestQuery {

  @BeforeClass
  public static void addTestDataToDB() throws Exception {
    setupTestData();
  }

  @Test
  public void testBasicQueryFROMWithTableName() throws Exception {
    test("select * from openTSDB.`warp.speed.test`");
  }

  @Test
  public void testBasicQueryFROMWithRequiredParams() throws Exception {
    test("select * from openTSDB.`(metric=warp.speed.test)`");
  }

  @Test
  public void testBasicQueryFROMWithParameters() throws Exception {
    test("select * from openTSDB.`(metric=warp.speed.test, aggregator=sum)`;");
  }

  @Test(expected = UserRemoteException.class)
  public void testBasicQueryWithoutTableName() throws Exception {
    test("select * from openTSDB.``;");
  }

  @Test(expected = UserRemoteException.class)
  public void testBasicQueryWithNonExistentTableName() throws Exception {
    test("select * from openTSDB.`warp.spee`");
  }

  @Test
  public void testDescribe() throws Exception {
    test("use openTSDB;");
    test("show tables;");
    test("describe `warp.speed.test`");
  }
}
