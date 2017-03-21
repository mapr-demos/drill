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

import org.apache.drill.PlanTestBase;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.rpc.user.QueryDataBatch;
import org.junit.Assert;

import java.util.List;

class TestBase extends PlanTestBase {

  void runSQLVerifyCount(String sql, int expectedRowCount)
      throws Exception {
    List<QueryDataBatch> results = runSQLWithResults(sql);
    printResultAndVerifyRowCount(results, expectedRowCount);
  }

  private List<QueryDataBatch> runSQLWithResults(String sql)
      throws Exception {
    return testSqlWithResults(sql);
  }

  private void printResultAndVerifyRowCount(List<QueryDataBatch> results,
                                            int expectedRowCount) throws SchemaChangeException {
    setColumnWidth(30);
    int rowCount = printResult(results);
    if (expectedRowCount != -1) {
      Assert.assertEquals(expectedRowCount, rowCount);
    }
  }
}
