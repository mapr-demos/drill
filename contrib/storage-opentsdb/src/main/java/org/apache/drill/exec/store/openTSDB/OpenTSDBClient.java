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
import org.apache.http.HttpException;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Set;

import static org.apache.drill.exec.store.openTSDB.Util.TABLES_NAME_QUERY;
import static org.apache.drill.exec.store.openTSDB.Util.performGETRequestToDB;

@Slf4j
public class OpenTSDBClient {

    private String connection = "http://";

    public OpenTSDBClient(String connection) throws IOException {
        this.connection += connection;
    }

    // TODO: Implement getting table schema!!!!
    public Schema getSchema(String tableName) {
        return new Schema(tableName);
    }

    public Set<String> getAllTables() throws IOException, HttpException {
        return getTablesFromDatabase();
    }

    private Set<String> getTablesFromDatabase() throws IOException, HttpException {
        CloseableHttpResponse responseFromDB = performGETRequestToDB(connection, TABLES_NAME_QUERY);
        return Util.parseJsonArrayToSet(readResponse(responseFromDB));
    }

    private String readResponse(CloseableHttpResponse response) throws IOException {
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(response.getEntity().getContent()));
        StringBuilder result = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            result.append(line);
        }
        return result.toString();
    }

    public final void close() {
    }
}
