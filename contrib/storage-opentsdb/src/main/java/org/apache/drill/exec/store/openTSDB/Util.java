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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.util.Set;

public class Util {
    /**
     * List of API QUERIES for openTSDB
     */
    static final String TABLES_NAME_QUERY = "/api/suggest?type=metrics&max=100";

    /**
     * Parse Json Array to Set<String>
     *
     * @param stringJsonArray String Json Array
     * @return Set<String>
     * @throws IOException if something went wrong
     */
    public static Set<String> parseJsonArrayToSet(final String stringJsonArray) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(stringJsonArray, new TypeReference<Set<String>>() {});
    }

    /**
     * @param connection Host of the Database
     * @param query      Query to DB which you want to perform
     * @return CloseableHttpResponse response from DB
     * @throws IOException
     * @throws HttpException
     */
    public static CloseableHttpResponse performGETRequestToDB(String connection, String query) throws HttpException, IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
        HttpGet httpget = new HttpGet(connection + query);
        CloseableHttpResponse response = httpclient.execute(httpget);
        if (response.getStatusLine().getStatusCode() == 200) {
            return response;
        } else {
            throw new HttpException(response.toString());
        }
    }
}
