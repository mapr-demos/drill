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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;

import static java.nio.charset.StandardCharsets.UTF_8;

class TestTableGenerator {

  private static final int PORT = 4242; //replace with your port
  private static final String HOSTNAME = "172.16.229.128"; //replace with the node where tsd runs

  private static final String TEST_FILE_NAME = "tmp_input";

  private static Socket socket;
  private static PrintWriter pw;

  static void setupTestData() throws Exception {
    setupSocketConnection();
    String pathToFile = getPathToTestFile();
    InputStreamReader reader = new InputStreamReader(new FileInputStream(pathToFile), UTF_8);
    BufferedReader br = new BufferedReader(reader);
    readAndSendDataToDB(br);
  }

  private static String getPathToTestFile() {
    return Thread.currentThread().getContextClassLoader().getResource(TEST_FILE_NAME).getPath();
  }

  private static void setupSocketConnection() throws IOException {
    socket = new Socket(HOSTNAME, PORT);
    OutputStreamWriter stream = new OutputStreamWriter(socket.getOutputStream(), UTF_8);
    pw = new PrintWriter(stream, true);
  }

  private static void readAndSendDataToDB(BufferedReader br) throws IOException {
    String line;
    while ((line = br.readLine()) != null) {
      if (!line.contains("*")) {
        System.out.println(line);
        pw.println(line);
        pw.flush();
      }
    }
    System.out.println();
    br.close();
  }
}
