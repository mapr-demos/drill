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

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.openTSDB.client.OpenTSDB;
import org.apache.drill.exec.store.openTSDB.dto.TableDTO;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

@Slf4j
public class OpenTSDBRecordReader extends AbstractRecordReader {

    private final OpenTSDB client;
    private final OpenTSDBSubScan.OpenTSDBSubScanSpec scanSpec;
    private final boolean unionEnabled;
    private OperatorContext operatorContext;

    private TableDTO table;
    private VectorContainerWriter vectorWriter;

    private OutputMutator output;
    private OperatorContext context;


    public OpenTSDBRecordReader(OpenTSDB client, OpenTSDBSubScan.OpenTSDBSubScanSpec subScanSpec,
                                List<SchemaPath> projectedColumns, FragmentContext context) {
        setColumns(projectedColumns);
        this.client = client;
        scanSpec = subScanSpec;
        unionEnabled = context.getOptions().getOption(ExecConstants.ENABLE_UNION_TYPE);
        log.debug("Scan spec: {}", subScanSpec);
    }

    @Override
    public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
        this.output = output;
        this.context = context;

        this.vectorWriter = new VectorContainerWriter(output, unionEnabled);
        this.operatorContext = context;

        try {
            table = client.getTable("5y-ago", scanSpec.getTableName())
                    .execute()
                    .body()
                    .get(0);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> columns) {
        Set<SchemaPath> transformed = Sets.newLinkedHashSet();
        if (!isStarQuery()) {
            for (SchemaPath column : this.getColumns()) {
                transformed.add(column);
            }
        } else {
            transformed.add(AbstractRecordReader.STAR_COLUMN);
        }
        return transformed;
    }

    @Override
    public int next() {
        return 1;
    }

    @Override
    public void close() throws Exception {

    }
}
