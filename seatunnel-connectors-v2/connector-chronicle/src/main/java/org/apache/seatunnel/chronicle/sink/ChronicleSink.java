/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.chronicle.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.chronicle.sink.state.ChronicleAggCommitInfo;
import org.apache.seatunnel.chronicle.sink.state.ChronicleCommitInfo;
import org.apache.seatunnel.chronicle.sink.state.ChronicleSinkState;
import org.apache.seatunnel.chronicle.sink.writer.ChronicleWriter;
import org.apache.seatunnel.chronicle.sink.writer.ChronicleWriterOptions;

import com.google.auto.service.AutoService;

import java.io.IOException;

import static org.apache.seatunnel.chronicle.config.ChronicleConfig.CONNECTOR_IDENTITY;

@AutoService(SeaTunnelSink.class)
public class ChronicleSink
        implements SeaTunnelSink<
                SeaTunnelRow, ChronicleSinkState, ChronicleCommitInfo, ChronicleAggCommitInfo> {

    private ChronicleWriterOptions options;
    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public String getPluginName() {
        return CONNECTOR_IDENTITY;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void prepare(Config config) throws PrepareFailException {
        //        this.option =
        //                ReaderOption.builder()
        //                        .shardMetadata(metadata)
        //                        .properties(clickhouseProperties)
        //                        .tableEngine(table.getEngine())
        //                        .tableSchema(tableSchema)
        //                        .bulkSize(config.getInt(BULK_SIZE.key()))
        //                        .primaryKeys(primaryKeys)
        //                        .supportUpsert(supportUpsert)
        //
        // .allowExperimentalLightweightDelete(allowExperimentalLightweightDelete)
        //                        .build();
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, ChronicleCommitInfo, ChronicleSinkState> createWriter(
            SinkWriter.Context context) throws IOException {
        return new ChronicleWriter();
    }
}
