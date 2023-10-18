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

package org.apache.seatunnel.chronicle.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.chronicle.source.split.ChronicleSourceSplit;

import com.google.auto.service.AutoService;

import java.util.ArrayList;

import static org.apache.seatunnel.chronicle.config.ChronicleConfig.CONNECTOR_IDENTITY;

@AutoService(SeaTunnelSource.class)
public class ChronicleSource
        implements SeaTunnelSource<
                        SeaTunnelRow, ChronicleSourceSplit, ArrayList<ChronicleSourceSplit>>,
                SupportColumnProjection {
    private SeaTunnelRowType rowTypeInfo;

    @Override
    public String getPluginName() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {}

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, ChronicleSourceSplit> createReader(
            SourceReader.Context readerContext) throws Exception {
        return null;
    }

    @Override
    public SourceSplitEnumerator<ChronicleSourceSplit, ArrayList<ChronicleSourceSplit>>
            createEnumerator(SourceSplitEnumerator.Context<ChronicleSourceSplit> enumeratorContext)
                    throws Exception {
        return null;
    }

    @Override
    public SourceSplitEnumerator<ChronicleSourceSplit, ArrayList<ChronicleSourceSplit>>
            restoreEnumerator(
                    SourceSplitEnumerator.Context<ChronicleSourceSplit> enumeratorContext,
                    ArrayList<ChronicleSourceSplit> checkpointState)
                    throws Exception {
        return null;
    }
}
