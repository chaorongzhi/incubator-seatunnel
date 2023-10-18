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

package org.apache.seatunnel.chronicle.sink.writer;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.chronicle.sink.state.ChronicleCommitInfo;
import org.apache.seatunnel.chronicle.sink.state.ChronicleSinkState;

import lombok.extern.slf4j.Slf4j;
import net.openhft.chronicle.queue.ExcerptTailer;

import java.io.IOException;
import java.util.Optional;

@Slf4j
public class ChronicleWriter
        implements SinkWriter<SeaTunnelRow, ChronicleCommitInfo, ChronicleSinkState> {

    private ExcerptTailer tailer;

    public ChronicleWriter() {
        // TODO init client  需要简单封装下  队列  在这里初始化
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {}

    @Override
    public Optional<ChronicleCommitInfo> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() {
        tailer.close();
    }
}
