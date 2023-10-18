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

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.chronicle.source.split.ChronicleSourceSplit;

import com.google.auto.service.AutoService;

import java.util.ArrayList;

import static org.apache.seatunnel.chronicle.config.ChronicleConfig.CONNECTOR_IDENTITY;

@AutoService(Factory.class)
public class ChronicleSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                //                .required(
                //                        MongodbConfig.URI,
                //                        MongodbConfig.DATABASE,
                //                        MongodbConfig.COLLECTION,
                //                        CatalogTableUtil.SCHEMA)
                //                .optional(
                //                        MongodbConfig.PROJECTION,
                //                        MongodbConfig.MATCH_QUERY,
                //                        MongodbConfig.SPLIT_SIZE,
                //                        MongodbConfig.SPLIT_KEY,
                //                        MongodbConfig.CURSOR_NO_TIMEOUT,
                //                        MongodbConfig.FETCH_SIZE,
                //                        MongodbConfig.MAX_TIME_MIN)
                .build();
    }

    @Override
    public Class<
                    ? extends
                            SeaTunnelSource<
                                    SeaTunnelRow,
                                    ChronicleSourceSplit,
                                    ArrayList<ChronicleSourceSplit>>>
            getSourceClass() {
        return ChronicleSource.class;
    }
}
