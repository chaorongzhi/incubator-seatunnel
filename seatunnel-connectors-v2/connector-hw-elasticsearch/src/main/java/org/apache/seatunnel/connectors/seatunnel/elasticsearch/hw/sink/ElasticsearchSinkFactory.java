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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.config.EsClusterConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.config.SinkConfig;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class ElasticsearchSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "HwElasticsearch";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(EsClusterConnectionConfig.HOSTS, SinkConfig.INDEX)
                .optional(
                        SinkConfig.INDEX_TYPE,
                        SinkConfig.PRIMARY_KEYS,
                        SinkConfig.KEY_DELIMITER,
                        EsClusterConnectionConfig.USERNAME,
                        EsClusterConnectionConfig.PASSWORD,
                        SinkConfig.MAX_RETRY_COUNT,
                        SinkConfig.MAX_BATCH_SIZE,
                        EsClusterConnectionConfig.TLS_VERIFY_CERTIFICATE,
                        EsClusterConnectionConfig.TLS_VERIFY_HOSTNAME,
                        EsClusterConnectionConfig.TLS_KEY_STORE_PATH,
                        EsClusterConnectionConfig.TLS_KEY_STORE_PASSWORD,
                        EsClusterConnectionConfig.TLS_TRUST_STORE_PATH,
                        EsClusterConnectionConfig.TLS_TRUST_STORE_PASSWORD,
                        EsClusterConnectionConfig.HW_ES_AUTH_CONFIG)
                .build();
    }
}
