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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigList;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigMemorySize;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigMergeable;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigOrigin;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigResolveOptions;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.dto.ElasticsearchClusterInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.sink.ElasticsearchSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.hw.source.ElasticsearchSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class ElasticsearchFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new ElasticsearchSourceFactory()).optionRule());
        Assertions.assertNotNull((new ElasticsearchSinkFactory()).optionRule());
    }
}
