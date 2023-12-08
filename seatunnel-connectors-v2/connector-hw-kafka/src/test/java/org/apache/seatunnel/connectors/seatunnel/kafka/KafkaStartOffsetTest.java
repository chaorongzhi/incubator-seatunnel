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

package org.apache.seatunnel.connectors.seatunnel.kafka;

import org.apache.seatunnel.connectors.seatunnel.kafka.hw.sink.KafkaNoTransactionSender;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Properties;

public class KafkaStartOffsetTest {

    @Test
    void getTopicNameAndPartition() {
        String topicName = "my-topic-test";
        int partIndex = 1;
        String key = "my-topic-test-1";
        int splitIndex = key.lastIndexOf("-");
        String topic = key.substring(0, splitIndex);
        String partition = key.substring(splitIndex + 1);
        Assertions.assertEquals(topic, topicName);
        Assertions.assertEquals(Integer.valueOf(partition), partIndex);
    }
}
