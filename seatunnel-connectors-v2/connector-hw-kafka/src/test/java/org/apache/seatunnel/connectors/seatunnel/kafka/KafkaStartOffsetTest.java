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
    public static String saslJaasConfig =
            "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                    + "        username=\"kafka\"\n"
                    + "        password=\"m33dt0lb6uAGR+4zgpR7jsR8YMIkXhQ=\";";

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

    @Test
    void producer() {
        Properties props = new Properties();
        System.setProperty(
                "java.security.krb5.conf",
                "D:/env/hw-auth/kafkauser_1701846620406_keytab/krb5.conf");
        props.put(
                SaslConfigs.SASL_JAAS_CONFIG,
                "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"D:/env/hw-auth/kafkauser_1701846620406_keytab/user.keytab\" principal=\"kafkauser\" debug=true;");
        props.put("bootstrap.servers", "10.68.120.90:21007,10.68.120.91:21007,10.68.120.92:21007");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        KafkaNoTransactionSender kafkaNoTransactionSender = new KafkaNoTransactionSender(props);

        for (int i = 0; i < 20; i++) {
            kafkaNoTransactionSender.send(
                    new ProducerRecord<String, String>("test-hw", "test huawei kafka!"));
        }
    }

    @Test
    void consumer() {
        Properties props = new Properties();
        System.setProperty(
                "java.security.krb5.conf",
                "D:/env/hw-auth/kafkauser_1701846620406_keytab/krb5.conf");
        props.put(
                SaslConfigs.SASL_JAAS_CONFIG,
                "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true keyTab=\"D:/env/hw-auth/kafkauser_1701846620406_keytab/user.keytab\" principal=\"kafkauser\" debug=true;");
        props.put("bootstrap.servers", "10.68.120.90:21007,10.68.120.91:21007,10.68.120.92:21007");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.kerberos.service.name", "kafka");
        props.put("sasl.mechanism", "GSSAPI");
        props.setProperty(
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test_hw");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);
        kafkaConsumer.subscribe(Arrays.asList("test-hw"));
        for (int i = 0; i < 1000; i++) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);
            // 消息处理
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(
                        "[NewConsumerExample], Received message: ("
                                + record.key()
                                + ", "
                                + record.value()
                                + ") at offset "
                                + record.offset());
            }
        }
    }
}
