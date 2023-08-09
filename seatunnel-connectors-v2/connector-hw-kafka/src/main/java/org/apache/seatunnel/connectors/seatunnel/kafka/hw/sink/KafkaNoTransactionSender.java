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

package org.apache.seatunnel.connectors.seatunnel.kafka.hw.sink;

import org.apache.seatunnel.connectors.seatunnel.kafka.hw.state.KafkaCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.kafka.hw.state.KafkaSinkState;
import org.apache.seatunnel.connectors.seatunnel.kafka.hw.utils.LoginUtil;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * This sender will send the data to the Kafka, and will not guarantee the data is committed to the
 * Kafka exactly-once.
 *
 * @param <K> key type.
 * @param <V> value type.
 */
public class KafkaNoTransactionSender<K, V> implements KafkaProduceSender<K, V> {

    private KafkaProducer<K, V> kafkaProducer;

    /** 用户自己申请的机机账号keytab文件名称 */
    private static final String USER_KEYTAB_FILE = "user.keytab";

    /** 用户自己申请的机机账号名称 */
    private static final String USER_PRINCIPAL = "venuskafka";

    public KafkaNoTransactionSender(Properties properties) {
        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    public void securityPrepare() throws IOException {
        String filePath =
                "D:\\code\\seatunnel-dev\\incubator-seatunnel\\seatunnel-connectors-v2\\connector-kafka-hw\\src\\main\\resources\\conf\\";
        String krbFile = filePath + "krb5.conf";
        String userKeyTableFile = filePath + USER_KEYTAB_FILE;

        // windows路径下分隔符替换
        userKeyTableFile = userKeyTableFile.replace("\\", "\\\\");
        krbFile = krbFile.replace("\\", "\\\\");

        LoginUtil.setKrb5Config(krbFile);
        LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");
        LoginUtil.setJaasFile(USER_PRINCIPAL, userKeyTableFile);
    }

    @Override
    public void send(ProducerRecord<K, V> producerRecord) {
        kafkaProducer.send(producerRecord);
    }

    @Override
    public void beginTransaction(String transactionId) {
        // no-op
    }

    @Override
    public Optional<KafkaCommitInfo> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void abortTransaction() {
        // no-op
    }

    @Override
    public void abortTransaction(long checkpointId) {
        // no-op
    }

    @Override
    public List<KafkaSinkState> snapshotState(long checkpointId) {
        kafkaProducer.flush();
        return Collections.emptyList();
    }

    @Override
    public void close() {
        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
