/**
 * Copyright (C) 2016 Etaia AS (oss@hubrick.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hubrick.vertx.kafka.producer.config;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public class KafkaProducerConfiguration {

    private final String defaultTopic;
    private final String brokerList;
    private final Integer requestAcks;
    private StatsDConfiguration statsDConfiguration;

    public KafkaProducerConfiguration(String defaultTopic,
                                      String brokerList,
                                      Integer requestAcks) {
        checkNotNull(defaultTopic, "defaultTopic must not be null");
        checkNotNull(brokerList, "brokerList must not be null");
        checkNotNull(requestAcks, "requestAcks must not be null");

        this.defaultTopic = defaultTopic;
        this.brokerList = brokerList;
        this.requestAcks = requestAcks;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public Integer getRequestAcks() {
        return requestAcks;
    }

    public StatsDConfiguration getStatsDConfiguration() {
        return statsDConfiguration;
    }

    public void setStatsDConfiguration(StatsDConfiguration statsDConfiguration) {
        this.statsDConfiguration = statsDConfiguration;
    }
}
