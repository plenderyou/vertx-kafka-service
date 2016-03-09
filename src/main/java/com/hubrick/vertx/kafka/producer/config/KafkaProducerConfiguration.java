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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public class KafkaProducerConfiguration {

    private final String defaultTopic;
    private final String bootstrapServers;
    private final String acks;
    private final int retries;
    private final int requestTimeoutMs;
    private final int maxBlockTimeMs;
    private StatsDConfiguration statsDConfiguration;

    public KafkaProducerConfiguration(String defaultTopic,
                                      String bootstrapServers,
                                      String acks,
                                      int retries,
                                      int requestTimeoutMs,
                                      int maxBlockTimeMs) {
        checkNotNull(defaultTopic, "defaultTopic must not be null");
        checkNotNull(bootstrapServers, "bootstrapServers must not be null");
        checkNotNull(acks, "acks must not be null");
        checkArgument(retries >= 0, "retries must be positive");
        checkArgument(requestTimeoutMs > 0, "requestTimeoutMs timeout must be larger than zero");
        checkArgument(maxBlockTimeMs > 0, "maxBlockTimeMs timeout must be larger than zero");


        this.defaultTopic = defaultTopic;
        this.bootstrapServers = bootstrapServers;
        this.acks = acks;
        this.retries = retries;
        this.requestTimeoutMs = requestTimeoutMs;
        this.maxBlockTimeMs = maxBlockTimeMs;
    }

    public String getDefaultTopic() {
        return defaultTopic;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getAcks() {
        return acks;
    }

    public StatsDConfiguration getStatsDConfiguration() {
        return statsDConfiguration;
    }

    public void setStatsDConfiguration(StatsDConfiguration statsDConfiguration) {
        this.statsDConfiguration = statsDConfiguration;
    }

    public int getRetries() {
        return retries;
    }

    public int getRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    public int getMaxBlockTimeMs() {
        return maxBlockTimeMs;
    }
}
