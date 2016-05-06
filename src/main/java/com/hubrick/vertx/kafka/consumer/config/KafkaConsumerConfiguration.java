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
package com.hubrick.vertx.kafka.consumer.config;

/**
 * Configuration Options for the Kafka Consumer.
 *
 * @author Marcus Thiesen
 * @since 1.0.0
 */
public class KafkaConsumerConfiguration {

    private final String clientId;
    private final String groupId;
    private final String kafkaTopicRegex;
    private final String bootstrapServers;
    private final String offsetReset;
    private final int maxRetries;
    private final int initialRetryDelaySeconds;
    private final int maxRetryDelaySeconds;
    private final long eventBusSendTimeout;

    private KafkaConsumerConfiguration(final String groupId,
                                       final String clientId,
                                       final String kafkaTopicRegex,
                                       final String bootstrapServers,
                                       final String offsetReset,
                                       final int maxRetries,
                                       final int initialRetryDelaySeconds,
                                       final int maxRetryDelaySeconds,
                                       final long eventBusSendTimeout) {
        this.clientId = clientId;
        this.groupId = groupId;
        this.kafkaTopicRegex = kafkaTopicRegex;
        this.bootstrapServers = bootstrapServers;
        this.offsetReset = offsetReset;
        this.maxRetries = maxRetries;
        this.initialRetryDelaySeconds = initialRetryDelaySeconds;
        this.maxRetryDelaySeconds = maxRetryDelaySeconds;
        this.eventBusSendTimeout = eventBusSendTimeout;
    }

    public static KafkaConsumerConfiguration create(final String groupId,
                                                    final String clilentId,
                                                    final String kafkaTopic,
                                                    final String bootstrapServers,
                                                    final String offsetReset,
                                                    final int maxRetries,
                                                    final int initialRetryDelaySeconds,
                                                    final int maxRetryDelaySeconds,
                                                    final long eventBusSendTimeout) {
        return new KafkaConsumerConfiguration(
                groupId,
                clilentId,
                kafkaTopic,
                bootstrapServers,
                offsetReset,
                maxRetries,
                initialRetryDelaySeconds,
                maxRetryDelaySeconds,
                eventBusSendTimeout);
    }

    public String getGroupId() {
        return groupId;
    }

    public String getKafkaTopicRegex() {
        return kafkaTopicRegex;
    }

    public String getOffsetReset() {
        return offsetReset;
    }

    public int getMaxRetryDelaySeconds() {
        return maxRetryDelaySeconds;
    }

    public int getInitialRetryDelaySeconds() {
        return initialRetryDelaySeconds;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public long getEventBusSendTimeout() {
        return eventBusSendTimeout;
    }

    public String getClientId() {
        return clientId;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }
}
