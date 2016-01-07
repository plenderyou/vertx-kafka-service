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
package com.hubrick.vertx.kafka.producer;

import com.google.common.base.Strings;
import com.hubrick.vertx.kafka.producer.config.KafkaProducerConfiguration;
import com.hubrick.vertx.kafka.producer.config.StatsDConfiguration;
import com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import io.vertx.serviceproxy.ProxyHelper;

import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.*;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.BROKER_LIST;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.BROKER_LIST_DEFAULT;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.DEFAULT_TOPIC;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.REQUEST_ACKS;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.REQUEST_ACKS_DEFAULT;
import static com.hubrick.vertx.kafka.producer.property.KafkaProducerProperties.STATSD;
import static com.hubrick.vertx.kafka.producer.property.StatsDProperties.HOST_DEFAULT;
import static com.hubrick.vertx.kafka.producer.property.StatsDProperties.PORT_DEFAULT;
import static com.hubrick.vertx.kafka.producer.property.StatsDProperties.PREFIX_DEFAULT;
import static com.hubrick.vertx.kafka.producer.property.StatsDProperties.HOST;
import static com.hubrick.vertx.kafka.producer.property.StatsDProperties.PORT;
import static com.hubrick.vertx.kafka.producer.property.StatsDProperties.PREFIX;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public class KafkaProducerServiceVerticle extends AbstractVerticle {

    private KafkaProducerService kafkaProducerService;

    @Override
    public void start() {
        // Get the address of EventBus where the message was published
        final String address = config().getString(ADDRESS);
        if(Strings.isNullOrEmpty(address)) {
            throw new IllegalStateException("address must be specified in config");
        }

        // Get the address of EventBus where the message was published
        final String topic = config().getString(DEFAULT_TOPIC);
        if(Strings.isNullOrEmpty(topic)) {
            throw new IllegalStateException("topic must be specified in config");
        }


        final JsonObject statsDConfig = config().getJsonObject(STATSD);

        StatsDConfiguration statsDConfiguration = null;
        if (statsDConfig != null) {
            final String prefix = statsDConfig.getString(PREFIX, PREFIX_DEFAULT);
            final String host = statsDConfig.getString(HOST, HOST_DEFAULT);
            final int port = statsDConfig.getInteger(PORT, PORT_DEFAULT);
            statsDConfiguration = new StatsDConfiguration(host, port, prefix);
        }

        final KafkaProducerConfiguration kafkaProducerConfiguration = new KafkaProducerConfiguration(
                topic,
                config().getString(BROKER_LIST, BROKER_LIST_DEFAULT),
                config().getInteger(REQUEST_ACKS, REQUEST_ACKS_DEFAULT)
        );
        kafkaProducerConfiguration.setStatsDConfiguration(statsDConfiguration);

        kafkaProducerService = new KafkaProducerServiceImpl(kafkaProducerConfiguration);
        ProxyHelper.registerService(KafkaProducerService.class, vertx, kafkaProducerService, address);

        kafkaProducerService.start();
    }

    @Override
    public void stop() {
        if (kafkaProducerService != null) {
            kafkaProducerService.stop();
        }
    }
}
