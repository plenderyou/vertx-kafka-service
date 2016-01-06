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

import com.hubrick.vertx.kafka.producer.model.ByteKafkaMessage;
import com.hubrick.vertx.kafka.producer.model.KafkaOptions;
import com.hubrick.vertx.kafka.producer.model.StringKafkaMessage;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.ProxyIgnore;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.serviceproxy.ProxyHelper;

/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
@ProxyGen
public interface KafkaProducerService {

    static KafkaProducerService createProxy(Vertx vertx, String address) {
        return ProxyHelper.createProxy(KafkaProducerService.class, vertx, address);
    }

    static KafkaProducerService createProxyWithTimeout(Vertx vertx, String address, Long timeout) {
        return ProxyHelper.createProxy(KafkaProducerService.class, vertx, address, new DeliveryOptions().setSendTimeout(timeout));
    }

    @ProxyIgnore
    void start();

    @ProxyIgnore
    void stop();

    @ProxyIgnore
    @GenIgnore
    default void sendString(StringKafkaMessage message, Handler<AsyncResult<Void>> resultHandler) {
        sendString(message, new KafkaOptions(), resultHandler);
    }

    void sendString(StringKafkaMessage message, KafkaOptions options, Handler<AsyncResult<Void>> resultHandler);

    @ProxyIgnore
    @GenIgnore
    default void sendBytes(ByteKafkaMessage message, Handler<AsyncResult<Void>> resultHandler) {
        sendBytes(message, new KafkaOptions(), resultHandler);
    }

    void sendBytes(ByteKafkaMessage message, KafkaOptions options, Handler<AsyncResult<Void>> resultHandler);
}
