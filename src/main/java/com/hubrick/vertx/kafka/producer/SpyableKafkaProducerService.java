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

import com.hubrick.vertx.kafka.producer.model.AbstractKafkaMessage;
import com.hubrick.vertx.kafka.producer.model.ByteKafkaMessage;
import com.hubrick.vertx.kafka.producer.model.KafkaOptions;
import com.hubrick.vertx.kafka.producer.model.StringKafkaMessage;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author Emir Dizdarevic
 * @since 1.0.0
 */
public class SpyableKafkaProducerService implements KafkaProducerService {

    private static final Logger log = LoggerFactory.getLogger(SpyableKafkaProducerService.class);

    private SpyableKafkaProducerCallback callback;

    public SpyableKafkaProducerService(SpyableKafkaProducerCallback callback) {
        this.callback = callback;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public void sendString(StringKafkaMessage message, KafkaOptions options, Handler<AsyncResult<Void>> resultHandler) {
        if (!isValid(message.getPayload())) {
            log.error("Invalid kafka message provided. Message not sent to kafka...");
            sendError(resultHandler, new IllegalStateException(String.format("Invalid kafka message provided. Property [%s] is not set.", AbstractKafkaMessage.PAYLOAD)));
            return;
        }

        callback.handle(message, options, resultHandler);
    }

    @Override
    public void sendBytes(ByteKafkaMessage message, KafkaOptions options, Handler<AsyncResult<Void>> resultHandler) {
        if (!isValid(message.getPayload())) {
            log.error("Invalid kafka message provided. Message not sent to kafka...");
            sendError(resultHandler, new IllegalStateException(String.format("Invalid kafka message provided. Property [%s] is not set.", AbstractKafkaMessage.PAYLOAD)));
            return;
        }

        callback.handle(message, options, resultHandler);
    }

    private boolean isValid(String str) {
        return str != null && !str.isEmpty();
    }

    private boolean isValid(Buffer buffer) {
        return buffer != null && buffer.length() > 0;
    }

    protected void sendOK(Handler<AsyncResult<Void>> resultHandler) {
        resultHandler.handle(Future.succeededFuture());
    }

    protected void sendError(Handler<AsyncResult<Void>> resultHandler, Throwable t) {
        resultHandler.handle(Future.failedFuture(t));
    }

    @FunctionalInterface
    public interface SpyableKafkaProducerCallback<T extends AbstractKafkaMessage> {
        void handle(T message, KafkaOptions options, Handler<AsyncResult<Void>> resultHandler);
    }
}
