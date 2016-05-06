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
package com.hubrick.vertx.kafka.consumer;

import com.google.common.math.IntMath;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hubrick.vertx.kafka.commitstrategy.CommitStrategy;
import com.hubrick.vertx.kafka.consumer.config.KafkaConsumerConfiguration;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Function;
import java.util.regex.Pattern;

/**
 * @author Marcus Thiesen
 * @since 1.0.0
 */
public class KafkaConsumerManager {

    private final static Logger LOG = LoggerFactory.getLogger(KafkaConsumerManager.class);

    private final static ThreadFactory FACTORY = new ThreadFactoryBuilder()
                                                    .setNameFormat("kafka-consumer-thread-%d")
                                                    .setUncaughtExceptionHandler((thread, throwable) -> LOG.error("Uncaught exception in thread {}", thread.getName(), throwable))
                                                    .setDaemon(true)
                                                    .build();

    private final Vertx vertx;
    private final KafkaConsumer<String,String> consumer;
    private final KafkaConsumerConfiguration configuration;
    private final KafkaConsumerHandler handler;
    private final ExecutorService messageProcessorExececutor = Executors.newSingleThreadExecutor(FACTORY);
    private final CommitStrategy<String, String> commitStrategy;
    private final KafkaConsumerFailedHandler failHander;

    public KafkaConsumerManager(Vertx vertx, KafkaConsumer<String,String> consumer, KafkaConsumerConfiguration configuration, KafkaConsumerHandler handler, final KafkaConsumerFailedHandler failHandler, final Function<KafkaConsumerManager, CommitStrategy<String, String>> commitStrategyFunction) {
        this.vertx = vertx;
        this.consumer = consumer;
        this.configuration = configuration;
        this.handler = handler;
        this.failHander = failHandler;
        commitStrategy = commitStrategyFunction.apply(this);
    }

    public static KafkaConsumerManager create(final Vertx vertx, final KafkaConsumerConfiguration configuration, final KafkaConsumerHandler handler, final Function<KafkaConsumerManager, CommitStrategy<String, String>> commitStrategyFunction) {
        return create(vertx, configuration, handler, null, commitStrategyFunction);
    }

    public static KafkaConsumerManager create(final Vertx vertx, final KafkaConsumerConfiguration configuration, final KafkaConsumerHandler handler, final KafkaConsumerFailedHandler failHandler,  final Function<KafkaConsumerManager, CommitStrategy<String, String>> commitStrategyFunction) {
        final Properties properties = createProperties(configuration);
        final KafkaConsumer consumer = new KafkaConsumer(properties, new StringDeserializer(), new StringDeserializer());
        return new KafkaConsumerManager(vertx, consumer, configuration, handler, failHandler,  commitStrategyFunction);
    }

    protected static Properties createProperties(KafkaConsumerConfiguration configuration) {
        final Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, configuration.getClientId());
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, configuration.getBootstrapServers());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, configuration.getGroupId());
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, Boolean.FALSE.toString());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, configuration.getOffsetReset());

        return properties;
    }

    public void stop() {
        messageProcessorExececutor.shutdownNow();
        consumer.unsubscribe();
        consumer.close();
    }

    public void start() {

        final String kafkaTopicRegex = configuration.getKafkaTopicRegex();
        consumer.subscribe(Pattern.compile(kafkaTopicRegex), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
                LOG.warn("onPartitionsRevoked: has been called");
                commitStrategy.forceCommit(partitions);
            }

            @Override
            public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
                LOG.warn("onPartitionsAssigned: has bee called");
            }
        });

        messageProcessorExececutor.submit(() -> read());
    }

    private void read() {
        while (!consumer.subscription().isEmpty()) {
            final ConsumerRecords<String, String> records = consumer.poll(60000);
            final Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
            while (iterator.hasNext()) {


                final ConsumerRecord<String, String> msg = iterator.next();
                handle(msg, configuration.getMaxRetries(), configuration.getInitialRetryDelaySeconds());
        }
        }
    }

    private void handle(final ConsumerRecord<String, String> msg, int tries, int delaySeconds) {
        final Future<Void> futureResult = Future.future();
        final TopicPartition topicPartition = new TopicPartition(msg.topic(), msg.partition());
        futureResult.setHandler(result -> {
            if(result.succeeded()) {
                commitStrategy.messageHandled(topicPartition, msg.offset());
            } else {
                final int nextDelaySeconds = computeNextDelay(delaySeconds);
                if (tries > 0) {
                    LOG.error("Exception occurred during kafka message processing, will retry in {} seconds: {}", delaySeconds, msg, result.cause());
                    final int nextTry = tries - 1;
                    vertx.setTimer(delaySeconds * 1000, event -> handle(msg, nextTry, nextDelaySeconds));
                } else {
                    LOG.error("Exception occurred during kafka message processing. Max number of retries reached. Skipping message: {}", msg, result.cause());
                    commitStrategy.messageHandled(topicPartition, msg.offset());
                    if( failHander != null ) {
                        failHander.handle(msg);
                    }
                }
            }
        });
        handler.handle(msg, futureResult);
    }



    private int computeNextDelay(int delaySeconds) {
        try {
            return Math.min(IntMath.checkedMultiply(delaySeconds, 2), configuration.getMaxRetryDelaySeconds());
        } catch (ArithmeticException e) {
            return configuration.getMaxRetryDelaySeconds();
        }
    }

    public void commit(final Map<TopicPartition, OffsetAndMetadata> partitionMap) {
        consumer.commitSync(partitionMap);
    }
}
