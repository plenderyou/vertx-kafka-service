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
package com.hubrick.vertx.kafka.commitstrategy;

import com.hubrick.vertx.kafka.consumer.KafkaConsumerManager;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * User: plenderyou
 * Date: 22/04/2016
 * Time: 8:50 AM
 */
public class TimeBasedCommitStrategy<K, V> implements CommitStrategy<K, V> {

    private static final Logger logger = LoggerFactory.getLogger(TimeBasedCommitStrategy.class);

    private final Vertx vertx;
    private final KafkaConsumerManager manager;
    private final Map<TopicPartition, PartitionInformation> partitionMap = new ConcurrentHashMap<>();
    private final int maxTimeForCommits;
    private final int commmitMillis;

    public TimeBasedCommitStrategy(final Vertx vertx, final KafkaConsumerManager manager, final int commitMillis, final int maxTimeForCommits) {
        this.vertx = vertx;
        this.manager = manager;
        this.commmitMillis = commitMillis;
        this.maxTimeForCommits = maxTimeForCommits;
        vertx.setPeriodic(commitMillis, this::periodically);
    }


    private PartitionInformation info(final TopicPartition topicPartition) {
        PartitionInformation partitionInformation = partitionMap.get(topicPartition);
        if (partitionInformation == null) {
            partitionInformation = new PartitionInformation(topicPartition);
            partitionMap.put(topicPartition, partitionInformation);
        }
        return partitionInformation;
    }

    @Override
    public void messageHandled(final TopicPartition topicPartition, final long offset, final Future<Void> externalFuture) {
        logger.debug("messageHandled: Message for Topic[{}], partition[{}], offset[{}]",  topicPartition.topic(), topicPartition.partition(), offset);

        runSafely(f -> {
            // Here we do the work to add the arrived offset
            PartitionInformation info = info(topicPartition);
            info.arrived(offset);
            f.complete();
        }, externalFuture);

    }

    @Override
    public void messageSent(final ConsumerRecord<K, V> consumerRecord, final Future<Void> externalFuture) {
        logger.debug("messageSent: Message for Topic[{}], partition[{}], offset[{}]",  consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset());

        runSafely(f -> {
            final TopicPartition topicPartition = new TopicPartition(consumerRecord.topic(), consumerRecord.partition());
            PartitionInformation info = partitionMap.get(topicPartition);
            if( info == null ) {
                logger.warn("messageSent: No Partition information exists for {}", topicPartition);
            } else {
                info.sent(consumerRecord.offset());
            }
            f.complete();

        }, externalFuture);
    }


    @Override
    public void forceCommit(final Collection<TopicPartition> partition) {
        final CountDownLatch latch = new CountDownLatch(1);
        Future<Void> future = Future.future();

        future.setHandler(ar -> {
            logger.info("forceCommit: status {}", ar.succeeded());
            latch.countDown();
        });

        runSafely(f -> {

            final Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
            partitionMap.entrySet().removeIf(es -> {
                if (partition.contains(es.getKey())) {
                    final PartitionInformation pi = es.getValue();
                    long maxToCommit = pi.maxSent();
                    map.put(pi.topicPartition, new OffsetAndMetadata(maxToCommit));
                    return true;
                }

                return false;
            });

            manager.commit(map);
        }, future);

        try {
            latch.await(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("forceCommit: Commit took longer than 10 seconds");
        }

    }

    protected void periodically(final Long aLong) {

        logger.debug("periodically: Starting commit round");
        // Get the latest offsets for each partition
        // so that we can commit this
        processCommit(null);

    }

    protected void processCommit(final Future<Void> externalFuture) {
        runOnWorker(f -> {
            Map<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
            partitionMap.forEach((tp, pi) -> {
                Optional.<OffsetAndMetadata>ofNullable(pi.offsetAndMetadata()).ifPresent(omd -> map.put(tp, omd));
            });
            if( !map.isEmpty() ) {
                manager.commit(map);
                map.forEach((k, v) -> {
                    final PartitionInformation partitionInformation = partitionMap.get(k);
                    // NOTE this is -1 because the offset is the next record to commit
                    partitionInformation.successfulCommit(v);
                });
            }
            f.complete();

        }, externalFuture);
    }


    public void runSafely(final Consumer<Future<Void>> consumer, final Future<Void> externalFuture) {
        // Offloading this to a worker thread as it's called on the event loop
        final String threadName = Thread.currentThread().getName();
        if (!threadName.startsWith("vert.x-eventloop")) {
            logger.info("runSafely: Should be started on an event loop thread");
            vertx.runOnContext(v -> {
                runOnWorker(consumer, externalFuture);
            });
        } else {
            runOnWorker(consumer, externalFuture);
        }
    }

    protected void runOnWorker(final Consumer<Future<Void>> consumer, final Future<Void> externalFuture) {
        vertx.<Void>executeBlocking(
            x -> {
                consumer.accept(x);
            },
            true,
            y -> {
                if (y.failed()) {
                    logger.error("runSafely: Failed to process commit strategy", y.cause());
                    if (externalFuture != null) {
                        externalFuture.fail(y.cause());
                    }
                } else {
                    logger.debug("runSafely: Completed ok");
                    if (externalFuture != null) {
                        externalFuture.complete();
                    }
                }
            }
        );
    }




    private class PartitionInformation {
        private final TopicPartition topicPartition;
        private final ConcurrentSkipListMap<Long, OffsetInfo> offsetMap;
        private long lastCommittedRecord = -1;

        private PartitionInformation(final TopicPartition topicPartition) {
            this.topicPartition = topicPartition;
            this.offsetMap = new ConcurrentSkipListMap<>();
        }


        public void sent(final long offset) {
            offsetMap.put(offset, new OffsetInfo(offset));
        }

        public void arrived(final long offset) {
            if( offset <= lastCommittedRecord ) {
                // OOPS this is aleady committed
                logger.error("arrived: A Previously commit record has arrived {}, offset", topicPartition, offset);
                return;
            }
            final OffsetInfo offsetInfo = offsetMap.get(offset);
            if( offsetInfo == null ) {
                logger.warn("arrived: Offset[{}] has never been sent", offset);
            }
            offsetInfo.arrived = new Date();

        }

        /**
         * Calculates the OffsetAndMetadata for this partition
         *
         * @return OffsetAndMetadata the metadata to return NULL if there's nothing to commit
         */
        public OffsetAndMetadata offsetAndMetadata() {
            // Nothing to commit
            if (offsetMap.isEmpty()) {
                return null;
            }

            // There should never be a timeout on the processing of records because the
            // outbound call
            long toCommit = lastCommittedRecord;
            long now = System.currentTimeMillis();
            final Iterator<Map.Entry<Long, OffsetInfo>> iterator = offsetMap.entrySet().iterator();
            while( iterator.hasNext() ) {
                final Map.Entry<Long, OffsetInfo> offsetEntry = iterator.next();
                final OffsetInfo offsetInfo = offsetEntry.getValue();
                logger.debug("offsetAndMetadata: Checking {}", offsetInfo.getOffset());

                if( offsetInfo.notArrivedInTime(now) ) {
                    break;
                }

                if ((offsetInfo.getOffset() - toCommit != 1) && toCommit > 0) {
                    logger.error("offsetAndMetadata: Looks like a missing offset {} - {}", offsetInfo.getOffset(), toCommit);
                    // Do nothing
                }
                toCommit = offsetEntry.getKey();
            }
            if (toCommit > 0)
            {
                // the commit call is for the next record
                return new OffsetAndMetadata(toCommit + 1);
            }
            return null;
        }

        public void successfulCommit(final OffsetAndMetadata meta) {
            this.lastCommittedRecord = meta.offset() -1;
            offsetMap.entrySet().removeIf(e-> e.getKey() <= lastCommittedRecord);
        }

        public long maxSent() {
            return offsetMap.lastKey();
        }
    }

    private class OffsetInfo {
        private final Long offset;
        private Date arrived = null;
        private final Date sent = new Date();

        public OffsetInfo(final long offset) {
            this.offset = offset;
        }

        public Long getOffset() {
            return offset;
        }

        public boolean notArrivedInTime(final long now) {
            long delay = now - sent.getTime();
            if (arrived == null && delay <= maxTimeForCommits) {
                logger.error("notArrivedInTime: Offset [{}] has not arrived yet {} milliseconds before force", offset, maxTimeForCommits - delay);
                return true;
            }
            return false;
        }
    }
}
