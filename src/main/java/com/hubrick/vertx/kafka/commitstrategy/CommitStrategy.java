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

import io.vertx.core.Future;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * User: plenderyou
 * Date: 21/04/2016
 * Time: 9:11 AM
 */
public interface CommitStrategy<K,V> {


    /**
     * Force the commit on this thread for the highest value in the map (regardless of whether it is acknowledged)
     *
     * This gets called by the re-balance listener onPartitionRevoked
     *
     *
     * @param partition
     */
    void forceCommit(Collection<TopicPartition> partition);

    /**
     * Called after the KafkaConsumer has called the handle
     *
     * N.B. This is called on the thread of the KafkaConsumerManager, synchronisation may be required
     * for the implementation of the commit strategy
     *
     * @param consumerRecord the partition of the message
     */
    void messageSent(final ConsumerRecord<K,V> consumerRecord, final Future<Void> future);


    default void messageSent(final ConsumerRecord<K,V> consumerRecord) {
        messageSent(consumerRecord, null);
    }



    /**
     * Called when message has reply has been received on the event bus
     *
     * This is called on the vertx event loop thread, synchronisation may be required
     * for the implementation of the commit strategy
     *
     * @param topicPartition the partition of the message
     * @param offset the offset that has arrived
     */
    void messageHandled(final TopicPartition topicPartition, final long offset, final Future<Void> future);

    default void messageHandled(final TopicPartition topicPartition, final long offset) {
        messageHandled(topicPartition, offset, null);
    }
}
