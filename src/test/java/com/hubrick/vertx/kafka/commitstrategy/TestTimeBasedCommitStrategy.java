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
import io.vertx.core.Vertx;

/**
 * User: plenderyou
 * Date: 22/04/2016
 * Time: 12:27 PM
 */
public class TestTimeBasedCommitStrategy extends TimeBasedCommitStrategy<String, String> {
    public TestTimeBasedCommitStrategy(final Vertx vertx, final KafkaConsumerManager manager, final int commitMillis, final int maxTimeForCommits) {
        super(vertx, manager, commitMillis, maxTimeForCommits);
    }

    @Override
    protected void periodically(final Long aLong) {
        // Do nothing so we can control the running of the call
    }
}
