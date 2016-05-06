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
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * User: plenderyou
 * Date: 21/04/2016
 * Time: 9:21 AM
 */
@RunWith(VertxUnitRunner.class)
public class TimeBasedCommitStrategyTest {
    private static final Logger logger = LoggerFactory.getLogger(TimeBasedCommitStrategyTest.class);
    public static final String SOME_TEST_TOPIC = "some.test.topic";
    public static final int PARTITION = 3;


    private Vertx vertx;

    @Mock
    private KafkaConsumerManager manager;

    private final TopicPartition topicPartition = new TopicPartition(SOME_TEST_TOPIC, PARTITION);
    private final TopicPartition topicPartition1 = new TopicPartition(SOME_TEST_TOPIC, PARTITION + 1);

    private final AtomicInteger commitCalled = new AtomicInteger();
    private final List<Map<TopicPartition, OffsetAndMetadata>> captured = new ArrayList<>();
    private Supplier<Void> supplier;
    private Handler<AsyncResult<Void>> checkResult;

    @Before
    public void setUp() throws Exception {
        vertx = Vertx.vertx();
        MockitoAnnotations.initMocks(this);

        Mockito.doAnswer(new Answer<Void>() {

            @Override
            public Void answer(final InvocationOnMock invocationOnMock) throws Throwable {
                commitCalled.incrementAndGet();
                final Map<TopicPartition, OffsetAndMetadata> arguments = (Map<TopicPartition, OffsetAndMetadata>) (invocationOnMock.getArguments()[0]);
                captured.add(arguments);
                return null;
            }
        }).when(manager).commit(Mockito.anyMap());
    }

    @After
    public void after(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    // Ensure that the commit is called exactly once for normal flow
    // And that there is only one partition
    public void testCommitOnMaxMessages(final TestContext context) throws Exception {
        logger.debug("testCommitOnMaxMessages:starting");

        final Async async = context.async();
        final TestTimeBasedCommitStrategy commitStrategy = new TestTimeBasedCommitStrategy(vertx, manager, 1000, 30000);
        final long messageCount = 22;

        checkResult = ar -> {
            logger.debug("testCommitOnMaxMessages: process commits completed");
            context.assertTrue(ar.succeeded());
            context.assertEquals(1, commitCalled.get());
            context.assertEquals(1, captured.size());

            final Map<TopicPartition, OffsetAndMetadata> metadataMap = captured.get(0);
            final OffsetAndMetadata offsetAndMetadata = metadataMap.get(topicPartition);
            context.assertNotNull(offsetAndMetadata);
            validateOffSetMetadata(context, offsetAndMetadata, messageCount);
            async.complete();
        };

        supplier = () -> {

            Future<Void> future = Future.future();
            future.setHandler(checkResult);

            for (int i = 0; i < messageCount; i++) {
                final ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(topicPartition.topic(), topicPartition.partition(), i, "key" + i, "value->" + i);
                commitStrategy.messageSent(record);
                commitStrategy.messageHandled(topicPartition, i);
            }
            commitStrategy.processCommit(future);
            return null;
        };
        runItViaTheContext();
    }


    @Test
    public void testSeparatePartitionsAreCorrect(final TestContext context) {
        final Async async = context.async();
        final long messageCount = 22;
        final TestTimeBasedCommitStrategy commitStrategy = new TestTimeBasedCommitStrategy(vertx, manager, 1000, 30000);


        checkResult = ar -> {
            logger.debug("testSeparatePartitionsAreCorrect: process commits completed");
            context.assertTrue(ar.succeeded());
            context.assertEquals(1, commitCalled.get());
            context.assertEquals(1, captured.size());

            final Map<TopicPartition, OffsetAndMetadata> metadataMap = captured.get(0);
            context.assertEquals(2, metadataMap.size());
            context.assertTrue(metadataMap.containsKey(topicPartition));
            context.assertTrue(metadataMap.containsKey(topicPartition1));
            metadataMap.forEach((k, v) -> {
                validateOffSetMetadata(context, v, messageCount);
            });
            async.complete();
        };

        supplier = () -> {
            Future<Void> future = Future.future();
            future.setHandler(checkResult);

            for (int i = 0; i < messageCount; i++) {
                ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(topicPartition.topic(), topicPartition.partition(), i, "key" + i, "testSeparatePartitionsAreCorrect->" + i);
                commitStrategy.messageSent(record);
                commitStrategy.messageHandled(topicPartition, i);
                record = new ConsumerRecord<String, String>(topicPartition1.topic(), topicPartition1.partition(), i, "key" + i, "value->" + i);
                commitStrategy.messageSent(record);
                commitStrategy.messageHandled(topicPartition1, i);
            }
            commitStrategy.processCommit(future);
            return null;
        };


        runItViaTheContext();
    }


    @Test
    public void testIncompleteAcknowlegements(final TestContext context) {
        final Async async = context.async();
        logger.debug("testCommitOnMaxMessages:starting");
        final TestTimeBasedCommitStrategy commitStrategy = new TestTimeBasedCommitStrategy(vertx, manager, 1000, 30000);
        final long messageCount = 10;
        final long missing = 5;

        checkResult = ar -> {
            logger.debug("testIncompleteAcknowlegements: process commits completed");
            context.assertTrue(ar.succeeded());
            context.assertEquals(2, commitCalled.get());
            context.assertEquals(2, captured.size());


            // Check the first commit
            Map<TopicPartition, OffsetAndMetadata> metadataMap = captured.get(0);
            OffsetAndMetadata offsetAndMetadata = metadataMap.get(topicPartition);
            context.assertNotNull(offsetAndMetadata);
            validateOffSetMetadata(context, offsetAndMetadata, missing);

            // Check the second commit
            metadataMap = captured.get(1);
            offsetAndMetadata = metadataMap.get(topicPartition);
            context.assertNotNull(offsetAndMetadata);
            validateOffSetMetadata(context, offsetAndMetadata, messageCount);
            async.complete();
        };

        supplier = () -> {
            Future<Void> firstCommitFuture = Future.future();
            for (int i = 0; i < messageCount; i++) {
                final ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(topicPartition.topic(), topicPartition.partition(), i, "key" + i, "testIncompleteAcknowlegements->" + i);
                commitStrategy.messageSent(record);
                if (i != missing) {
                    commitStrategy.messageHandled(topicPartition, i);
                }
            }

            firstCommitFuture.setHandler( ar->{
                Future<Void> future = Future.future();
                future.setHandler(checkResult);
                context.assertTrue(ar.succeeded());
                commitStrategy.messageHandled(topicPartition, missing);
                commitStrategy.processCommit(future);
            } );

            commitStrategy.processCommit(firstCommitFuture);

            return null;
        };
        runItViaTheContext();
    }

    @Test
    public void testIncompleteAcknowlegementsWithTimeout(final TestContext context) {
        final Async async = context.async();
        logger.debug("testCommitOnMaxMessages:starting");
        final TestTimeBasedCommitStrategy commitStrategy = new TestTimeBasedCommitStrategy(vertx, manager, 1000, 100);
        final long messageCount = 10;
        final long missing = 5;

        checkResult = ar -> {
            logger.debug("testIncompleteAcknowlegementsWithTimeout: process commits completed");
            context.assertTrue(ar.succeeded());
            context.assertEquals(2, commitCalled.get());
            context.assertEquals(2, captured.size());

            // Check the first commit is done
            Map<TopicPartition, OffsetAndMetadata> metadataMap = captured.get(0);
            OffsetAndMetadata offsetAndMetadata = metadataMap.get(topicPartition);
            context.assertNotNull(offsetAndMetadata);
            validateOffSetMetadata(context, offsetAndMetadata, missing);

            // Check the second commit
            metadataMap = captured.get(1);
            offsetAndMetadata = metadataMap.get(topicPartition);
            context.assertNotNull(offsetAndMetadata);
            validateOffSetMetadata(context, offsetAndMetadata, messageCount);
            async.complete();
        };

        supplier = () -> {
            Future<Void> future = Future.future();

            for (int i = 0; i < messageCount; i++) {
                final ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(topicPartition.topic(), topicPartition.partition(), i, "key" + i, "primary->" + i);
                commitStrategy.messageSent(record);
                if (i != missing) {
                    commitStrategy.messageHandled(topicPartition, i);
                }
            }

            future.setHandler( checkResult );

            vertx.setTimer(200, l->{
                logger.debug("testIncompleteAcknowlegementsWithTimeout: Running second commit");
                commitStrategy.processCommit(future);
            });

            commitStrategy.processCommit(null);

            return null;
        };
        runItViaTheContext();
    }

    @Test
    public void testCommitIsNotCalled(final TestContext context){
        final Async async = context.async();

        checkResult = ar -> {
            logger.debug("testCommitIsNotCalled: process commits completed");
            context.assertTrue(ar.succeeded());
            context.assertEquals(0, commitCalled.get());
            context.assertEquals(0, captured.size());
            async.complete();
        };


        supplier = () -> {
            Future<Void> future = Future.future();
            future.setHandler( checkResult );
            final TestTimeBasedCommitStrategy commitStrategy = new TestTimeBasedCommitStrategy(vertx, manager, 1000, 100);


            commitStrategy.processCommit(future);

            return null;
        };

        runItViaTheContext();
    }

    private void runItViaTheContext() {
        vertx.runOnContext(c -> {
            supplier.get();
        });

    }


    private void validateOffSetMetadata(final TestContext context, final OffsetAndMetadata offsetAndMetadata, final long l) {
        context.assertNotNull(offsetAndMetadata);
        context.assertEquals(l, offsetAndMetadata.offset());
    }

}