/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.openmessaging.benchmark.driver.eventhubs;

//import com.azure.messaging.eventhubs.EventDataBatch;
//import com.azure.messaging.eventhubs.EventHubProducerAsyncClient;
//import com.azure.messaging.eventhubs.EventHubProducerClient;
//import com.azure.messaging.eventhubs.models.CreateBatchOptions;
// import com.azure.messaging.eventhubs.EventDataBatch;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventDataBatch;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import io.openmessaging.benchmark.driver.BenchmarkProducer;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;

public class EventHubsBenchmarkProducer implements BenchmarkProducer {

    private final EventHubClient eventHubClient;
    private int producerBatchSize;

    public EventHubsBenchmarkProducer(EventHubClient eventHubClient, int producerBatchSize) {
        this.eventHubClient = eventHubClient;
        this.producerBatchSize = producerBatchSize;
    }

    @Override
    public CompletableFuture<Void> sendAsync(Optional<String> key, byte[] payload) {

        EventDataBatch eventDataBatch = null;
        try {
            if (producerBatchSize > 1) {
                eventDataBatch = eventHubClient.createBatch();
                for (int i = 0; i < producerBatchSize; i++) {
                    com.microsoft.azure.eventhubs.EventData event = EventData.create(payload);
                    event.getProperties().putIfAbsent("producer_timestamp", System.currentTimeMillis());
                    eventDataBatch.tryAdd(event);
                }
            } else {
                // When batching is disabled we can simply send one event at a time without using EventDataBatch.
                com.microsoft.azure.eventhubs.EventData event = EventData.create(payload);
                event.getProperties().putIfAbsent("producer_timestamp", System.currentTimeMillis());
                return eventHubClient.send(event).thenApply( unused -> null);

            }
        } catch (Exception e) {
            //ToDo Logging
            e.printStackTrace();
        }
        return eventHubClient.send(eventDataBatch).thenApply( unused -> null);
    }

    @Override
    public void close() throws Exception {
        eventHubClient.close();
    }
}
