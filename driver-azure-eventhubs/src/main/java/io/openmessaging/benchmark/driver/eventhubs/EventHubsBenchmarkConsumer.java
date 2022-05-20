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

import com.azure.messaging.eventhubs.EventProcessorClient;
import io.openmessaging.benchmark.driver.BenchmarkConsumer;
import io.openmessaging.benchmark.driver.ConsumerCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class EventHubsBenchmarkConsumer implements BenchmarkConsumer {

    private static final Logger log = LoggerFactory.getLogger(EventHubsBenchmarkConsumer.class);
    private final EventProcessorClient eventProcessorClient;
    private final ExecutorService executor;
    private final Future<?> consumerTask;

    public EventHubsBenchmarkConsumer(EventProcessorClient eventProcessorClient, ConsumerCallback consumerCallback) {
        this.eventProcessorClient = eventProcessorClient;
        this.executor = Executors.newSingleThreadExecutor();
        this.consumerTask = this.executor.submit(() -> {
            eventProcessorClient.start();
        });
    }


    @Override
    public void close() throws Exception {
        executor.shutdown();
        consumerTask.get();
        eventProcessorClient.stop();
    }
}
