/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.runtime;

import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

public class SinkConnectorOffsetFlushTest {

    @Test
    public void testOffsetFlushIntervalFallbackToWorkerConfig() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerProps.put("offset.flush.interval.ms", "30000");
        workerProps.put("connector.offset.flush.interval.override.enable", "true");
        WorkerConfig workerConfig = new StandaloneConfig(workerProps);

        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("name", "test-connector");
        connectorProps.put("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector");
        // No offset.flush.interval.ms specified at connector level

        Plugins plugins = mock(Plugins.class);
        ConnectorConfig connectorConfig = new ConnectorConfig(plugins, connectorProps);

        // Should fall back to worker config
        assertEquals(30000L, connectorConfig.offsetFlushInterval(workerConfig));
    }

    @Test
    public void testOffsetFlushIntervalConnectorOverride() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerProps.put("offset.flush.interval.ms", "30000");
        workerProps.put("connector.offset.flush.interval.override.enable", "true");
        WorkerConfig workerConfig = new StandaloneConfig(workerProps);

        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("name", "test-connector");
        connectorProps.put("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector");
        connectorProps.put("offset.flush.interval.ms", "10000"); // Override at connector level

        Plugins plugins = mock(Plugins.class);
        ConnectorConfig connectorConfig = new ConnectorConfig(plugins, connectorProps);

        // Should use connector-specific config
        assertEquals(10000L, connectorConfig.offsetFlushInterval(workerConfig));
    }

    @Test
    public void testOffsetFlushIntervalOverrideDisabled() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerProps.put("offset.flush.interval.ms", "30000");
        workerProps.put("connector.offset.flush.interval.override.enable", "false"); // Disable overrides
        WorkerConfig workerConfig = new StandaloneConfig(workerProps);

        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("name", "test-connector");
        connectorProps.put("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector");
        connectorProps.put("offset.flush.interval.ms", "10000"); // This should be ignored

        Plugins plugins = mock(Plugins.class);
        ConnectorConfig connectorConfig = new ConnectorConfig(plugins, connectorProps);

        // Should use worker config even though connector specifies different value
        assertEquals(30000L, connectorConfig.offsetFlushInterval(workerConfig));
    }

    @Test
    public void testOffsetFlushIntervalZeroValue() {
        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("key.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("offset.storage.file.filename", "/tmp/connect.offsets");
        workerProps.put("offset.flush.interval.ms", "30000");
        workerProps.put("connector.offset.flush.interval.override.enable", "true");
        WorkerConfig workerConfig = new StandaloneConfig(workerProps);

        Map<String, String> connectorProps = new HashMap<>();
        connectorProps.put("name", "test-connector");
        connectorProps.put("connector.class", "org.apache.kafka.connect.file.FileStreamSinkConnector");
        connectorProps.put("offset.flush.interval.ms", "0"); // Minimum allowed value

        Plugins plugins = mock(Plugins.class);
        ConnectorConfig connectorConfig = new ConnectorConfig(plugins, connectorProps);

        // Should use connector-specific config even if it's 0
        assertEquals(0L, connectorConfig.offsetFlushInterval(workerConfig));
    }
}
