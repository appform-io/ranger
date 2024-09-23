/*
 * Copyright 2024 Authors, Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.appform.ranger.drove.servicefinder;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.phonepe.drove.models.api.ExposedAppInfo;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.drove.common.DroveCommunicationException;
import io.appform.ranger.drove.common.DroveCommunicator;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import io.appform.ranger.drove.serde.DroveResponseDataDeserializer;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static com.phonepe.drove.models.application.PortType.TCP;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
class DroveNodeDataSourceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final class DNodeData {

    }

    private static final class DNodeDataDeserializer extends DroveResponseDataDeserializer<DNodeData> {

        @Override
        protected DNodeData translate(ExposedAppInfo appInfo, ExposedAppInfo.ExposedHost host) {
            return new DNodeData();
        }
    }

    @Test
    @SneakyThrows
    void testSuccess() {
        val service = Service.builder()
                .namespace("testns")
                .serviceName("TEST_APP")
                .build();
        val config = DroveUpstreamConfig.builder()
                .skipCaching(true)
                .endpoints(List.of())
                .build();
        val droveClient = mock(DroveCommunicator.class);

        when(droveClient.listNodes(any(Service.class)))
                .thenReturn(List.of(new ExposedAppInfo("TEST_APP",
                                                       "A1",
                                                       "xx.x.xx",
                                                       Map.of(),
                                                       List.of(new ExposedAppInfo.ExposedHost("h1", 100, TCP),
                                                               new ExposedAppInfo.ExposedHost("h2", 100, TCP)))));
        val ds = new DroveNodeDataSource<DNodeData, DNodeDataDeserializer>(
                service,
                config,
                MAPPER,
                droveClient);
        ds.start();
        val res = ds.refresh(new DNodeDataDeserializer()).orElse(null);
        assertNotNull(res);
        assertEquals(2, res.size());
        ds.stop();
    }

    @Test
    void testUpstreamFailure() {
        val service = Service.builder()
                .namespace("testns")
                .serviceName("TEST_APP")
                .build();
        val config = DroveUpstreamConfig.builder()
                .skipCaching(true)
                .endpoints(List.of())
                .build();
        val droveClient = mock(DroveCommunicator.class);

        when(droveClient.listNodes(any(Service.class)))
                .thenThrow(new DroveCommunicationException("test"));
        val ds = new DroveNodeDataSource<DNodeData, DNodeDataDeserializer>(
                service,
                config,
                MAPPER,
                droveClient);
        ds.start();
        val res = ds.refresh(new DNodeDataDeserializer()).orElse(null);
        assertNull(res);
        ds.stop();
    }
}