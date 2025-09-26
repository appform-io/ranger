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
package io.appform.ranger.server.bundle;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.appform.ranger.client.RangerHubClient;
import io.appform.ranger.client.stubs.RangerTestHub;
import io.appform.ranger.client.utils.RangerHubTestUtils;
import io.appform.ranger.core.finder.serviceregistry.ListBasedServiceRegistry;
import io.appform.ranger.core.units.TestNodeData;
import io.appform.ranger.core.utils.RangerTestUtils;
import io.dropwizard.core.Configuration;
import io.dropwizard.core.setup.AdminEnvironment;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.jersey.DropwizardResourceConfig;
import io.dropwizard.jersey.setup.JerseyEnvironment;
import io.dropwizard.lifecycle.setup.LifecycleEnvironment;
import lombok.val;
import org.eclipse.jetty.util.component.LifeCycle;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;

import static io.appform.ranger.client.utils.RangerHubTestUtils.service;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class RangerServerBundleTest {

    private static final JerseyEnvironment JERSEY_ENVIRONMENT = mock(JerseyEnvironment.class);
    private static final MetricRegistry METRIC_REGISTRY = mock(MetricRegistry.class);
    private static final LifecycleEnvironment LIFECYCLE_ENVIRONMENT = new LifecycleEnvironment(METRIC_REGISTRY);
    private static final Environment ENVIRONMENT = mock(Environment.class);
    private static final Bootstrap<?> BOOTSTRAP = mock(Bootstrap.class);
    private static final Configuration CONFIGURATION = mock(Configuration.class);

    private static final RangerServerBundle<TestNodeData, ListBasedServiceRegistry<TestNodeData>, Configuration>
            RANGER_SERVER_BUNDLE = new RangerServerBundle<>() {

        @Override
        protected List<RangerHubClient<TestNodeData, ListBasedServiceRegistry<TestNodeData>>> withHubs(Configuration configuration) {
            return Collections.singletonList(RangerHubTestUtils.getTestHub());
        }

        @Override
        protected List<HealthCheck> withHealthChecks(Configuration configuration) {
            return Collections.emptyList();
        }
    };

    @BeforeAll
    static void setup() throws Exception {
        when(JERSEY_ENVIRONMENT.getResourceConfig()).thenReturn(new DropwizardResourceConfig());
        when(ENVIRONMENT.jersey()).thenReturn(JERSEY_ENVIRONMENT);
        when(ENVIRONMENT.lifecycle()).thenReturn(LIFECYCLE_ENVIRONMENT);
        when(ENVIRONMENT.getObjectMapper()).thenReturn(new ObjectMapper());
        val adminEnvironment = mock(AdminEnvironment.class);
        doNothing().when(adminEnvironment).addTask(any());
        when(ENVIRONMENT.admin()).thenReturn(adminEnvironment);

        val healthCheckRegistry = mock(HealthCheckRegistry.class);
        doNothing().when(healthCheckRegistry).register(anyString(), any());
        when(ENVIRONMENT.healthChecks()).thenReturn(healthCheckRegistry);

        RANGER_SERVER_BUNDLE.initialize(BOOTSTRAP);
        RANGER_SERVER_BUNDLE.run(CONFIGURATION, ENVIRONMENT);
        for (val lifeCycle : LIFECYCLE_ENVIRONMENT.getManagedObjects()) {
            lifeCycle.start();
        }
    }


    @Test
    void testRangerBundle() {
        var hub = RANGER_SERVER_BUNDLE.getHubs().get(0);
        assertInstanceOf(RangerTestHub.class, hub);
        var node = hub.getNode(service).orElse(null);
        Assertions.assertNotNull(node);
        assertTrue(node.getHost().equalsIgnoreCase("localhost"));
        Assertions.assertEquals(9200, node.getPort());
        Assertions.assertEquals(1, node.getNodeData().getShardId());
        Assertions.assertNull(hub.getNode(RangerTestUtils.getService("test", "test")).orElse(null));
        Assertions.assertNull(hub.getNode(service, nodeData -> nodeData.getShardId() == 2).orElse(null));
        Assertions.assertNull(hub.getNode(RangerTestUtils.getService("test", "test"),
                                      nodeData -> nodeData.getShardId() == 1).orElse(null));
    }

    @AfterAll
    static void tearDown() throws Exception {
        for (LifeCycle lifeCycle : LIFECYCLE_ENVIRONMENT.getManagedObjects()) {
            lifeCycle.stop();
        }
    }
}
