/*
 * Copyright (c) 2023 Santanu Sinha <santanu.sinha@gmail.com>
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
 *
 */
package io.appform.ranger.discovery.bundle.resolvers;

import com.google.common.collect.Lists;
import io.dropwizard.Configuration;
import io.dropwizard.jetty.HttpConnectorFactory;
import io.dropwizard.jetty.HttpsConnectorFactory;
import io.dropwizard.server.DefaultServerFactory;
import io.dropwizard.server.SimpleServerFactory;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DefaultPortSchemeResolverTest {

    @Test
    void testPortSchemeDefaultServerFactory() {
        val server = mock(DefaultServerFactory.class);
        val connectorFactory = mock(HttpConnectorFactory.class);
        when(server.getApplicationConnectors()).thenReturn(Lists.newArrayList(connectorFactory));
        val resolver = new DefaultPortSchemeResolver<>();
        val configuration = mock(Configuration.class);
        when(configuration.getServerFactory()).thenReturn(server);
        Assertions.assertEquals("http", resolver.resolve(configuration));
    }

    @Test
    void testPortSchemeSimpleServerFactory() {
        val server = mock(SimpleServerFactory.class);
        val connectorFactory = mock(HttpsConnectorFactory.class);
        when(server.getConnector()).thenReturn(connectorFactory);
        val resolver = new DefaultPortSchemeResolver<>();
        val configuration = mock(Configuration.class);
        when(configuration.getServerFactory()).thenReturn(server);
        Assertions.assertEquals("https", resolver.resolve(configuration));
    }

    @Test
    void testPortSchemeDefault() {
        val server = mock(SimpleServerFactory.class);
        when(server.getConnector()).thenReturn(null);
        val resolver = new DefaultPortSchemeResolver<>();
        val configuration = mock(Configuration.class);
        when(configuration.getServerFactory()).thenReturn(server);
        Assertions.assertEquals("http", resolver.resolve(configuration));
    }
}
