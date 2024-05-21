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

import io.appform.ranger.core.model.PortSchemes;
import io.dropwizard.core.Configuration;
import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpsConnectorFactory;
import io.dropwizard.core.server.DefaultServerFactory;
import io.dropwizard.core.server.ServerFactory;
import io.dropwizard.core.server.SimpleServerFactory;
import lombok.val;

import java.util.Optional;

/**
 * DefaultPortSchemeResolver.java
 * To derive PortScheme from the ServerFactory from Dropwizard startup config
 */
public class DefaultPortSchemeResolver<T extends Configuration> implements PortSchemeResolver<T> {

    /**
     * Returns a PortScheme basis the configuration. The default in case of a new
     * Connector found (Possibly on version upgrades, if we have forgotten mutate it,
     * is HTTP)
     *
     * @param configuration {@link Configuration} the dropwizard startup config
     * @return {@link String} The relevant portScheme with HTTP as default
     */
    @Override
    public String resolve(T configuration) {
        val connectionFactory = getConnectorFactory(configuration.getServerFactory());
        return connectionFactory.filter(HttpsConnectorFactory.class::isInstance)
                .map(factory -> PortSchemes.HTTPS)
                .orElse(PortSchemes.HTTP);
    }

    private Optional<ConnectorFactory> getConnectorFactory(ServerFactory serverFactory) {
        if (serverFactory instanceof DefaultServerFactory) {
            val defaultFactory = (DefaultServerFactory) serverFactory;
            return defaultFactory.getApplicationConnectors()
                    .stream()
                    .findFirst();
        } else if (serverFactory instanceof SimpleServerFactory) {
            val defaultFactory = (SimpleServerFactory) serverFactory;
            return Optional.ofNullable(defaultFactory.getConnector());
        } else {
            return Optional.empty();
        }
    }
}
