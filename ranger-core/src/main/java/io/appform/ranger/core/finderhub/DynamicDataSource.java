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

package io.appform.ranger.core.finderhub;

import io.appform.ranger.core.model.Service;
import lombok.NoArgsConstructor;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@NoArgsConstructor
public class DynamicDataSource implements ServiceDataSource {
    private final Set<Service> serviceCollection = ConcurrentHashMap.newKeySet();

    public DynamicDataSource(Collection<Service> initialServices) {
        this.serviceCollection.addAll(initialServices);
    }

    @Override
    public Collection<Service> services() {
        return Collections.unmodifiableSet(serviceCollection);
    }

    @Override
    public void add(Service service) {
        this.serviceCollection.add(service);
    }

    @Override
    public void start() {
        // Nothing to do here
    }

    @Override
    public void stop() {
        // Nothing to do here
    }
}
