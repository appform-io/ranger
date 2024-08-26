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
package io.appform.ranger.core.finder.nodeselector;

import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceNodeSelector;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinServiceNodeSelector<T> implements ServiceNodeSelector<T> {
    private final AtomicInteger index = new AtomicInteger();

    @Override
    public ServiceNode<T> select(List<ServiceNode<T>> serviceNodes) {
        return serviceNodes.get(index.updateAndGet(i -> (i + 1) % serviceNodes.size()));
    }
}
