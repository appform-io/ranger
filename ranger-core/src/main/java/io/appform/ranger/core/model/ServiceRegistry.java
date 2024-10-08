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
package io.appform.ranger.core.model;

import java.util.concurrent.atomic.AtomicBoolean;

import java.util.List;
import lombok.Getter;

public abstract class ServiceRegistry<T> {
    @Getter
    private final Service service;
    private final AtomicBoolean refreshed = new AtomicBoolean(false);

    public abstract List<ServiceNode<T>> nodeList();

    public void updateNodes(List<ServiceNode<T>> nodes) {
        update(nodes);
        refreshed.set(true);
    }

    public boolean isRefreshed() {
        return refreshed.get();
    }

    protected ServiceRegistry(Service service) {
        this.service = service;
    }

    protected abstract void update(List<ServiceNode<T>> nodes);
}
