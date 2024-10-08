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
package io.appform.ranger.zookeeper.servicefinderhub;

import com.google.common.base.Preconditions;
import io.appform.ranger.core.finderhub.ServiceFinderHub;
import io.appform.ranger.core.finderhub.ServiceFinderHubBuilder;
import io.appform.ranger.core.model.ServiceRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 *
 */
@Slf4j
public class ZkServiceFinderHubBuilder<T, R extends ServiceRegistry<T>> extends ServiceFinderHubBuilder<T, R> {
    private String namespace;
    private CuratorFramework curatorFramework;
    private String connectionString;

    public ZkServiceFinderHubBuilder<T, R> withNamespace(final String namespace) {
        this.namespace = namespace;
        return this;
    }

    public ZkServiceFinderHubBuilder<T, R> withCuratorFramework(CuratorFramework curatorFramework) {
        this.curatorFramework = curatorFramework;
        return this;
    }

    public ZkServiceFinderHubBuilder<T, R> withConnectionString(final String connectionString) {
        this.connectionString = connectionString;
        return this;
    }

    @Override
    protected void preBuild() {
        if (null == curatorFramework) {
            Preconditions.checkNotNull(connectionString);
            log.info("Building custom curator framework");
            curatorFramework = CuratorFrameworkFactory.builder()
                    .namespace(namespace)
                    .connectString(connectionString)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 100))
                    .build();
            super.withExtraStartSignalConsumer(x -> curatorFramework.start());
            super.withExtraStartSignalConsumer(x -> curatorFramework.close());
        }
    }

    @Override
    protected void postBuild(ServiceFinderHub<T, R> serviceFinderHub) {
        log.debug("No post build steps necessary");
    }
}
