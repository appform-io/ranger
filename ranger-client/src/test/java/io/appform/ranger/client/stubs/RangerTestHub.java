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
package io.appform.ranger.client.stubs;

import com.google.common.collect.Sets;
import io.appform.ranger.client.AbstractRangerHubClient;
import io.appform.ranger.client.utils.RangerHubTestUtils;
import io.appform.ranger.core.finder.serviceregistry.ListBasedServiceRegistry;
import io.appform.ranger.core.finderhub.ServiceDataSource;
import io.appform.ranger.core.finderhub.ServiceFinderFactory;
import io.appform.ranger.core.finderhub.ServiceFinderHub;
import io.appform.ranger.core.finderhub.ServiceFinderHubBuilder;
import io.appform.ranger.core.finderhub.StaticDataSource;
import io.appform.ranger.core.units.TestNodeData;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;

@Getter
@SuperBuilder
public class RangerTestHub extends AbstractRangerHubClient<TestNodeData,
        ListBasedServiceRegistry<TestNodeData>, TestDeserializer<TestNodeData>> {

    @Builder.Default
    private final boolean useDefaultDataSource = true;

    @Override
    protected ServiceFinderHub<TestNodeData, ListBasedServiceRegistry<TestNodeData>> buildHub() {
        return new ServiceFinderHubBuilder<TestNodeData, ListBasedServiceRegistry<TestNodeData>>() {
            @Override
            protected void preBuild() {

            }

            @Override
            protected void postBuild(ServiceFinderHub<TestNodeData, ListBasedServiceRegistry<TestNodeData>> serviceFinderHub) {

            }
        }.withServiceDataSource(getServiceDataSource())
            .withServiceFinderFactory(getFinderFactory())
                .build();
    }

    @Override
    protected ServiceDataSource getDefaultDataSource() {
        return new StaticDataSource(Sets.newHashSet(RangerHubTestUtils.service));
    }

    @Override
    protected ServiceFinderFactory<TestNodeData, ListBasedServiceRegistry<TestNodeData>> getFinderFactory() {
        return new TestServiceFinderFactory();
    }
}


