/*
 * Copyright 2015 Flipkart Internet Pvt. Ltd.
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
package io.appform.ranger.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import io.appform.ranger.client.utils.CriteriaUtils;
import io.appform.ranger.core.finderhub.ServiceDataSource;
import io.appform.ranger.core.finderhub.ServiceFinderFactory;
import io.appform.ranger.core.finderhub.ServiceFinderHub;
import io.appform.ranger.core.finderhub.StaticDataSource;
import io.appform.ranger.core.model.Deserializer;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceRegistry;
import lombok.Builder;
import lombok.Getter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.function.Predicate;

@Slf4j
@Getter
@SuperBuilder
public abstract class AbstractRangerHubClient<T, R extends ServiceRegistry<T>, D extends Deserializer<T>> implements RangerHubClient<T> {

    private final String namespace;
    private final ObjectMapper mapper;
    private final D deserializer;
    private final Predicate<T> initialCriteria;
    private final boolean alwaysUseInitialCriteria;
    private int nodeRefreshTimeMs;
    private ServiceFinderHub<T, R> hub;
    @Builder.Default
    private Set<Service> services = Collections.emptySet();

    @Override
    public void start(){
        Preconditions.checkNotNull(mapper, "Mapper can't be null");
        Preconditions.checkNotNull(namespace, "namespace can't be null");
        Preconditions.checkNotNull(deserializer, "deserializer can't be null");

        if (this.nodeRefreshTimeMs < RangerClientConstants.MINIMUM_REFRESH_TIME) {
            log.warn("Node info update interval too low: {} ms. Has been upgraded to {} ms ",
                    this.nodeRefreshTimeMs,
                    RangerClientConstants.MINIMUM_REFRESH_TIME);
        }
        this.nodeRefreshTimeMs = Math.max(RangerClientConstants.MINIMUM_REFRESH_TIME, this.nodeRefreshTimeMs);
        this.hub = buildHub();
        this.hub.start();
    }

    @Override
    public void stop(){
        hub.stop();
    }

    @Override
    public Optional<ServiceNode<T>> getNode(
            final Service service
    ){
        return getNode(service, initialCriteria);
    }

    @Override
    public List<ServiceNode<T>> getAllNodes(
            final Service service
    ){
        return getAllNodes(service, initialCriteria);
    }

    @Override
    public Optional<ServiceNode<T>> getNode(
            final Service service,
            final Predicate<T> criteria
    ){
        return  this.getHub().finder(service).flatMap(trServiceFinder -> trServiceFinder.get(
                CriteriaUtils.getCriteria(alwaysUseInitialCriteria, initialCriteria, criteria))
        );
    }

    @Override
    public List<ServiceNode<T>> getAllNodes(
            final Service service,
            final Predicate<T> criteria
    ){
        return this.getHub().finder(service).map(trServiceFinder -> trServiceFinder.getAll(
                CriteriaUtils.getCriteria(alwaysUseInitialCriteria, initialCriteria, criteria))
        ).orElse(Collections.emptyList());
    }

    @Override
    public Collection<Service> getRegisteredServices() {
        try{
            return this.getHub().getServiceDataSource().services();
        }catch (Exception e){
            log.error("Call to the hub failed with exception, {}", e.getMessage());
            return Collections.emptySet();
        }
    }

    protected abstract ServiceDataSource getDataStore();

    protected ServiceDataSource withServiceDataStore(){
        return !services.isEmpty() ? new StaticDataSource(services) : getDataStore();
    }

    protected abstract ServiceFinderFactory<T, R> withFinderFactory();

    protected abstract ServiceFinderHub<T, R> buildHub();

}

