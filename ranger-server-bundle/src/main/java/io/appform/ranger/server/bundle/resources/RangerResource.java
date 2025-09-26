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
package io.appform.ranger.server.bundle.resources;

import com.codahale.metrics.annotation.Timed;
import io.appform.ranger.client.RangerHubClient;
import io.appform.ranger.core.model.Service;
import io.appform.ranger.core.model.ServiceNode;
import io.appform.ranger.core.model.ServiceRegistry;
import io.appform.ranger.http.response.model.GenericResponse;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.inject.Inject;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@Path("/ranger")
public class RangerResource<T, R extends ServiceRegistry<T>> {

    private final List<RangerHubClient<T, R>> rangerHubs;

    @Inject
    public RangerResource(List<RangerHubClient<T, R>> rangerHubs) {
        this.rangerHubs = rangerHubs;
    }

    @GET
    @Path("/services/v1")
    @Timed
    public GenericResponse<Set<Service>> getServices(
            @QueryParam("skipDataFromReplicationSources") @DefaultValue("false") boolean skipDataFromReplicationSources) {
        return GenericResponse.<Set<Service>>builder()
                .data(rangerHubs.stream()
                              .filter(hub -> !skipDataFromReplicationSources || !hub.isReplicationSource())
                              .map(RangerHubClient::getRegisteredServices)
                              .flatMap(Collection::stream)
                              .collect(Collectors.toSet()))
                .build();
    }

    @GET
    @Path("/nodes/v1/{namespace}/{serviceName}")
    @Timed
    public GenericResponse<Collection<ServiceNode<T>>> getNodes(
            @NotNull @NotEmpty @PathParam("namespace") final String namespace,
            @NotNull @NotEmpty @PathParam("serviceName") final String serviceName,
            @QueryParam("skipDataFromReplicationSources") @DefaultValue("false") boolean skipDataFromReplicationSources) {
        val service = Service.builder().namespace(namespace).serviceName(serviceName).build();
        return GenericResponse.<Collection<ServiceNode<T>>>builder()
                .data(rangerHubs.stream()
                              .filter(hub -> !(skipDataFromReplicationSources && hub.isReplicationSource()))
                              .map(hub -> hub.getAllNodes(service))
                              .flatMap(List::stream)
                              .collect(Collectors.toMap(node -> node.getHost() + ":" + node.getPort(),
                                                        Function.identity(),
                                                        (oldV, newV) ->
                                                                oldV.getLastUpdatedTimeStamp() > newV.getLastUpdatedTimeStamp()
                                                                ? oldV
                                                                : newV))
                              .values())
                .build();
    }
}
