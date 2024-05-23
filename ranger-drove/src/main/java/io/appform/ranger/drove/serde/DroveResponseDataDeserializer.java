/*
 * Copyright 2015 Flipkart Internet Pvt. Ltd.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.appform.ranger.drove.serde;

import com.phonepe.drove.models.api.ExposedAppInfo;
import io.appform.ranger.core.healthcheck.HealthcheckStatus;
import io.appform.ranger.core.model.Deserializer;
import io.appform.ranger.core.model.ServiceNode;
import lombok.val;

import java.util.List;

/**
 *
 */
public abstract class DroveResponseDataDeserializer<T> implements Deserializer<T> {
    public final List<ServiceNode<T>> deserialize(List<ExposedAppInfo> appInfo) {
        val currTime = System.currentTimeMillis();
        return appInfo.stream()
                .flatMap(appEndpoints ->
                                 appEndpoints.getHosts()
                                         .stream()
                                         .map(endpoint -> new ServiceNode<>(endpoint.getHost(),
                                                                            endpoint.getPort(),
                                                                            translate(appEndpoints, endpoint),
                                                                            HealthcheckStatus.healthy,
                                                                            currTime,
                                                                            endpoint.getPortType().name())))
                .toList();
    }

    protected abstract T translate(final ExposedAppInfo appInfo, final ExposedAppInfo.ExposedHost host);
}
