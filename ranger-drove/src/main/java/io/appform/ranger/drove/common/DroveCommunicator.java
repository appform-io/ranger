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

package io.appform.ranger.drove.common;

import com.phonepe.drove.models.api.ExposedAppInfo;
import io.appform.ranger.core.model.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 *
 */
public interface DroveCommunicator extends AutoCloseable {
    Optional<String> leader();

    boolean healthy();

    List<String> services();

    default List<ExposedAppInfo> listNodes(final Service service) {
        return listNodes(Set.of(service)).getOrDefault(service, List.of());
    }

    Map<Service, List<ExposedAppInfo>> listNodes(Iterable<? extends Service> services);
}
