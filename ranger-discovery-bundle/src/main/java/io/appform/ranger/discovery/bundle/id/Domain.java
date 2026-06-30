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

package io.appform.ranger.discovery.bundle.id;


import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Getter
public class Domain {
    public static final String DEFAULT_DOMAIN_NAME = "__DEFAULT_DOMAIN__";
    public static final Domain DEFAULT = new Domain(DEFAULT_DOMAIN_NAME,
                                                    List.of(),
                                                    TimeUnit.MILLISECONDS);

    private final String domain;
    private final List<IdValidationConstraint> constraints;
    private final CollisionChecker collisionChecker;


    @Builder
    public Domain(@NonNull String domain,
                  @NonNull List<IdValidationConstraint> constraints,
                  TimeUnit resolution) {
        this.domain = domain;
        this.constraints = constraints;
        this.collisionChecker = new CollisionChecker(Objects.requireNonNullElse(resolution, TimeUnit.MILLISECONDS));
    }
}
