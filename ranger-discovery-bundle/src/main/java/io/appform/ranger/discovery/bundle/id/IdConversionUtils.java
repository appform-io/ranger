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

import lombok.experimental.UtilityClass;

/**
 * Internal utility for converting between {@link Id} and {@link InternalId}.
 */
@UtilityClass
public class IdConversionUtils {

    public Id toId(final InternalId internalId) {
        return Id.builder()
                .id(internalId.getId())
                .prefix(internalId.getPrefix())
                .suffix(internalId.getSuffix())
                .generatedDate(internalId.getGeneratedDate())
                .node(internalId.getNode())
                .exponent(internalId.getExponent())
                .build();
    }

    public InternalId toInternalId(final Id id) {
        return InternalId.builder()
                .id(id.getId())
                .prefix(id.getPrefix())
                .suffix(id.getSuffix())
                .generatedDate(id.getGeneratedDate())
                .node(id.getNode())
                .exponent(id.getExponent())
                .build();
    }
}
