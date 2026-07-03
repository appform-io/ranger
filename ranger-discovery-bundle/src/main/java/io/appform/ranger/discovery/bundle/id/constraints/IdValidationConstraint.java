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

package io.appform.ranger.discovery.bundle.id.constraints;


import io.appform.ranger.discovery.bundle.id.Id;
import io.appform.ranger.discovery.bundle.id.InternalId;

/**
 * Validates a generated id.
 * <p>
 * {@code isValid(Id)} remains the abstract method (and thus the binary-compatible contract) so that
 * pre-existing implementations compiled against older versions of this interface continue to work
 * unchanged. {@code isValid(InternalId)} is provided as a default that adapts to the legacy contract;
 * implementations that want to avoid the {@link InternalId} -&gt; {@link Id} conversion (e.g. because
 * they only need fields present on {@link InternalId}) can override it directly.
 */
public interface IdValidationConstraint {

    boolean isValid(final Id id);

    default boolean isValid(final InternalId internalId) {
        return isValid(toId(internalId));
    }

    default boolean failFast() {
        return false;
    }

    static Id toId(final InternalId internalId) {
        return Id.builder()
                .id(internalId.getId())
                .prefix(internalId.getPrefix())
                .suffix(internalId.getSuffix())
                .generatedDate(internalId.getGeneratedDate())
                .node(internalId.getNode())
                .exponent(internalId.getExponent())
                .build();
    }

    static InternalId toInternalId(final Id id) {
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
