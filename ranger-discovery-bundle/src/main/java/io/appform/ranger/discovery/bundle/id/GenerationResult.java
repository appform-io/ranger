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

import lombok.Getter;
import lombok.Value;

/**
 * Internal implementation detail of the id generation pipeline (used across the {@code generator}
 * and {@code nonce} subpackages). This is <b>not</b> part of the public API contract — its shape may
 * change between releases without being treated as a breaking change.
 */
@Getter
@Value
public class GenerationResult {
    int exponent;
    long time;
    InternalId internalId;
    IdValidationState state;
    Domain domain;
}
