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

package io.appform.ranger.core.util;

import lombok.experimental.UtilityClass;

import java.util.Objects;

/**
 * Utility calss . To be removed when we move to J11
 */
@UtilityClass
public class ObjectUtils {
    public <T> T requireNonNullElse(final T value, final T defaultValue) {
        Objects.requireNonNull(defaultValue, "Default cannot be null");
        return null != value ? value : defaultValue;
    }
}
