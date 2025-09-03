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

package io.appform.ranger.core;

import lombok.experimental.UtilityClass;

/**
 * Constants
 */
@UtilityClass
public class Constants {
    public static final long DEFAULT_MIN_NODE_AGE = 60000L;
    public static final double DEFAULT_BOOST_FACTOR = 1.0;
    public static final int DEFAULT_WEIGHTED_SELECTION_THRESHOLD = 10;
    public static final double DEFAULT_ROUTING_WEIGHT = 1.0;
}
