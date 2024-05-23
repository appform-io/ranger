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
package io.appform.ranger.drove;

import io.appform.ranger.drove.servicefinder.DroveShardedServiceFinderBuilder;
import io.appform.ranger.drove.servicefinder.DroveUnshardedServiceFinderBuilider;
import lombok.experimental.UtilityClass;

@UtilityClass
@SuppressWarnings("unused")
public final class DroveServiceFinderBuilders {

    public static <T> DroveShardedServiceFinderBuilder<T> droveShardedServiceFinderBuilder(){
        return new DroveShardedServiceFinderBuilder<>();
    }

    public static <T> DroveUnshardedServiceFinderBuilider<T> droveUnshardedServiceFinderBuilider(){
        return new DroveUnshardedServiceFinderBuilider<>();
    }
}
