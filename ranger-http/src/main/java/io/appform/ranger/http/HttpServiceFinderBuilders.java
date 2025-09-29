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
package io.appform.ranger.http;

import io.appform.ranger.http.servicefinder.HttpShardedServiceFinderBuilder;
import io.appform.ranger.http.servicefinder.HttpUnshardedServiceFinderBuilder;
import lombok.experimental.UtilityClass;

@UtilityClass
@SuppressWarnings("unused")
public final class HttpServiceFinderBuilders {

    public static <T> HttpShardedServiceFinderBuilder<T> httpShardedServiceFinderBuilder(){
        return new HttpShardedServiceFinderBuilder<>();
    }

    public static <T> HttpUnshardedServiceFinderBuilder<T> httpUnshardedServiceFinderBuilider(){
        return new HttpUnshardedServiceFinderBuilder<>();
    }
}
