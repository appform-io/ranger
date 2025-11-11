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
package io.appform.ranger.discovery.core.resolvers;

/**
 * CriteriaResolver.java
 * Interface to help resolve from an argument A to the typed object T.
 * Keeping this as the qualified class instead of using Function so that in the future if all criteria resolvers were to
 * be fetched to register using reflections et al., there is a qualified naming.
 */
@FunctionalInterface
public interface CriteriaResolver<T, A> {

    T resolve(A args);

}
