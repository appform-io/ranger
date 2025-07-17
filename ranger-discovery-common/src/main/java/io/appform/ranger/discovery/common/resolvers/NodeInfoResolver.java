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
package io.appform.ranger.discovery.common.resolvers;

import io.appform.ranger.common.server.ShardInfo;
import io.appform.ranger.discovery.common.ServiceDiscoveryConfiguration;

/**
 * NodeInfoResolver.java
 * Interface to help build a node to be saved in the discovery backend while building the serviceProvider.
 * To define your custom nodeData {@link ShardInfo}, please define your own implementation.
 */
@FunctionalInterface
public interface NodeInfoResolver extends CriteriaResolver<ShardInfo, ServiceDiscoveryConfiguration> {

}
