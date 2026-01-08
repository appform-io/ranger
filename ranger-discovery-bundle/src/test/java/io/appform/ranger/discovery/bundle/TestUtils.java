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

package io.appform.ranger.discovery.bundle;

import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.request.IdGenerationRequestV2;
import io.dropwizard.Configuration;
import lombok.experimental.UtilityClass;
import org.awaitility.Awaitility;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;


/**
 *
 */
@UtilityClass
public class TestUtils {
    public static<T extends Configuration> void assertNodePresence(ServiceDiscoveryBundle<T> bundle) {
        Awaitility.await()
                .pollInterval(Duration.ofSeconds(1))
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertNotNull(bundle.getServiceDiscoveryClient()
                                                           .getNode()
                                                           .orElse(null)));
    }
    public static<T extends Configuration>  void assertNodeAbsence(ServiceDiscoveryBundle<T> bundle) {
        Awaitility.await()
                .pollInterval(Duration.ofSeconds(1))
                .atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertNull(bundle.getServiceDiscoveryClient()
                                                           .getNode()
                                                           .orElse(null)));
    }
    
    public IdGenerationRequestV2 getBase36Formatter(final String prefix,
                                                    final String suffix,
                                                    final String domain,
                                                    final boolean skipGlobal,
                                                    final List<IdValidationConstraint> constraints) {
        return IdGenerationRequestV2.builder()
                .withPrefix(prefix)
                .withSuffix(suffix)
                .includeBase36()
                .includeRandomNonce()
                .domain(domain)
                .skipGlobal(skipGlobal)
                .constraints(constraints)
                .build();
    }
    
    public IdGenerationRequestV2 getDefaultV2Formatter(final String prefix,
                                                       final String suffix,
                                                       final String domain,
                                                       final boolean skipGlobal,
                                                       final List<IdValidationConstraint> constraints) {
        return IdGenerationRequestV2.builder()
                .withPrefix(prefix)
                .withSuffix(suffix)
                .includeRandomNonce()
                .domain(domain)
                .skipGlobal(skipGlobal)
                .constraints(constraints)
                .build();
    }
}
