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
package io.appform.ranger.id;

import io.appform.ranger.id.constraints.IdValidationConstraint;
import io.appform.ranger.id.request.IdGenerationRequestV2;
import io.appform.ranger.id.NodeUtils;
import lombok.val;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IdGenerationRequestV2Test {
    @BeforeEach
    void setup() {
        NodeUtils.setNode(23);
        IdGeneratorV2.initialize();
    }

    @AfterEach
    void cleanup() {
        IdGeneratorV2.cleanUp();
    }

    @Test
    void builderReuseCannotChangeAnExistingRequestsEncoding() {
        val builder = IdGenerationRequestV2.builder().includeRandomNonce();
        val original = builder.build();
        val encoded = builder.includeBase36().build();
        val originalId = IdGeneratorV2.generate(original);
        val encodedId = IdGeneratorV2.generate(encoded);
        assertEquals(24, originalId.getId().length());
        assertEquals(18, encodedId.getId().length());
        assertEquals(23, IdGeneratorV2.parse(originalId.getId()).orElseThrow().getNode());
        assertEquals(23, IdGeneratorV2.parse(encodedId.getId()).orElseThrow().getNode());
    }

    @Test
    void callerCannotRemoveConstraintsFromBuiltRequest() {
        val constraints = new ArrayList<IdValidationConstraint>(List.of(id -> false));
        val request = IdGenerationRequestV2.builder().includeRandomNonce().constraints(constraints).build();
        constraints.clear();
        assertTrue(IdGeneratorV2.generateWithConstraints(request).isEmpty());
        assertThrows(UnsupportedOperationException.class, () -> request.getConstraints().clear());
    }

    @Test
    void callerCannotChangeBuiltDecoratorChain() {
        val request = IdGenerationRequestV2.builder().includeRandomNonce().includeBase36().build();
        assertThrows(UnsupportedOperationException.class, () -> request.getIdDecorators().clear());
    }

    @Test
    void nullConstraintsInheritDomainButEmptyConstraintsOverrideIt() {
        IdGeneratorV2.registerDomainSpecificConstraints("DENY", id -> false);
        val inherited = IdGenerationRequestV2.builder().includeRandomNonce().domain("DENY").build();
        val overridden = IdGenerationRequestV2.builder().includeRandomNonce().domain("DENY")
                .constraints(List.of()).build();
        assertTrue(IdGeneratorV2.generateWithConstraints(inherited).isEmpty());
        assertTrue(IdGeneratorV2.generateWithConstraints(overridden).isPresent());
    }
}
