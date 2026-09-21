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

import io.appform.ranger.discovery.bundle.id.decorators.IdDecorators;
import io.appform.ranger.discovery.bundle.id.formatter.DefaultIdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.RandomNonceIdFormatter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

class IdGeneratorTypeTest {

    @Test
    void shouldFindTypeForFreshFormatterInstances() {
        Assertions.assertEquals(
                Optional.of(IdGeneratorType.DEFAULT.getValue()),
                IdGeneratorType.findValue(new DefaultIdFormatter(), List.of()));
        Assertions.assertEquals(
                Optional.of(IdGeneratorType.DEFAULT_V2_RANDOM_NONCE.getValue()),
                IdGeneratorType.findValue(new RandomNonceIdFormatter(), List.of()));
        Assertions.assertEquals(
                Optional.of(IdGeneratorType.BASE_36_RANDOM_NONCE.getValue()),
                IdGeneratorType.findValue(
                        new RandomNonceIdFormatter(),
                        List.of(IdDecorators.base36())));
    }
}
