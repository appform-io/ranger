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
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.formatter.IdParsersV2;
import lombok.val;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IdParsersV2StrictTest {
    private static final String PAYLOAD = "2407101232336168748798";
    private static final String NUMERIC = "TEST01" + PAYLOAD;
    private static final String BASE36_PAYLOAD = IdDecorators.base36().decorate(PAYLOAD);
    private static final String BASE36 = "TEST02" + BASE36_PAYLOAD;

    @Test
    void rejectsGarbageBeforeAnySupportedId() {
        for (val id : List.of("TEST" + PAYLOAD, NUMERIC, BASE36)) {
            assertTrue(IdParsersV2.parse("!" + id).isEmpty(), id);
        }
    }

    @Test
    void rejectsSuffixesThatGenerationDisallows() {
        for (val id : List.of(NUMERIC, BASE36)) {
            for (val suffix : List.of("!", "_ABC", " A", "\n", "\nignored")) {
                assertTrue(IdParsersV2.parse(id + suffix).isEmpty(), id + suffix);
            }
        }
    }

    @Test
    void acceptsWholeLegacyAndV2Ids() {
        assertEquals(8748, IdParsersV2.parse("TEST" + PAYLOAD).orElseThrow().getNode());
        for (val id : List.of(NUMERIC, BASE36, "01" + PAYLOAD, "02" + BASE36_PAYLOAD)) {
            for (val suffix : List.of("", "000007", "aB09")) {
                val parsed = IdParsersV2.parse(id + suffix).orElseThrow();
                assertEquals(8748, parsed.getNode());
                assertEquals(798, parsed.getExponent());
                assertEquals(suffix, parsed.getSuffix());
                assertEquals(id + suffix, parsed.getId());
            }
        }
    }

    @Test
    void directFormatterAndDecoratorRejectPartialInputs() {
        assertTrue(IdFormatters.randomNonce().parse("!" + NUMERIC).isEmpty());
        assertTrue(IdFormatters.randomNonce().parse(NUMERIC + "_").isEmpty());
        assertTrue(IdDecorators.base36().parse("!" + BASE36_PAYLOAD).isEmpty());
        assertTrue(IdDecorators.base36().parse(BASE36_PAYLOAD + "_").isEmpty());
    }

    @Test
    void rejectsBase36PayloadWiderThanNumericFormat() {
        val tooWide = IdDecorators.base36().decorate(PAYLOAD + "0");
        assertTrue(IdDecorators.base36().parse(tooWide).isEmpty());
        assertTrue(IdParsersV2.parse("TEST02" + tooWide).isEmpty());
    }

    @Test
    void preservesLeadingZeroesInBase36Payload() {
        val payload = "0407101232336168748798";
        val encoded = IdDecorators.base36().decorate(payload);
        assertEquals(payload + "Ab09", IdDecorators.base36().parse(encoded + "Ab09").orElseThrow());
        assertEquals(8748, IdParsersV2.parse("TEST02" + encoded).orElseThrow().getNode());
    }
}
