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

import io.appform.ranger.discovery.bundle.TestUtils;
import io.appform.ranger.discovery.bundle.id.formatter.IdParsersV2;
import io.appform.ranger.discovery.bundle.util.NodeUtils;
import lombok.val;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;

class IdParsersV2Test {

    @BeforeEach
    void setup() {
        IdGeneratorV2.cleanUp();
        NodeUtils.setNode(23);
        IdGeneratorV2.initialize();
    }

    @AfterEach
    void cleanup() {
        IdGeneratorV2.cleanUp();
    }

    @Test
    void testDefaultId() throws ParseException {
        val id = "T2407101232336168748798";
        val parsedId = IdParsersV2.parse(id).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(id, parsedId.getId());
        Assertions.assertEquals(798, parsedId.getExponent());
        Assertions.assertEquals(8748, parsedId.getNode());
        assertDate("240710123233616", parsedId.getGeneratedDate());
    }

    @Test
    void testParseSuccessAfterGenerationWithSuffix() {
        val prefix = "TEST";
        val suffix = "000007";
        val generatedId = IdGeneratorV2.generate(TestUtils.getDefaultV2Formatter(prefix, suffix, "", false, null));
        val parsedId = IdGeneratorV2.parse(generatedId.getId()).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(prefix, parsedId.getPrefix());
        Assertions.assertEquals(suffix, parsedId.getSuffix());
        Assertions.assertEquals(parsedId.getId(), generatedId.getId());
        Assertions.assertEquals(parsedId.getExponent(), generatedId.getExponent());
        Assertions.assertEquals(parsedId.getNode(), generatedId.getNode());
        Assertions.assertEquals(parsedId.getGeneratedDate(), generatedId.getGeneratedDate());
    }

    @Test
    void testParseSuccessAfterGenerationWithConstraintsSuffix() {
        val prefix = "TEST";
        val suffix = "000007";
        val domain = "TEST";

        IdGeneratorV2.registerDomainSpecificConstraints(domain, Collections.singletonList(id -> true));
        val id = IdGeneratorV2.generateWithConstraints(TestUtils.getDefaultV2Formatter(prefix, suffix, domain, false, null));

        Assertions.assertTrue(id.isPresent());
        Assertions.assertEquals(34, id.get().getId().length());

        val parsedId = IdGeneratorV2.parse(id.get().getId()).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(prefix, parsedId.getPrefix());
        Assertions.assertEquals(suffix, parsedId.getSuffix());
        Assertions.assertEquals(parsedId.getId(), id.get().getId());
        Assertions.assertEquals(parsedId.getExponent(), id.get().getExponent());
        Assertions.assertEquals(parsedId.getNode(), id.get().getNode());
        Assertions.assertEquals(parsedId.getGeneratedDate(), id.get().getGeneratedDate());
    }

    @Test
    void testParseSuccessAfterGenerationWithBase36Suffix() {
        val prefix = "TEST";
        val suffix = "007";
        val generatedId = IdGeneratorV2.generate(TestUtils.getBase36Formatter(prefix, suffix, "", false, null));
        val parsedId = IdGeneratorV2.parse(generatedId.getId()).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(prefix, parsedId.getPrefix());
        Assertions.assertEquals(suffix, parsedId.getSuffix());
        Assertions.assertEquals(parsedId.getId(), generatedId.getId());
        Assertions.assertEquals(parsedId.getExponent(), generatedId.getExponent());
        Assertions.assertEquals(parsedId.getNode(), generatedId.getNode());
        Assertions.assertEquals(parsedId.getGeneratedDate(), generatedId.getGeneratedDate());
    }

    private void assertDate(final String dateString, final Date date) throws ParseException {
        Assertions.assertEquals(new SimpleDateFormat("yyMMddHHmmssSSS").parse(dateString), date);
    }
}
