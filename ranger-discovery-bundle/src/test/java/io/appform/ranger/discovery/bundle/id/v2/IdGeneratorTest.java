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

package io.appform.ranger.discovery.bundle.id.v2;

import com.google.common.collect.ImmutableList;
import io.appform.ranger.discovery.bundle.id.Id;
import io.appform.ranger.discovery.bundle.id.v2.generator.IdGenerator;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.constraints.impl.JavaHashCodeBasedKeyPartitioner;
import io.appform.ranger.discovery.bundle.id.constraints.impl.PartitionValidator;
import io.appform.ranger.discovery.bundle.id.v2.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.util.NodeUtils;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

/**
 * Test for {@link IdGenerator}
 */
@Slf4j
@SuppressWarnings({"unused", "FieldMayBeFinal"})
class IdGeneratorTest {
    private final int nodeId = 23;

    @Getter
    private static final class Runner implements Callable<Long> {
        private boolean stop = false;
        private long count = 0L;

        @Override
        public Long call() {
            while (!stop) {
                val id = IdGenerator.generate("X", "Y", IdFormatters.suffixed());
                count++;
            }
            return count;
        }
    }

    @Getter
    private static final class ConstraintRunner implements Callable<Long> {
        private final IdValidationConstraint constraint;
        private boolean stop = false;
        private long count = 0L;

        private ConstraintRunner(IdValidationConstraint constraint) {
            this.constraint = constraint;
        }

        @Override
        public Long call() {
            while (!stop) {
                Optional<Id> id = IdGenerator.generateWithConstraints("X", "Y", Collections.singletonList(constraint), IdFormatters.suffixed());
                Assertions.assertTrue(id.isPresent());
                count++;
            }
            return count;
        }
    }

    @BeforeEach
    void setup() {
        NodeUtils.setNode(nodeId);
    }

    @AfterEach
    void cleanup() {
        IdGenerator.cleanUp();
    }

    @Test
    void testGenerate() {
        val numRunners = 20;
        val runners = IntStream.range(0, numRunners).mapToObj(i -> new Runner()).toList();
        val executorService = Executors.newFixedThreadPool(numRunners);
        runners.forEach(executorService::submit);
        Awaitility.await()
                .pollInterval(Duration.ofSeconds(10))
                .timeout(Duration.ofSeconds(11))
                .until(() -> true);
        executorService.shutdownNow();
        val totalCount = runners.stream().mapToLong(Runner::getCount).sum();
        log.debug("Generated ID count: {}", totalCount);
        log.debug("Generated ID rate: {}/sec", totalCount / 10);
        Assertions.assertTrue(totalCount > 0);
    }

    @Test
    void testGenerateOriginalId() {
        val id = IdGenerator.generate("TEST", "", IdFormatters.original());
        Assertions.assertEquals(28, id.getId().length());
    }
    
    @Test
    void testGenerateSuffixedId() {
        val id = IdGenerator.generate("TEST", "001XYZ001", IdFormatters.suffixed());
        Assertions.assertEquals(37, id.getId().length());
    }

    @Test
    void testGenerateBase36SuffixedId() {
        val id = IdGenerator.generate("TEST", "001XYZ001", IdFormatters.base36Suffixed());
        Assertions.assertEquals(31, id.getId().length());
    }

    @Test
    void testGenerateWithOriginalIdConstraints() {
        IdGenerator.registerDomainSpecificConstraints("TEST", Collections.singletonList(id -> true));
        Optional<Id> id = IdGenerator.generateWithConstraints("TEST", "", "TEST", IdFormatters.original());

        Assertions.assertTrue(id.isPresent());
        Assertions.assertEquals(28, id.get().getId().length());

        // Unregistered Domain
        id = IdGenerator.generateWithConstraints("TEST", "", "TEST1", IdFormatters.original());
        Assertions.assertTrue(id.isPresent());
        Assertions.assertEquals(28, id.get().getId().length());
    }
    
    @Test
    void testGenerateWithSuffixedIdConstraints() {
        IdGenerator.registerDomainSpecificConstraints("TEST", Collections.singletonList(id -> true));
        Optional<Id> id = IdGenerator.generateWithConstraints("TEST", "001XYZ001", "TEST", IdFormatters.suffixed());
        
        Assertions.assertTrue(id.isPresent());
        Assertions.assertEquals(37, id.get().getId().length());
        
        // Unregistered Domain
        id = IdGenerator.generateWithConstraints("TEST", "001XYZ001", "TEST1", IdFormatters.suffixed());
        Assertions.assertTrue(id.isPresent());
        Assertions.assertEquals(37, id.get().getId().length());
    }
    
    @Test
    void testGenerateWithBase36SuffixedIdConstraints() {
        IdGenerator.registerDomainSpecificConstraints("TEST", Collections.singletonList(id -> true));
        Optional<Id> id = IdGenerator.generateWithConstraints("TEST", "001001", "TEST", IdFormatters.base36Suffixed());
        
        Assertions.assertTrue(id.isPresent());
        Assertions.assertEquals(28, id.get().getId().length());
        
        // Unregistered Domain
        id = IdGenerator.generateWithConstraints("TEST", "001XYZ001", "TEST1", IdFormatters.base36Suffixed());
        Assertions.assertTrue(id.isPresent());
        Assertions.assertEquals(31, id.get().getId().length());
    }

    @Test
    void testGenerateOriginalIdWithConstraintsFailedWithLocalConstraint() {
        IdGenerator.registerGlobalConstraints(Collections.singletonList(id -> false));
        IdGenerator.registerDomainSpecificConstraints("TEST", Collections.singletonList(id -> false));
        Optional<Id> id = IdGenerator.generateWithConstraints("TEST", "", "TEST", IdFormatters.original());
        Assertions.assertFalse(id.isPresent());
    }
    
    @Test
    void testGenerateSuffixedIdWithConstraintsFailedWithLocalConstraint() {
        IdGenerator.registerGlobalConstraints(Collections.singletonList(id -> false));
        IdGenerator.registerDomainSpecificConstraints("TEST", Collections.singletonList(id -> false));
        Optional<Id> id = IdGenerator.generateWithConstraints("TEST", "00AA00A", "TEST", IdFormatters.suffixed());
        Assertions.assertFalse(id.isPresent());
    }
    
    @Test
    void testGenerateBase36SuffixedIdWithConstraintsFailedWithLocalConstraint() {
        IdGenerator.registerGlobalConstraints(Collections.singletonList(id -> false));
        IdGenerator.registerDomainSpecificConstraints("TEST", Collections.singletonList(id -> false));
        Optional<Id> id = IdGenerator.generateWithConstraints("TEST", "00AA0", "TEST", IdFormatters.base36Suffixed());
        Assertions.assertFalse(id.isPresent());
    }

    @Test
    void testGenerateOriginalIdWithConstraintsFailedWithGlobalConstraint() {
        IdGenerator.registerGlobalConstraints(Collections.singletonList(id -> false));
        IdGenerator.registerDomainSpecificConstraints("TEST", Collections.singletonList(id -> false));
        Optional<Id> id = IdGenerator.generateWithConstraints("TEST", "", "TEST", IdFormatters.original(), false);
        Assertions.assertFalse(id.isPresent());
    }
    
    @Test
    void testGenerateSuffixedIdWithConstraintsFailedWithGlobalConstraint() {
        IdGenerator.registerGlobalConstraints(Collections.singletonList(id -> false));
        IdGenerator.registerDomainSpecificConstraints("TEST", Collections.singletonList(id -> false));
        Optional<Id> id = IdGenerator.generateWithConstraints("TEST", "00AA00A", "TEST", IdFormatters.suffixed(), false);
        Assertions.assertFalse(id.isPresent());
    }
    
    @Test
    void testGenerateBase36SuffixedIdWithConstraintsFailedWithGlobalConstraint() {
        IdGenerator.registerGlobalConstraints(Collections.singletonList(id -> false));
        IdGenerator.registerDomainSpecificConstraints("TEST", Collections.singletonList(id -> false));
        Optional<Id> id = IdGenerator.generateWithConstraints("TEST", "00AA00A", "TEST", IdFormatters.base36Suffixed(), false);
        Assertions.assertFalse(id.isPresent());
    }


    @Test
    void testGenerateWithConstraintsNoConstraint() {
        int numRunners = 20;

        val runners = IntStream.range(0, numRunners).mapToObj(i -> new ConstraintRunner(new PartitionValidator(4, new JavaHashCodeBasedKeyPartitioner(16)))).toList();
        val executorService = Executors.newFixedThreadPool(numRunners);
        runners.forEach(executorService::submit);
        Awaitility.await()
                .pollInterval(Duration.ofSeconds(10))
                .timeout(Duration.ofSeconds(11))
                .until(() -> true);
        executorService.shutdownNow();
        val totalCount = runners.stream().mapToLong(ConstraintRunner::getCount).sum();
        log.debug("Generated ID count: {}", totalCount);
        log.debug("Generated ID rate: {}/sec", totalCount / 10);
        Assertions.assertTrue(totalCount > 0);

    }

    @Test
    void testConstraintFailure() {
        Assertions.assertFalse(IdGenerator.generateWithConstraints(
                "TST",
                "TEST1",
                ImmutableList.of(id -> false),
                IdFormatters.original(),
                false).isPresent());
    }

    @Test
    void testParseFailure() {
        //Null or Empty String
        Assertions.assertFalse(IdGenerator.parse(null).isPresent());
        Assertions.assertFalse(IdGenerator.parse("").isPresent());

        //Invalid length
        Assertions.assertFalse(IdGenerator.parse("TEST").isPresent());

        //Invalid chars
        Assertions.assertFalse(IdGenerator.parse("XCL983dfb1ee0a847cd9e7321fcabc2f223").isPresent());
        Assertions.assertFalse(IdGenerator.parse("XCL98-3df-b1e:e0a847cd9e7321fcabc2f223").isPresent());

        //Invalid month
        Assertions.assertFalse(IdGenerator.parse("ABC2032250959030643972247").isPresent());
        //Invalid date
        Assertions.assertFalse(IdGenerator.parse("ABC2011450959030643972247").isPresent());
        //Invalid hour
        Assertions.assertFalse(IdGenerator.parse("ABC2011259659030643972247").isPresent());
        //Invalid minute
        Assertions.assertFalse(IdGenerator.parse("ABC2011250972030643972247").isPresent());
        //Invalid sec
        Assertions.assertFalse(IdGenerator.parse("ABC2011250959720643972247").isPresent());
    }

    @Test
    void testParseSuccess() {
        val idString = "ABC012011250959030643972247";
        val id = IdGenerator.parse(idString).orElse(null);
        Assertions.assertNotNull(id);
        Assertions.assertEquals(idString, id.getId());
        Assertions.assertEquals(247, id.getExponent());
        Assertions.assertEquals(3972, id.getNode());
        Assertions.assertEquals(generateDate(2020, 11, 25, 9, 59, 3, 64, ZoneId.systemDefault()),
                                id.getGeneratedDate());
    }

    @Test
    void testOriginalIdParseSuccessAfterGeneration() {
        val generatedId = IdGenerator.generate("TEST", "", IdFormatters.original());
        val parsedId = IdGenerator.parse(generatedId.getId()).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(parsedId.getId(), generatedId.getId());
        Assertions.assertEquals(parsedId.getExponent(), generatedId.getExponent());
        Assertions.assertEquals(parsedId.getNode(), generatedId.getNode());
        Assertions.assertEquals(parsedId.getGeneratedDate(), generatedId.getGeneratedDate());
    }
    
    @Test
    void testSuffixedIdParseSuccessAfterGeneration() {
        val generatedId = IdGenerator.generate("TEST", "00AA001", IdFormatters.suffixed());
        val parsedId = IdGenerator.parse(generatedId.getId()).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(parsedId.getId(), generatedId.getId());
        Assertions.assertEquals(parsedId.getExponent(), generatedId.getExponent());
        Assertions.assertEquals(parsedId.getNode(), generatedId.getNode());
        Assertions.assertEquals(parsedId.getGeneratedDate(), generatedId.getGeneratedDate());
    }
    
    @Test
    void testBase36SuffixedIdParseSuccessAfterGeneration() {
        val generatedId = IdGenerator.generate("TEST", "00AA001", IdFormatters.base36Suffixed());
        val parsedId = IdGenerator.parse(generatedId.getId()).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(parsedId.getId(), generatedId.getId());
        Assertions.assertEquals(parsedId.getExponent(), generatedId.getExponent());
        Assertions.assertEquals(parsedId.getNode(), generatedId.getNode());
        Assertions.assertEquals(parsedId.getGeneratedDate(), generatedId.getGeneratedDate());
    }
    
    @Test
    void testGenerateWithNumericalPrefix() {
        val prefix = "10";
        val exception = Assertions.assertThrows(IllegalArgumentException.class, () -> IdGenerator.generate(prefix, "", IdFormatters.original()));
        Assertions.assertEquals("Prefix does not match the required regex: ^[a-zA-Z]+$", exception.getMessage());
    }
    
    @Test
    void testGenerateWithBlankPrefix() {
        val prefix = "";
        val exception = Assertions.assertThrows(IllegalArgumentException.class, () -> IdGenerator.generate(prefix, "", IdFormatters.original()));
        Assertions.assertEquals("Prefix does not match the required regex: ^[a-zA-Z]+$", exception.getMessage());
    }


    @SuppressWarnings("SameParameterValue")
    private Date generateDate(int year, int month, int day, int hour, int min, int sec, int ms, ZoneId zoneId) {
        return Date.from(
                Instant.from(
                        ZonedDateTime.of(
                                LocalDateTime.of(
                                        year, month, day, hour, min, sec, Math.multiplyExact(ms, 1000000)
                                                ),
                                zoneId
                                        )
                            )
                        );
    }
}
