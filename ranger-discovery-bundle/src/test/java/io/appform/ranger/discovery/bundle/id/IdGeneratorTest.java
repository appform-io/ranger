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

import com.google.common.collect.ImmutableList;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.constraints.impl.JavaHashCodeBasedKeyPartitioner;
import io.appform.ranger.discovery.bundle.id.constraints.impl.PartitionValidator;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.*;
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
                val id = IdGenerator.generate("X");
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
                Optional<Id> id = IdGenerator.generateWithConstraints("X", Collections.singletonList(constraint));
                Assertions.assertTrue(id.isPresent());
                count++;
            }
            return count;
        }
    }

    @BeforeEach
    void setup() {
        IdGenerator.initialize(nodeId);
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
    void testGenerateOriginal() {
        String id = IdGenerator.generate("TEST", IdFormatters.original()).getId();
        Assertions.assertEquals(26, id.length());
    }

    @Test
    void testGenerateBase36() {
        String id = IdGenerator.generate("TEST", IdFormatters.base36()).getId();
        Assertions.assertEquals(18, id.length());
    }

    @Test
    void testGenerateWithConstraints() {
        IdGenerator.registerDomainSpecificConstraints("TEST", Collections.singletonList(id -> true));
        Optional<Id> id = IdGenerator.generateWithConstraints("TEST", "TEST");

        Assertions.assertTrue(id.isPresent());
        Assertions.assertEquals(26, id.get().getId().length());

        // Unregistered Domain
        id = IdGenerator.generateWithConstraints("TEST", "TEST1");
        Assertions.assertTrue(id.isPresent());
        Assertions.assertEquals(26, id.get().getId().length());
    }

    @Test
    void testGenerateWithConstraintsFailedWithLocalConstraint() {
        IdGenerator.registerGlobalConstraints(Collections.singletonList(id -> false));
        IdGenerator.registerDomainSpecificConstraints("TEST", Collections.singletonList(id -> false));
        Optional<Id> id = IdGenerator.generateWithConstraints("TEST", "TEST");
        Assertions.assertFalse(id.isPresent());
    }

    @Test
    void testGenerateWithConstraintsFailedWithGlobalConstraint() {
        IdGenerator.registerGlobalConstraints(Collections.singletonList(id -> false));
        IdGenerator.registerDomainSpecificConstraints("TEST", Collections.singletonList(id -> false));
        Optional<Id> id = IdGenerator.generateWithConstraints("TEST", "TEST", false);
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
                ImmutableList.of(id -> false),
                false).isPresent());
    }

    @Test
    void testNodeId() {
        val generatedId = IdGenerator.generate("TEST");
        val parsedId = IdGenerator.parse(generatedId.getId()).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(parsedId.getNode(), nodeId);
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
        val idString = "ABC2011250959030643972247";
        val id = IdGenerator.parse(idString).orElse(null);
        Assertions.assertNotNull(id);
        Assertions.assertEquals(idString, id.getId());
        Assertions.assertEquals(247, id.getExponent());
        Assertions.assertEquals(3972, id.getNode());
        Assertions.assertEquals(generateDate(2020, 11, 25, 9, 59, 3, 64, ZoneId.systemDefault()),
                                id.getGeneratedDate());
    }

    @Test
    void testParseFailAfterGeneration() {
        val generatedId = IdGenerator.generate("TEST123");
        val parsedId = IdGenerator.parse(generatedId.getId()).orElse(null);
        Assertions.assertNull(parsedId);
    }

    @Test
    void testParseSuccessAfterGeneration() {
        val prefix = "TEST";
        val generatedId = IdGenerator.generate(prefix);
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
        val exception = Assertions.assertThrows(IllegalArgumentException.class, () -> IdGenerator.generate(prefix));
        Assertions.assertEquals("Prefix does not match the required regex: ^[a-zA-Z]+$", exception.getMessage());
    }

    @Test
    void testGenerateWithEmptyPrefix() {
        val prefix = "";
        val exception = Assertions.assertThrows(IllegalArgumentException.class, () -> IdGenerator.generate(prefix));
        Assertions.assertEquals("Namespace cannot be null or empty", exception.getMessage());
    }

    @Test
    void testGenerateWithConstraintsWithNumericalPrefix() {
        val prefix = "10";
        val domain = "TEST";
        IdGenerator.registerDomainSpecificConstraints(domain, Collections.singletonList(id -> true));
        val exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> IdGenerator.generateWithConstraints(prefix, domain));
        Assertions.assertEquals("Prefix does not match the required regex: ^[a-zA-Z]+$", exception.getMessage());
    }

    @Test
    void testGenerateWithConstraintsWithEmptyPrefix() {
        val prefix = "";
        val domain = "TEST";
        IdGenerator.registerDomainSpecificConstraints(domain, Collections.singletonList(id -> true));
        val exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> IdGenerator.generateWithConstraints(prefix, domain));
        Assertions.assertEquals("Namespace cannot be null or empty", exception.getMessage());
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