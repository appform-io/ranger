package io.appform.ranger.discovery.bundle.id;

import io.appform.ranger.discovery.bundle.id.constraints.KeyValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Test for {@link WeightedIdGenerator}
 */
@Slf4j
@SuppressWarnings({"unused", "FieldMayBeFinal"})
class WeightedIdGeneratorTest {
    final int partitionCount = 1024;
    final Function<String, Integer> partitionResolverSupplier = (txnId) -> Integer.parseInt(txnId.substring(txnId.length()-6)) % partitionCount;
    private WeightedIdGenerator weightedIdGenerator;
    private WeightedIdConfig weightedIdConfig;

    @BeforeEach
    void setup() {
        List<WeightedPartition> partitionConfigList = new ArrayList<>();
        partitionConfigList.add(WeightedPartition.builder()
                .partitionRange(PartitionRange.builder().start(0).end(511).build())
                .weight(400).build());
        partitionConfigList.add(WeightedPartition.builder()
                .partitionRange(PartitionRange.builder().start(512).end(1023).build())
                .weight(600).build());
        weightedIdConfig = WeightedIdConfig.builder()
                .partitions(partitionConfigList)
                .build();
        weightedIdGenerator = new WeightedIdGenerator(7, partitionCount, partitionResolverSupplier, weightedIdConfig);
    }

    @Test
    void testGenerateWithBenchmark() throws IOException {
        val totalTime = TestUtil.runMTTest(5, 10000000, (k) -> weightedIdGenerator.generate("P"), this.getClass().getName() + ".testGenerateWithBenchmark");
//        testUniqueIds(weightedIdGenerator.getDataStore());
    }

    @Test
    void testGenerateWithConstraints() throws IOException {
        KeyValidationConstraint partitionConstraint = (k) -> k % 10 == 0;
        weightedIdGenerator.registerGlobalConstraints(partitionConstraint);
        val totalTime = TestUtil.runMTTest(5, 1000000, (k) -> weightedIdGenerator.generateWithConstraints("P", (String) null, false), this.getClass().getName() + ".testGenerateWithConstraints");
        testUniqueIds(weightedIdGenerator.getDataStore());

        for (Map.Entry<String, Map<Long, PartitionIdTracker>> entry : weightedIdGenerator.getDataStore().entrySet()) {
            val prefix = entry.getKey();
            val prefixIds = entry.getValue();
            HashSet<Integer> uniqueIds = new HashSet<>();
            for (Map.Entry<Long, PartitionIdTracker> prefixEntry : prefixIds.entrySet()) {
                val key = prefixEntry.getKey();
                val partitionIdTracker = prefixEntry.getValue();
                for (int idx=0; idx < partitionIdTracker.getPartitionSize(); idx += 1) {
                    if (!partitionConstraint.isValid(idx)) {
                        Assertions.assertEquals(0, partitionIdTracker.getIdList()[idx].getPointer().get());
                    }
                }
            }
        }
    }

    void testUniqueIds(Map<String, Map<Long, PartitionIdTracker>> dataStore) {
        boolean allIdsUnique = true;
        for (Map.Entry<String, Map<Long, PartitionIdTracker>> entry : dataStore.entrySet()) {
            val prefix = entry.getKey();
            val prefixIds = entry.getValue();
            for (Map.Entry<Long, PartitionIdTracker> prefixEntry : prefixIds.entrySet()) {
                val key = prefixEntry.getKey();
                val partitionIdTracker = prefixEntry.getValue();
                HashSet<Integer> uniqueIds = new HashSet<>();
                for (val idPool: partitionIdTracker.getIdList()) {
                    boolean allIdsUniqueInList = true;
                    HashSet<Integer> uniqueIdsInList = new HashSet<>();
                    for (val id: idPool.getIds()) {
                        if (uniqueIdsInList.contains(id)) {
                            allIdsUniqueInList = false;
                            allIdsUnique = false;
                        } else {
                            uniqueIdsInList.add(id);
                        }

                        if (uniqueIds.contains(id)) {
                            allIdsUnique = false;
                        } else {
                            uniqueIds.add(id);
                        }
                    }
                    Assertions.assertTrue(allIdsUniqueInList);
                }
            }
        }
        Assertions.assertTrue(allIdsUnique);
    }

    @Test
    void testGenerateOriginal() {
        weightedIdGenerator = new WeightedIdGenerator(23, partitionCount, partitionResolverSupplier, IdFormatters.original(), weightedIdConfig);
        String id = weightedIdGenerator.generate("TEST").getId();
        Assertions.assertEquals(26, id.length());
    }

    @Test
    void testGenerateBase36() {
        weightedIdGenerator = new WeightedIdGenerator(23, partitionCount, (txnId) -> new BigInteger(txnId.substring(txnId.length()-6), 36).abs().intValue() % partitionCount, IdFormatters.base36(), weightedIdConfig);
        String id = weightedIdGenerator.generate("TEST").getId();
        Assertions.assertEquals(18, id.length());
    }

    @Test
    void testConstraintFailure() {
        Assertions.assertFalse(weightedIdGenerator.generateWithConstraints(
                "TST",
                Collections.singletonList((id -> false)),
                true).isPresent());
    }

    @Test
    void testParseFailure() {
        //Null or Empty String
        Assertions.assertFalse(weightedIdGenerator.parse(null).isPresent());
        Assertions.assertFalse(weightedIdGenerator.parse("").isPresent());

        //Invalid length
        Assertions.assertFalse(weightedIdGenerator.parse("TEST").isPresent());

        //Invalid chars
        Assertions.assertFalse(weightedIdGenerator.parse("XCL983dfb1ee0a847cd9e7321fcabc2f223").isPresent());
        Assertions.assertFalse(weightedIdGenerator.parse("XCL98-3df-b1e:e0a847cd9e7321fcabc2f223").isPresent());

        //Invalid month
        Assertions.assertFalse(weightedIdGenerator.parse("ABC2032250959030643972247").isPresent());
        //Invalid date
        Assertions.assertFalse(weightedIdGenerator.parse("ABC2011450959030643972247").isPresent());
        //Invalid hour
        Assertions.assertFalse(weightedIdGenerator.parse("ABC2011259659030643972247").isPresent());
        //Invalid minute
        Assertions.assertFalse(weightedIdGenerator.parse("ABC2011250972030643972247").isPresent());
        //Invalid sec
        Assertions.assertFalse(weightedIdGenerator.parse("ABC2011250959720643972247").isPresent());
    }

    @Test
    void testParseSuccess() {
        val idString = "ABC2011250959030643972247";
        val id = weightedIdGenerator.parse(idString).orElse(null);
        Assertions.assertNotNull(id);
        Assertions.assertEquals(idString, id.getId());
        Assertions.assertEquals(972247, id.getExponent());
        Assertions.assertEquals(643, id.getNode());
        Assertions.assertEquals(generateDate(2020, 11, 25, 9, 59, 3, 0, ZoneId.systemDefault()),
                id.getGeneratedDate());
    }

    @Test
    void testParseSuccessAfterGeneration() {
        val generatedId = weightedIdGenerator.generate("TEST123");
        val parsedId = weightedIdGenerator.parse(generatedId.getId()).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(parsedId.getId(), generatedId.getId());
        Assertions.assertEquals(parsedId.getExponent(), generatedId.getExponent());
        Assertions.assertEquals(parsedId.getNode(), generatedId.getNode());
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