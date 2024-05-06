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
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;

/**
 * Test for {@link DistributedIdGenerator}
 */
@Slf4j
@SuppressWarnings({"unused", "FieldMayBeFinal"})
class DistributedIdGeneratorTest {
    final int partitionCount = 1024;
    final Function<String, Integer> partitionResolverSupplier = (txnId) -> Integer.parseInt(txnId.substring(txnId.length()-6)) % partitionCount;
    private DistributedIdGenerator distributedIdGenerator;

    @BeforeEach
    void setup() {
        distributedIdGenerator = new DistributedIdGenerator(23, partitionCount, partitionResolverSupplier);
    }

    @Test
    void testGenerateWithBenchmark() throws IOException {
        val totalTime = TestUtil.runMTTest(5, 100000, (k) -> distributedIdGenerator.generate("P"), this.getClass().getName() + ".testGenerateWithBenchmark");
        testUniqueIdsInDataStore(distributedIdGenerator.getDataStore());
    }

    @Test
    void testGenerateWithConstraints() throws IOException {
        KeyValidationConstraint partitionConstraint = (k) -> k % 10 == 0;
        distributedIdGenerator.registerGlobalConstraints(partitionConstraint);
        val totalTime = TestUtil.runMTTest(5, 100000, (k) -> distributedIdGenerator.generateWithConstraints("P", (String) null, false), this.getClass().getName() + ".testGenerateWithConstraints");
        testUniqueIdsInDataStore(distributedIdGenerator.getDataStore());

        for (Map.Entry<String, Map<Long, PartitionIdTracker>> entry : distributedIdGenerator.getDataStore().entrySet()) {
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

    void testUniqueIdsInDataStore(Map<String, Map<Long, PartitionIdTracker>> dataStore) {
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
    void testUniqueIds() {
        HashSet<String> allIDs = new HashSet<>();
        boolean allIdsUnique = true;
        for (int i=0; i < 10000; i+=1) {
            val txnId = distributedIdGenerator.generate("P").getId();
            if (allIDs.contains(txnId)) {
                log.warn(txnId);
                log.warn(String.valueOf(allIDs));
                allIdsUnique = false;
            } else {
                allIDs.add(txnId);
            }
        }
        Assertions.assertTrue(allIdsUnique);
    }

    @Test
    void testGenerateOriginal() {
        distributedIdGenerator = new DistributedIdGenerator(23, partitionCount, partitionResolverSupplier, IdFormatters.original());
        String id = distributedIdGenerator.generate("TEST").getId();
        Assertions.assertEquals(26, id.length());
    }

    @Test
    void testGenerateBase36() {
        distributedIdGenerator = new DistributedIdGenerator(23, partitionCount, (txnId) -> new BigInteger(txnId.substring(txnId.length()-6), 36).abs().intValue() % partitionCount, IdFormatters.base36());
        String id = distributedIdGenerator.generate("TEST").getId();
        Assertions.assertEquals(18, id.length());
    }

    @Test
    void testConstraintFailure() {
        Assertions.assertFalse(distributedIdGenerator.generateWithConstraints(
                "TST",
                Collections.singletonList((id -> false)),
                true).isPresent());
    }

    @Test
    void testParseFailure() {
        //Null or Empty String
        Assertions.assertFalse(distributedIdGenerator.parse(null).isPresent());
        Assertions.assertFalse(distributedIdGenerator.parse("").isPresent());

        //Invalid length
        Assertions.assertFalse(distributedIdGenerator.parse("TEST").isPresent());

        //Invalid chars
        Assertions.assertFalse(distributedIdGenerator.parse("XCL983dfb1ee0a847cd9e7321fcabc2f223").isPresent());
        Assertions.assertFalse(distributedIdGenerator.parse("XCL98-3df-b1e:e0a847cd9e7321fcabc2f223").isPresent());

        //Invalid month
        Assertions.assertFalse(distributedIdGenerator.parse("ABC2032250959030643972247").isPresent());
        //Invalid date
        Assertions.assertFalse(distributedIdGenerator.parse("ABC2011450959030643972247").isPresent());
        //Invalid hour
        Assertions.assertFalse(distributedIdGenerator.parse("ABC2011259659030643972247").isPresent());
        //Invalid minute
        Assertions.assertFalse(distributedIdGenerator.parse("ABC2011250972030643972247").isPresent());
        //Invalid sec
        Assertions.assertFalse(distributedIdGenerator.parse("ABC2011250959720643972247").isPresent());
    }

    @Test
    void testParseSuccess() {
        val idString = "ABC2011250959030643972247";
        val id = distributedIdGenerator.parse(idString).orElse(null);
        Assertions.assertNotNull(id);
        Assertions.assertEquals(idString, id.getId());
        Assertions.assertEquals(972247, id.getExponent());
        Assertions.assertEquals(643, id.getNode());
        Assertions.assertEquals(generateDate(2020, 11, 25, 9, 59, 3, 0, ZoneId.systemDefault()),
                id.getGeneratedDate());
    }

    @Test
    void testParseSuccessAfterGeneration() {
        val generatedId = distributedIdGenerator.generate("TEST123");
        val parsedId = distributedIdGenerator.parse(generatedId.getId()).orElse(null);
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