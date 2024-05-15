package io.appform.ranger.discovery.bundle.id;

import io.appform.ranger.discovery.bundle.id.config.IdGeneratorRetryConfig;
import io.appform.ranger.discovery.bundle.id.constraints.PartitionValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.weighted.PartitionAwareIdGenerator;
import io.appform.ranger.discovery.bundle.id.weighted.PartitionIdTracker;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Function;

/**
 * Test for {@link PartitionAwareIdGenerator}
 */
@Slf4j
@SuppressWarnings({"unused", "FieldMayBeFinal"})
class PartitionAwareIdGeneratorTest {
    final int partitionCount = 1024;
    final Function<String, Integer> partitionResolverSupplier = (txnId) -> Integer.parseInt(txnId.substring(txnId.length() - 6)) % partitionCount;
    private PartitionAwareIdGenerator partitionAwareIdGenerator;

    @BeforeEach
    void setup() {
        partitionAwareIdGenerator = new PartitionAwareIdGenerator(
                partitionCount, partitionResolverSupplier,
                IdGeneratorRetryConfig.builder().idGenerationRetryCount(100).partitionRetryCount(100).build()
        );
    }

    @Test
    void testGenerateWithBenchmark() throws IOException {
        val totalTime = TestUtil.runMTTest(5, 100000, (k) -> partitionAwareIdGenerator.generate("P"), this.getClass().getName() + ".testGenerateWithBenchmark");
        testUniqueIdsInDataStore(partitionAwareIdGenerator.getIdStore());
    }

    @Test
    void testGenerateWithConstraints() throws IOException {
        PartitionValidationConstraint partitionConstraint = (k) -> k % 10 == 0;
        partitionAwareIdGenerator.registerGlobalConstraints(partitionConstraint);
        val totalTime = TestUtil.runMTTest(5, 100000, (k) -> partitionAwareIdGenerator.generateWithConstraints("P", (String) null, false), this.getClass().getName() + ".testGenerateWithConstraints");
        testUniqueIdsInDataStore(partitionAwareIdGenerator.getIdStore());

        for (Map.Entry<String, Map<Long, PartitionIdTracker>> entry : partitionAwareIdGenerator.getIdStore().entrySet()) {
            val prefix = entry.getKey();
            val prefixIds = entry.getValue();
            HashSet<Integer> uniqueIds = new HashSet<>();
            for (Map.Entry<Long, PartitionIdTracker> prefixEntry : prefixIds.entrySet()) {
                val key = prefixEntry.getKey();
                val partitionIdTracker = prefixEntry.getValue();
                for (int idx = 0; idx < partitionIdTracker.getPartitionSize(); idx += 1) {
                    if (!partitionConstraint.isValid(idx)) {
                        Assertions.assertEquals(0, partitionIdTracker.getIdPoolList()[idx].getPointer().get());
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
                for (val idPool : partitionIdTracker.getIdPoolList()) {
                    boolean allIdsUniqueInList = true;
                    HashSet<Integer> uniqueIdsInList = new HashSet<>();
                    for (val id : idPool.getIdList()) {
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
        for (int i = 0; i < 10000; i += 1) {
            val txnId = partitionAwareIdGenerator.generate("P").getId();
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
        partitionAwareIdGenerator = new PartitionAwareIdGenerator(
                partitionCount, partitionResolverSupplier,
                IdGeneratorRetryConfig.builder().idGenerationRetryCount(100).partitionRetryCount(100).build(),
                IdFormatters.original()
        );
        String id = partitionAwareIdGenerator.generate("TEST").getId();
        Assertions.assertEquals(26, id.length());
    }

    @Test
    void testGenerateBase36() {
        partitionAwareIdGenerator = new PartitionAwareIdGenerator(
                partitionCount,
                (txnId) -> new BigInteger(txnId.substring(txnId.length() - 6), 36).abs().intValue() % partitionCount,
                IdGeneratorRetryConfig.builder().idGenerationRetryCount(100).partitionRetryCount(100).build(),
                IdFormatters.base36()
        );
        String id = partitionAwareIdGenerator.generate("TEST").getId();
        Assertions.assertEquals(18, id.length());
    }

    @Test
    void testConstraintFailure() {
        Assertions.assertFalse(partitionAwareIdGenerator.generateWithConstraints(
                "TST",
                Collections.singletonList((id -> false)),
                true).isPresent());
    }

    @Test
    void testParseFailure() {
        //Null or Empty String
        Assertions.assertFalse(partitionAwareIdGenerator.parse(null).isPresent());
        Assertions.assertFalse(partitionAwareIdGenerator.parse("").isPresent());

        //Invalid length
        Assertions.assertFalse(partitionAwareIdGenerator.parse("TEST").isPresent());

        //Invalid chars
        Assertions.assertFalse(partitionAwareIdGenerator.parse("XCL983dfb1ee0a847cd9e7321fcabc2f223").isPresent());
        Assertions.assertFalse(partitionAwareIdGenerator.parse("XCL98-3df-b1e:e0a847cd9e7321fcabc2f223").isPresent());

        //Invalid month
        Assertions.assertFalse(partitionAwareIdGenerator.parse("ABC2032250959030643972247").isPresent());
        //Invalid date
        Assertions.assertFalse(partitionAwareIdGenerator.parse("ABC2011450959030643972247").isPresent());
        //Invalid hour
        Assertions.assertFalse(partitionAwareIdGenerator.parse("ABC2011259659030643972247").isPresent());
        //Invalid minute
        Assertions.assertFalse(partitionAwareIdGenerator.parse("ABC2011250972030643972247").isPresent());
        //Invalid sec
        Assertions.assertFalse(partitionAwareIdGenerator.parse("ABC2011250959720643972247").isPresent());
    }

    @Test
    void testParseSuccess() {
        val idString = "ABC2011250959030643972247";
        val id = partitionAwareIdGenerator.parse(idString).orElse(null);
        Assertions.assertNotNull(id);
        Assertions.assertEquals(idString, id.getId());
        Assertions.assertEquals(972247, id.getExponent());
        Assertions.assertEquals(643, id.getNode());
        Assertions.assertEquals(TestUtil.generateDate(2020, 11, 25, 9, 59, 3, 0, ZoneId.systemDefault()),
                id.getGeneratedDate());
    }

    @Test
    void testParseSuccessAfterGeneration() {
        val generatedId = partitionAwareIdGenerator.generate("TEST123");
        val parsedId = partitionAwareIdGenerator.parse(generatedId.getId()).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(parsedId.getId(), generatedId.getId());
        Assertions.assertEquals(parsedId.getExponent(), generatedId.getExponent());
        Assertions.assertEquals(parsedId.getNode(), generatedId.getNode());
    }

}