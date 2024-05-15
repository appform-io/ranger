package io.appform.ranger.discovery.bundle.id;

import io.appform.ranger.discovery.bundle.id.config.IdGeneratorRetryConfig;
import io.appform.ranger.discovery.bundle.id.config.WeightedIdConfig;
import io.appform.ranger.discovery.bundle.id.constraints.PartitionValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.weighted.PartitionIdTracker;
import io.appform.ranger.discovery.bundle.id.config.PartitionRange;
import io.appform.ranger.discovery.bundle.id.weighted.WeightedIdGenerator;
import io.appform.ranger.discovery.bundle.id.config.WeightedPartition;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
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
    final Function<String, Integer> partitionResolverSupplier = (txnId) -> Integer.parseInt(txnId.substring(txnId.length() - 6)) % partitionCount;
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
        weightedIdGenerator = new WeightedIdGenerator(
                partitionCount, partitionResolverSupplier,
                IdGeneratorRetryConfig.builder().idGenerationRetryCount(100).partitionRetryCount(100).build(),
                weightedIdConfig
        );
    }

    @Test
    void testGenerateWithBenchmark() throws IOException {
        val totalTime = TestUtil.runMTTest(5, 100000, (k) -> weightedIdGenerator.generate("P"), this.getClass().getName() + ".testGenerateWithBenchmark");
        testUniqueIds(weightedIdGenerator.getIdStore());
    }

    @Test
    void testGenerateWithConstraints() throws IOException {
        PartitionValidationConstraint partitionConstraint = (k) -> k % 10 == 0;
        weightedIdGenerator.registerGlobalConstraints(partitionConstraint);
        val totalTime = TestUtil.runMTTest(5, 100000, (k) -> weightedIdGenerator.generateWithConstraints("P", (String) null, false), this.getClass().getName() + ".testGenerateWithConstraints");
        testUniqueIds(weightedIdGenerator.getIdStore());

        for (Map.Entry<String, Map<Long, PartitionIdTracker>> entry : weightedIdGenerator.getIdStore().entrySet()) {
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

    void testUniqueIds(Map<String, Map<Long, PartitionIdTracker>> dataStore) {
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
    void testGenerateOriginal() {
        weightedIdGenerator = new WeightedIdGenerator(
                partitionCount, partitionResolverSupplier,
                IdGeneratorRetryConfig.builder().idGenerationRetryCount(100).partitionRetryCount(100).build(),
                weightedIdConfig,
                IdFormatters.original()
        );
        String id = weightedIdGenerator.generate("TEST").getId();
        Assertions.assertEquals(26, id.length());
    }

    @Test
    void testGenerateBase36() {
        weightedIdGenerator = new WeightedIdGenerator(
                partitionCount,
                (txnId) -> new BigInteger(txnId.substring(txnId.length() - 6), 36).abs().intValue() % partitionCount,
                IdGeneratorRetryConfig.builder().idGenerationRetryCount(100).partitionRetryCount(100).build(),
                weightedIdConfig,
                IdFormatters.base36()
        );
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
        Assertions.assertEquals(TestUtil.generateDate(2020, 11, 25, 9, 59, 3, 0, ZoneId.systemDefault()),
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

}