package io.appform.ranger.discovery.bundle.id;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorRetryConfig;
import io.appform.ranger.discovery.bundle.id.config.WeightedIdConfig;
import io.appform.ranger.discovery.bundle.id.constraints.PartitionValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Test for {@link WeightedIdGenerator}
 */
@Slf4j
@SuppressWarnings({"unused", "FieldMayBeFinal"})
class WeightedIdGeneratorTest {
    final int numThreads = 5;
    final int iterationCount = 100000;
    final int partitionCount = 64;
    final Function<String, Integer> partitionResolverSupplier = (txnId) -> Integer.parseInt(txnId.substring(txnId.length() - 6)) % partitionCount;
    private WeightedIdGenerator weightedIdGenerator;
    private IdGeneratorConfig idGeneratorConfig;

    @BeforeEach
    void setup() {
        val metricRegistry = mock(MetricRegistry.class);
        val meter = mock(Meter.class);
        doReturn(meter).when(metricRegistry).meter(anyString());
        doNothing().when(meter).mark();
        List<WeightedPartition> partitionConfigList = new ArrayList<>();
        partitionConfigList.add(WeightedPartition.builder()
                .partitionRange(PartitionRange.builder().start(0).end(31).build())
                .weight(400).build());
        partitionConfigList.add(WeightedPartition.builder()
                .partitionRange(PartitionRange.builder().start(32).end(63).build())
                .weight(600).build());
        val weightedIdConfig = WeightedIdConfig.builder()
                .partitions(partitionConfigList)
                .build();
        idGeneratorConfig =
                IdGeneratorConfig.builder()
                        .partitionCount(partitionCount)
                        .weightedIdConfig(weightedIdConfig)
                        .idPoolSize(100)
                        .retryConfig(IdGeneratorRetryConfig.builder().idGenerationRetryCount(4096).partitionRetryCount(4096).build())
                        .build();
        weightedIdGenerator = new WeightedIdGenerator(idGeneratorConfig, partitionResolverSupplier, metricRegistry);
    }

    @Test
    void testGenerateWithBenchmark() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<String>());
        val totalTime = TestUtil.runMTTest(
                numThreads,
                iterationCount,
                (k) -> {
                    val id = weightedIdGenerator.generate("P");
                    id.ifPresent(value -> allIdsList.add(value.getId()));
                },
                this.getClass().getName() + ".testGenerateWithBenchmark");
        Assertions.assertEquals(numThreads * iterationCount, allIdsList.size());
        checkUniqueIds(allIdsList);
        checkDistribution(allIdsList);
    }

    @Test
    void testGenerateAccuracy() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<String>());
        val numThreads = 1;
        val totalIdCount = numThreads * iterationCount;
        val totalTime = TestUtil.runMTTest(
                numThreads,
                iterationCount,
                (k) -> {
                    val id = weightedIdGenerator.generate("P");
                    id.ifPresent(value -> allIdsList.add(value.getId()));
                },
                this.getClass().getName() + ".testGenerateWithBenchmark");
        Assertions.assertEquals(numThreads * iterationCount, allIdsList.size());
        checkUniqueIds(allIdsList);
        checkDistribution(allIdsList);
    }

    @Test
    void testGenerateWithConstraints() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<String>());
        PartitionValidationConstraint partitionConstraint = (k) -> k % 10 == 0;
        weightedIdGenerator.registerGlobalConstraints(partitionConstraint);
        val totalTime = TestUtil.runMTTest(
                numThreads,
                iterationCount,
                (k) -> {
                    val id = weightedIdGenerator.generateWithConstraints("P", (String) null, false);
                    id.ifPresent(value -> allIdsList.add(value.getId()));
                },
                this.getClass().getName() + ".testGenerateWithConstraints");
        checkUniqueIds(allIdsList);

//        Assert No ID was generated for Invalid partitions
        for (val id: allIdsList) {
            val partitionId = partitionResolverSupplier.apply(id);
            Assertions.assertTrue(partitionConstraint.isValid(partitionId));
        }
    }

    void checkUniqueIds(List<String> allIdsList) {
        HashSet<String> uniqueIds = new HashSet<>(allIdsList);
        Assertions.assertEquals(allIdsList.size(), uniqueIds.size());
    }

    void checkDistribution(List<String> allIdsList) {
        val idCountMap = new HashMap<Integer, Integer>();
        for (val id: allIdsList) {
            val partitionId = partitionResolverSupplier.apply(id);
            idCountMap.put(partitionId, idCountMap.getOrDefault(partitionId, 0) + 1);
        }

        for (WeightedPartition partition: idGeneratorConfig.getWeightedIdConfig().getPartitions()) {
            val expectedIdCount = ((double) partition.getWeight() / weightedIdGenerator.getMaxShardWeight()) *  ((double) allIdsList.size() / (partition.getPartitionRange().getEnd()-partition.getPartitionRange().getStart()+1));
            int idCountForPartition = 0;
            for (int partitionId = partition.getPartitionRange().getStart(); partitionId <= partition.getPartitionRange().getEnd(); partitionId++) {
                Assertions.assertTrue(expectedIdCount * 0.8 <= idCountMap.get(partitionId));
                Assertions.assertTrue(idCountMap.get(partitionId) <= expectedIdCount * 1.2);
                idCountForPartition += idCountMap.get(partitionId);
            }
            log.debug("Partition ID Count: {} - Percentage: {}", idCountForPartition, (double) idCountForPartition * 100 / allIdsList.size());
        }
    }

    @Test
    void testGenerateOriginal() {
        weightedIdGenerator = new WeightedIdGenerator(idGeneratorConfig, partitionResolverSupplier, IdFormatters.original(), mock(MetricRegistry.class));
        val idOptional = weightedIdGenerator.generate("TEST");
        String id = idOptional.isPresent() ? idOptional.get().getId() : "";
        Assertions.assertEquals(26, id.length());
    }

    @Test
    void testGenerateBase36() {
        weightedIdGenerator = new WeightedIdGenerator(
                idGeneratorConfig,
                (txnId) -> new BigInteger(txnId.substring(txnId.length() - 6), 36).abs().intValue() % partitionCount,
                IdFormatters.base36(),
                mock(MetricRegistry.class)
        );
        val idOptional = weightedIdGenerator.generate("TEST");
        String id = idOptional.isPresent() ? idOptional.get().getId() : "";
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
        val generatedIdOptional = weightedIdGenerator.generate("TEST123");
        val generatedId = generatedIdOptional.orElse(null);
        Assertions.assertNotNull(generatedId);
        val parsedId = weightedIdGenerator.parse(generatedId.getId()).orElse(null);
        Assertions.assertNotNull(parsedId);
        Assertions.assertEquals(parsedId.getId(), generatedId.getId());
        Assertions.assertEquals(parsedId.getExponent(), generatedId.getExponent());
        Assertions.assertEquals(parsedId.getNode(), generatedId.getNode());
    }

}