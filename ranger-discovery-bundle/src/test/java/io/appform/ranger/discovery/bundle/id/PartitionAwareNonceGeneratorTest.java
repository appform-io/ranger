package io.appform.ranger.discovery.bundle.id;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.discovery.bundle.id.config.DefaultNamespaceConfig;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.constraints.IdValidationConstraint;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import io.appform.ranger.discovery.bundle.id.nonce.NonceGeneratorType;
import io.appform.ranger.discovery.bundle.id.generator.DistributedIdGenerator;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Test for {@link DistributedIdGenerator}
 */
@SuppressWarnings({"unused", "FieldMayBeFinal"})
@Slf4j
class PartitionAwareNonceGeneratorTest {
    final int numThreads = 5;
    final int iterationCount = 100000;
    final int partitionCount = 1024;
    final Function<String, Integer> partitionResolverSupplier = (txnId) -> Integer.parseInt(txnId.substring(txnId.length() - 6)) % partitionCount;
    protected IdGeneratorConfig idGeneratorConfig =
            IdGeneratorConfig.builder()
                    .partitionCount(partitionCount)
                    .defaultNamespaceConfig(DefaultNamespaceConfig.builder().idPoolSizePerPartition(100).build())
                    .build();
    protected DistributedIdGenerator distributedIdGenerator;
    private MetricRegistry metricRegistry = mock(MetricRegistry.class);
    protected NonceGeneratorType nonceGeneratorType;

    @BeforeEach
    void setup() {
        nonceGeneratorType = NonceGeneratorType.DISTRIBUTED;
        val metricRegistry = mock(MetricRegistry.class);
        val meter = mock(Meter.class);
        doReturn(meter).when(metricRegistry).meter(anyString());
        doNothing().when(meter).mark();
        distributedIdGenerator = new DistributedIdGenerator(
                idGeneratorConfig, partitionResolverSupplier, nonceGeneratorType, metricRegistry, Clock.systemDefaultZone()
        );
    }

    @Test
    void testGenerateWithBenchmark() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<Id>());
        val totalTime = TestUtil.runMTTest(
                numThreads,
                iterationCount,
                (k) -> {
                    val id = distributedIdGenerator.generate("P");
                    allIdsList.add(id);
                },
                true,
                this.getClass().getName() + ".testGenerateWithBenchmark");
        Assertions.assertEquals(numThreads * iterationCount, allIdsList.size());
        checkUniqueIds(allIdsList);
        checkDistribution(allIdsList, partitionResolverSupplier, idGeneratorConfig);
    }

    @Test
    void testGenerateWithConstraints() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<Id>());
        IdValidationConstraint partitionConstraint = (k) -> k.getExponent() % 4 == 0;
        val iterationCount = 50000;
        distributedIdGenerator.registerGlobalConstraints((k) -> k.getExponent() % 4 == 0);
        val totalTime = TestUtil.runMTTest(
                numThreads,
                iterationCount,
                (k) -> {
                    val id = distributedIdGenerator.generateWithConstraints("P", List.of(), false);
                    id.ifPresent(allIdsList::add);
                },
                false,
                this.getClass().getName() + ".testGenerateWithConstraints");
        checkUniqueIds(allIdsList);

//        Assert No ID was generated for Invalid partitions
        for (val id: allIdsList) {
            Assertions.assertTrue(partitionConstraint.isValid(id));
        }
    }

    @Test
    void testGenerateAccuracy() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<Id>());
        val iterationCount = 500000;
        val totalIdCount = numThreads * iterationCount;
        val totalTime = TestUtil.runMTTest(
                numThreads,
                iterationCount,
                (k) -> {
                    val id = distributedIdGenerator.generate("P");
                    allIdsList.add(id);
                },
                false,
                this.getClass().getName() + ".testGenerateAccuracy");
        checkUniqueIds(allIdsList);
        checkDistribution(allIdsList, partitionResolverSupplier, idGeneratorConfig);
    }

    void checkUniqueIds(List<Id> allIdsList) {
        List<String> allIdStringList = new ArrayList<>(List.of());
        for (Id id: allIdsList) {
            allIdStringList.add(id.getId());
        }
        HashSet<String> uniqueIds = new HashSet<>(allIdStringList);
        Map<String, Integer> frequencyMap = new HashMap<>();

        // Count occurrences of each integer
        for (String num : allIdStringList) {
            frequencyMap.put(num, frequencyMap.getOrDefault(num, 0) + 1);
        }
        // Print integers with count >= 2
        for (Map.Entry<String, Integer> entry : frequencyMap.entrySet()) {
            if (entry.getValue() >= 2) {
                System.out.println("Integer: " + entry.getKey() + ", Count: " + entry.getValue());
            }
        }
        Assertions.assertEquals(allIdsList.size(), uniqueIds.size());
    }

    protected HashMap<Integer, Integer> getIdCountMap(List<Id> allIdsList, Function<String, Integer> partitionResolver) {
        val idCountMap = new HashMap<Integer, Integer>();
        for (val id: allIdsList) {
            val partitionId = partitionResolver.apply(id.getId());
            idCountMap.put(partitionId, idCountMap.getOrDefault(partitionId, 0) + 1);
        }
        return idCountMap;
    }

    protected void checkDistribution(List<Id> allIdsList, Function<String, Integer> partitionResolver, IdGeneratorConfig config) {
        val idCountMap = getIdCountMap(allIdsList, partitionResolver);
        val expectedIdCount = (double) allIdsList.size() / config.getPartitionCount();
        for (int partitionId=0; partitionId < config.getPartitionCount(); partitionId++) {
            Assertions.assertTrue(expectedIdCount * 0.8 <= idCountMap.get(partitionId),
                    String.format("Partition %s generated %s ids, expected was more than: %s",
                            partitionId, idCountMap.get(partitionId), expectedIdCount * 0.8));
            Assertions.assertTrue(idCountMap.get(partitionId) <= expectedIdCount * 1.2,
                    String.format("Partition %s generated %s ids, expected was less than: %s",
                            partitionId, idCountMap.get(partitionId), expectedIdCount * 1.2));
        }
    }

    @Test
    void testUniqueIds() {
        HashSet<String> allIDs = new HashSet<>();
        boolean allIdsUnique = true;
        for (int i = 0; i < iterationCount; i += 1) {
            val txnId = distributedIdGenerator.generate("P").getId();
            if (allIDs.contains(txnId)) {
                allIdsUnique = false;
            } else {
                allIDs.add(txnId);
            }
        }
        Assertions.assertTrue(allIdsUnique);
    }

    @Test
    void testFirstAndLastPartitionInclusion() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<Id>());
        val totalIdCount = numThreads * iterationCount;
        val totalTime = TestUtil.runMTTest(
                numThreads,
                iterationCount,
                (k) -> {
                    val id = distributedIdGenerator.generate("P");
                    allIdsList.add(id);
                },
                false,
                this.getClass().getName() + ".testGenerateAccuracy");

        val idCountMap = getIdCountMap(allIdsList, partitionResolverSupplier);
        Assertions.assertTrue(idCountMap.get(0) > 0);
        Assertions.assertTrue(idCountMap.get(partitionCount-1) > 0);
    }

    @Test
    void testDataReset() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<Id>());
        val clock = mock(Clock.class);
        doReturn(Instant.parse("2000-01-01T00:00:00Z")).when(clock).instant();
        val distributedIdGenerator = new DistributedIdGenerator(idGeneratorConfig, partitionResolverSupplier, nonceGeneratorType, metricRegistry, clock);
        TestUtil.runMTTest(
                numThreads,
                iterationCount,
                (k) -> {
                    val id = distributedIdGenerator.generate("P");
                    allIdsList.add(id);
                },
                false, null);
        doReturn(Instant.parse("2000-01-01T00:01:00Z")).when(clock).instant();
        TestUtil.runMTTest(
                numThreads,
                iterationCount,
                (k) -> {
                    val id = distributedIdGenerator.generate("P");
                    allIdsList.add(id);
                },
                false, null);
        Assertions.assertEquals(2 * numThreads * iterationCount, allIdsList.size());
        checkUniqueIds(allIdsList);
    }

    @Test
    void testComputation() throws IOException {
        val partitionResolverCount = new AtomicInteger(0);
        Function<String, Integer> partitionResolverSupplierWithCount = (t) -> {
            partitionResolverCount.incrementAndGet();
            return partitionResolverSupplier.apply(t);
        };
        val localdistributedIdGenerator = new DistributedIdGenerator(idGeneratorConfig, partitionResolverSupplierWithCount, nonceGeneratorType, metricRegistry, Clock.systemDefaultZone());
        val allIdsList = Collections.synchronizedList(new ArrayList<Id>());
        val totalTime = TestUtil.runMTTest(
                numThreads,
                iterationCount,
                (k) -> {
                    val id = localdistributedIdGenerator.generate("P");
                    allIdsList.add(id);
                },
                false,
                null);
        val expectedIdCount = numThreads * iterationCount;
        Assertions.assertEquals(expectedIdCount, allIdsList.size());
        checkUniqueIds(allIdsList);
        log.warn("partitionResolverSupplier was called {} times - expected count was: {}", partitionResolverCount.get(), expectedIdCount);
    }

    @Test
    void testGenerateOriginal() {
        distributedIdGenerator = new DistributedIdGenerator(
                idGeneratorConfig, partitionResolverSupplier, nonceGeneratorType, mock(MetricRegistry.class), Clock.systemDefaultZone()
        );
        val idOptional = distributedIdGenerator.generate("TEST");
        String id = idOptional.getId();
        Assertions.assertEquals(26, id.length());
    }

    @Test
    void testGenerateBase36() {
        val distributedIdGeneratorLocal = new DistributedIdGenerator(
                idGeneratorConfig,
                (txnId) -> new BigInteger(txnId.substring(txnId.length() - 6), 36).abs().intValue() % partitionCount,
                nonceGeneratorType,
                IdFormatters.base36(),
                mock(MetricRegistry.class),
                Clock.systemDefaultZone()
        );
        String id = distributedIdGeneratorLocal.generate("TEST").getId();
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
        Assertions.assertEquals(TestUtil.generateDate(2020, 11, 25, 9, 59, 3, 0, ZoneId.systemDefault()),
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

}