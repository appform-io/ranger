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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Test for {@link DistributedIdGenerator}
 */
@Slf4j
class PartitionAwareNonceGeneratorTest {
    final int numThreads = 5;
    final int iterationCountPerThread = 100000;
    final int partitionCount = 1000;
    final Function<String, Integer> partitionResolverSupplier = (txnId) -> Integer.parseInt(txnId.substring(txnId.length() - 6)) % partitionCount;
    private final IdGeneratorConfig idGeneratorConfig =
            IdGeneratorConfig.builder()
                    .partitionCount(partitionCount)
                    .defaultNamespaceConfig(DefaultNamespaceConfig.builder().idPoolSizePerPartition(100).build())
                    .build();
    private DistributedIdGenerator distributedIdGenerator;
    private NonceGeneratorType nonceGeneratorType;
    private final MetricRegistry metricRegistry = mock(MetricRegistry.class);

    @BeforeEach
    void setup() {
        nonceGeneratorType = NonceGeneratorType.PARTITION_AWARE;
        val meter = mock(Meter.class);
        doReturn(meter).when(metricRegistry).meter(anyString());
        doNothing().when(meter).mark();
        distributedIdGenerator = new DistributedIdGenerator(
                idGeneratorConfig, partitionResolverSupplier, nonceGeneratorType, metricRegistry, Clock.systemDefaultZone()
        );
        distributedIdGenerator.setNODE_ID(9999);
    }

    @Test
    void testGenerateWithBenchmark() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<Id>());
        TestUtil.runMTTest(
                numThreads,
                iterationCountPerThread,
                (k) -> {
                    val id = distributedIdGenerator.generate("P");
                    allIdsList.add(id);
                },
                true,
                this.getClass().getName() + ".testGenerateWithBenchmark");
        Assertions.assertEquals(numThreads * iterationCountPerThread, allIdsList.size());
        checkUniqueIds(allIdsList);
        checkDistribution(allIdsList, partitionResolverSupplier, idGeneratorConfig);
    }

    @Test
    void testGenerateWithConstraints() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<Id>());
        IdValidationConstraint partitionConstraint = (k) -> k.getExponent() % 4 == 0;
        val iterationCount = 50000;
        distributedIdGenerator.registerGlobalConstraints(List.of((k) -> k.getExponent() % 4 == 0));
        TestUtil.runMTTest(
                numThreads,
                iterationCount,
                (k) -> {
                    val id = distributedIdGenerator.generateWithConstraints("P", Domain.DEFAULT_DOMAIN_NAME, false);
                    id.ifPresent(allIdsList::add);
                },
                false,
                this.getClass().getName() + ".testGenerateWithConstraints");
        checkUniqueIds(allIdsList);

//        Assert No ID was generated for Invalid partitions
        for (val id : allIdsList) {
            Assertions.assertTrue(partitionConstraint.isValid(id));
        }
    }

    @Test
    void testGenerateAccuracy() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<Id>());
        TestUtil.runMTTest(
                numThreads,
                iterationCountPerThread,
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
        val uniqueIdSet = allIdsList.stream()
                .map(Id::getId)
                .collect(Collectors.toSet());
        Assertions.assertEquals(allIdsList.size(), uniqueIdSet.size());
    }

    protected HashMap<Integer, Integer> getIdCountMap(List<Id> allIdsList, Function<String, Integer> partitionResolver) {
        val idCountMap = new HashMap<Integer, Integer>();
        for (val id : allIdsList) {
            val partitionId = partitionResolver.apply(id.getId());
            idCountMap.put(partitionId, idCountMap.getOrDefault(partitionId, 0) + 1);
        }
        return idCountMap;
    }

    protected void checkDistribution(List<Id> allIdsList, Function<String, Integer> partitionResolver, IdGeneratorConfig config) {
        val idCountMap = getIdCountMap(allIdsList, partitionResolver);
        val expectedIdCount = (double) allIdsList.size() / config.getPartitionCount();
        for (int partitionId = 0; partitionId < config.getPartitionCount(); partitionId++) {
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
        for (int i = 0; i < iterationCountPerThread; i += 1) {
            val txnId = distributedIdGenerator.generate("P").getId();
            if (allIDs.contains(txnId)) {
                allIdsUnique = false;
                break;
            } else {
                allIDs.add(txnId);
            }
        }
        Assertions.assertTrue(allIdsUnique);
    }

    @Test
    void testFirstAndLastPartitionInclusion() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<Id>());
        TestUtil.runMTTest(
                numThreads,
                iterationCountPerThread,
                (k) -> {
                    val id = distributedIdGenerator.generate("P");
                    allIdsList.add(id);
                },
                false,
                this.getClass().getName() + ".testGenerateAccuracy");

        val idCountMap = getIdCountMap(allIdsList, partitionResolverSupplier);
        Assertions.assertTrue(idCountMap.get(0) > 0);
        Assertions.assertTrue(idCountMap.get(partitionCount - 1) > 0);
    }

    @Test
    void testDataReset() throws IOException {
        val allIdsList = Collections.synchronizedList(new ArrayList<Id>());
        val clock = mock(Clock.class);
        doReturn(Instant.parse("2000-01-01T00:00:00Z")).when(clock).instant();
        val distributedIdGenerator = new DistributedIdGenerator(idGeneratorConfig, partitionResolverSupplier, nonceGeneratorType, metricRegistry, clock);
        TestUtil.runMTTest(
                numThreads,
                iterationCountPerThread,
                (k) -> {
                    val id = distributedIdGenerator.generate("P");
                    allIdsList.add(id);
                },
                false, null);
        doReturn(Instant.parse("2000-01-01T00:01:00Z")).when(clock).instant();
        TestUtil.runMTTest(
                numThreads,
                iterationCountPerThread,
                (k) -> {
                    val id = distributedIdGenerator.generate("P");
                    allIdsList.add(id);
                },
                false, null);
        Assertions.assertEquals(2 * numThreads * iterationCountPerThread, allIdsList.size());
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
        TestUtil.runMTTest(
                numThreads,
                iterationCountPerThread,
                (k) -> {
                    val id = localdistributedIdGenerator.generate("P");
                    allIdsList.add(id);
                },
                false,
                null);
        val expectedIdCount = numThreads * iterationCountPerThread;
        Assertions.assertEquals(expectedIdCount, allIdsList.size());
        checkUniqueIds(allIdsList);
        log.debug("partitionResolverSupplier was called {} times - expected count was: {}", partitionResolverCount.get(), expectedIdCount);
    }

    @Test
    void testGenerateOriginal() {
        distributedIdGenerator = new DistributedIdGenerator(
                idGeneratorConfig, partitionResolverSupplier, nonceGeneratorType, metricRegistry, Clock.systemDefaultZone()
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
                metricRegistry, Clock.systemDefaultZone(), IdFormatters.base36()
        );
        String id = distributedIdGeneratorLocal.generate("TEST").getId();
        Assertions.assertEquals(18, id.length());
    }

    @Test
    void testConstraintFailure() {
        val domainName = "ALL_INVALID";
        distributedIdGenerator.registerDomain(
                Domain.builder()
                        .domain(domainName)
                        .constraints(List.of(id -> false))
                        .build()
        );
        Assertions.assertFalse(distributedIdGenerator.generateWithConstraints(
                "TST",
                domainName,
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
        val idString = "ABC2011250959031643972247";
        val id = distributedIdGenerator.parse(idString).orElse(null);
        Assertions.assertNotNull(id);
        Assertions.assertEquals(idString, id.getId());
        Assertions.assertEquals(164247, id.getExponent());
        Assertions.assertEquals(3972, id.getNode());
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