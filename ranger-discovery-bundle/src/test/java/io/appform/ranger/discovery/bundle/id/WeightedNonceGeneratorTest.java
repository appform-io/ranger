package io.appform.ranger.discovery.bundle.id;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.discovery.bundle.id.config.DefaultNamespaceConfig;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.config.WeightedIdConfig;
import io.appform.ranger.discovery.bundle.id.config.PartitionRange;
import io.appform.ranger.discovery.bundle.id.nonce.NonceGeneratorType;
import io.appform.ranger.discovery.bundle.id.generator.DistributedIdGenerator;
import io.appform.ranger.discovery.bundle.id.config.WeightedPartition;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

/**
 * Test for {@link DistributedIdGenerator}
 */
@Slf4j
@SuppressWarnings({"unused", "FieldMayBeFinal"})
class WeightedNonceGeneratorTest extends PartitionAwareNonceGeneratorTest {
    final int numThreads = 5;
    final int iterationCountPerThread = 100000;
    final int partitionCount = 1024;
    final Function<String, Integer> partitionResolverSupplier = (txnId) -> Integer.parseInt(txnId.substring(txnId.length() - 6)) % partitionCount;
    private MetricRegistry metricRegistry = mock(MetricRegistry.class);

    @BeforeEach
    void setup() {
        nonceGeneratorType = NonceGeneratorType.WEIGHTED_DISTRIBUTED;
        val meter = mock(Meter.class);
        doReturn(meter).when(metricRegistry).meter(anyString());
        doNothing().when(meter).mark();
        List<WeightedPartition> partitionConfigList = new ArrayList<>();
        partitionConfigList.add(WeightedPartition.builder()
                .partitionRange(PartitionRange.builder().start(0).end(511).build())
                .weight(400).build());
        partitionConfigList.add(WeightedPartition.builder()
                .partitionRange(PartitionRange.builder().start(512).end(1023).build())
                .weight(600).build());
        val weightedIdConfig = WeightedIdConfig.builder()
                .partitions(partitionConfigList)
                .build();
        idGeneratorConfig =
                IdGeneratorConfig.builder()
                        .partitionCount(partitionCount)
                        .weightedIdConfig(weightedIdConfig)
                        .defaultNamespaceConfig(DefaultNamespaceConfig.builder().idPoolSizePerPartition(100).build())
                        .build();
        distributedIdGenerator = new DistributedIdGenerator(idGeneratorConfig, partitionResolverSupplier, nonceGeneratorType, metricRegistry, Clock.systemDefaultZone());
    }

    protected void checkDistribution(List<Id> allIdsList, Function<String, Integer> partitionResolver, IdGeneratorConfig config) {
        val idCountMap = getIdCountMap(allIdsList, partitionResolver);
        int maxShardWeight = 1000;
        for (WeightedPartition partition: config.getWeightedIdConfig().getPartitions()) {
            val expectedIdCount = ((double) partition.getWeight() / maxShardWeight) *  ((double) allIdsList.size() / (partition.getPartitionRange().getEnd()-partition.getPartitionRange().getStart()+1));
            int idCountForPartition = 0;
            for (int partitionId = partition.getPartitionRange().getStart(); partitionId <= partition.getPartitionRange().getEnd(); partitionId++) {
                Assertions.assertTrue(expectedIdCount * 0.9 <= idCountMap.get(partitionId),
                        String.format("Partition %s generated %s ids, expected was more than: %s",
                                partitionId, idCountMap.get(partitionId), expectedIdCount * 0.9));
                Assertions.assertTrue(idCountMap.get(partitionId) <= expectedIdCount * 1.1,
                        String.format("Partition %s generated %s ids, expected was less than: %s",
                                partitionId, idCountMap.get(partitionId), expectedIdCount * 1.1));
                idCountForPartition += idCountMap.get(partitionId);
            }
            log.warn("Partition ID Count: {} - Percentage: {}", idCountForPartition, (double) idCountForPartition * 100 / allIdsList.size());
        }
    }


    @Test
    void testGenerateForMinimumRangePartition() throws IOException {
        val partitionCount = 10;
        Function<String, Integer> partitionResolver = (txnId) -> Integer.parseInt(txnId.substring(txnId.length() - 6)) % partitionCount;
        List<WeightedPartition> partitionConfigList = List.of(
                WeightedPartition.builder()
                        .partitionRange(PartitionRange.builder().start(0).end(0).build())
                        .weight(125).build(),
                WeightedPartition.builder()
                        .partitionRange(PartitionRange.builder().start(1).end(1).build())
                        .weight(150).build(),
                WeightedPartition.builder()
                        .partitionRange(PartitionRange.builder().start(2).end(8).build())
                        .weight(525).build(),
                WeightedPartition.builder()
                        .partitionRange(PartitionRange.builder().start(9).end(9).build())
                        .weight(200).build()
                );
        val weightedIdConfig = WeightedIdConfig.builder()
                .partitions(partitionConfigList)
                .build();
        val idGeneratorConfig =
                IdGeneratorConfig.builder()
                        .partitionCount(partitionCount)
                        .weightedIdConfig(weightedIdConfig)
                        .defaultNamespaceConfig(DefaultNamespaceConfig.builder().idPoolSizePerPartition(100).build())
                        .build();
        val distributedIdGenerator = new DistributedIdGenerator(idGeneratorConfig, partitionResolver, NonceGeneratorType.WEIGHTED_DISTRIBUTED, metricRegistry, Clock.systemDefaultZone());
        val allIdsList = Collections.synchronizedList(new ArrayList<Id>());
        val iterationCount = 100000;
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
        checkDistribution(allIdsList, partitionResolver, idGeneratorConfig);
    }

}