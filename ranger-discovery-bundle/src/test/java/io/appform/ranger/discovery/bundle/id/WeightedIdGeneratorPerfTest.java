package io.appform.ranger.discovery.bundle.id;

import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorRetryConfig;
import io.appform.ranger.discovery.bundle.id.config.WeightedIdConfig;
import io.appform.ranger.discovery.bundle.id.config.PartitionRange;
import io.appform.ranger.discovery.bundle.id.weighted.WeightedIdGenerator;
import io.appform.ranger.discovery.bundle.id.config.WeightedPartition;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.mockito.Mockito.mock;

/**
 * Test performance between different constructs
 */
@Slf4j
public class WeightedIdGeneratorPerfTest extends BenchmarkTest {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private WeightedIdGenerator weightedIdGenerator;
        final Function<String, Integer> partitionResolverSupplier = (txnId) -> Integer.parseInt(txnId.substring(txnId.length()-6)) % 1024;


        @Setup(Level.Trial)
        public void setUp() throws IOException {
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
            weightedIdGenerator = new WeightedIdGenerator(
                    IdGeneratorConfig.builder()
                            .partitionCount(1024)
                            .weightedIdConfig(weightedIdConfig)
                            .retryConfig(IdGeneratorRetryConfig.builder().idGenerationRetryCount(1024).partitionRetryCount(1024).build())
                            .build(),
                    partitionResolverSupplier,
                    mock(MetricRegistry.class)
            );
        }
    }

    @SneakyThrows
    @Benchmark
    public void testGenerate(Blackhole blackhole, BenchmarkState state) {
        state.weightedIdGenerator.generate("P");
    }
}