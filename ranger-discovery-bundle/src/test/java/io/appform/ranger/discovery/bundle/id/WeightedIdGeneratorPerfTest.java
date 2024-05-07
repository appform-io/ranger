package io.appform.ranger.discovery.bundle.id;

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
            weightedIdGenerator = new WeightedIdGenerator(1024, partitionResolverSupplier, weightedIdConfig);
        }
    }

    @SneakyThrows
    @Benchmark
    public void testGenerate(Blackhole blackhole, BenchmarkState state) {
        state.weightedIdGenerator.generate("P");
    }
}