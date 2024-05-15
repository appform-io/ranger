package io.appform.ranger.discovery.bundle.id;

import io.appform.ranger.discovery.bundle.id.config.IdGeneratorRetryConfig;
import io.appform.ranger.discovery.bundle.id.weighted.PartitionAwareIdGenerator;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.function.Function;

/**
 * Test performance between different constructs
 */
@Slf4j
public class PartitionAwareIdGeneratorPerfTest extends BenchmarkTest {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private PartitionAwareIdGenerator partitionAwareIdGenerator;
        final Function<String, Integer> partitionResolverSupplier = (txnId) -> Integer.parseInt(txnId.substring(txnId.length()-6)) % 1024;

        @Setup(Level.Trial)
        public void setUp() throws IOException {
            partitionAwareIdGenerator = new PartitionAwareIdGenerator(
                    1024, partitionResolverSupplier,
                    IdGeneratorRetryConfig.builder().idGenerationRetryCount(1024).partitionRetryCount(1024).build()
            );
        }
    }

    @SneakyThrows
    @Benchmark
    public void testGenerate(Blackhole blackhole, BenchmarkState state) {
        state.partitionAwareIdGenerator.generate("P");
    }
}