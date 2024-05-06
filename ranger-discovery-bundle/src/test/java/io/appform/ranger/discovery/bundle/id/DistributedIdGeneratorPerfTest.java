package io.appform.ranger.discovery.bundle.id;

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
public class DistributedIdGeneratorPerfTest extends BenchmarkTest {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        private DistributedIdGenerator distributedIdGenerator;
        final Function<String, Integer> partitionResolverSupplier = (txnId) -> Integer.parseInt(txnId.substring(txnId.length()-6)) % 1024;

        @Setup(Level.Trial)
        public void setUp() throws IOException {
            distributedIdGenerator = new DistributedIdGenerator(23, 1024, partitionResolverSupplier);
        }
    }

    @SneakyThrows
    @Benchmark
    public void testGenerate(Blackhole blackhole, BenchmarkState state) {
        state.distributedIdGenerator.generate("P");
    }
}