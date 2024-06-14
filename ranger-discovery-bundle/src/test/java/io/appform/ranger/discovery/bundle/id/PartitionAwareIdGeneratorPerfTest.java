package io.appform.ranger.discovery.bundle.id;

import com.codahale.metrics.MetricRegistry;
import io.appform.ranger.discovery.bundle.id.config.IdGeneratorConfig;
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

import static org.mockito.Mockito.mock;

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
                    IdGeneratorConfig.builder()
                            .partitionCount(1024)
                            .idPoolSize(100)
                            .retryConfig(IdGeneratorRetryConfig.builder().idGenerationRetryCount(4096).partitionRetryCount(4096).build())
                            .build(),
                    partitionResolverSupplier,
                    mock(MetricRegistry.class)
            );
        }
    }

    @SneakyThrows
    @Benchmark
    public void testGenerate(Blackhole blackhole, BenchmarkState state) {
        state.partitionAwareIdGenerator.generate("P");
    }
}