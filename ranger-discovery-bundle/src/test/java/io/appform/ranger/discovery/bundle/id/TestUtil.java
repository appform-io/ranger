package io.appform.ranger.discovery.bundle.id;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.logback.shaded.guava.base.Stopwatch;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@UtilityClass
public class TestUtil {
    private static final String OUTPUT_PATH = "perf/results/%s.json";

    public double runMTTest(int numThreads, int iterationCount, final Consumer<Integer> supplier, String outputFileName) throws IOException {
        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
        final List<Future<Long>> futures = IntStream.range(0, numThreads)
                .mapToObj(i -> executorService.submit(() -> {
                    final Stopwatch stopwatch = Stopwatch.createStarted();
                    IntStream.range(0, iterationCount).forEach(supplier::accept);
                    return stopwatch.elapsed(TimeUnit.MILLISECONDS);
                }))
                .collect(Collectors.toList());
        final long total = futures.stream()
                .mapToLong(f -> {
                    try {
                        return f.get();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return 0;
                    } catch (ExecutionException e) {
                        throw new IllegalStateException(e);
                    }
                })
                .sum();
        writeToFile(numThreads, iterationCount, total, outputFileName);
        log.warn("Finished Execution for {} iterations in avg time: {}", iterationCount, ((double) total) / numThreads);
        return total;
    }

    public void writeToFile(int numThreads, int iterationCount, long totalMillis, String outputFileName) throws IOException {
        val mapper = new ObjectMapper();
        val outputFilePath = Paths.get(String.format(OUTPUT_PATH, outputFileName));
        val outputNode = mapper.createObjectNode();
        outputNode.put("name", outputFileName);
        outputNode.put("iterations", iterationCount);
        outputNode.put("threads", numThreads);
        outputNode.put("totalMillis", totalMillis);
        outputNode.put("avgTime", ((double) totalMillis) / numThreads);
        Files.write(outputFilePath, mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(outputNode));
    }
}
