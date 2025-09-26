/*
 * Copyright 2024 Authors, Flipkart Internet Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.appform.ranger.id;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.val;
import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public abstract class BenchmarkTest {

    public static final ObjectMapper mapper = new ObjectMapper();

    @Test
    void testBenchmark() throws RunnerException {
        val opt = new OptionsBuilder()
                .include(String.format("%s.*", this.getClass().getName()))
                .mode(Mode.Throughput)
                .timeUnit(TimeUnit.SECONDS)
                .warmupTime(TimeValue.seconds(5))
                .warmupIterations(1)
                .measurementTime(TimeValue.seconds(5))
                .measurementIterations(4)
                .threads(1)
                .forks(3)
                .shouldFailOnError(true)
                .shouldDoGC(true)
                .build();
        val results = new Runner(opt).run();
        results.iterator()
                .forEachRemaining(new Consumer<RunResult>() {
                    @SneakyThrows
                    @Override
                    public void accept(RunResult runResult) {
                        val benchmarkName = runResult.getParams().getBenchmark();
                        val outputFilePath = Paths.get(String.format("perf/results/%s.json", benchmarkName));
                        val outputNode = mapper.createObjectNode();
                        outputNode.put("name", benchmarkName);
                        outputNode.put("mode", runResult.getParams().getMode().name());
                        outputNode.put("iterations", runResult.getParams().getMeasurement().getCount());
                        outputNode.put("threads", runResult.getParams().getThreads());
                        outputNode.put("forks", runResult.getParams().getForks());
                        outputNode.put("mean_ops", runResult.getPrimaryResult().getStatistics().getMean());
                        Files.write(outputFilePath, mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(outputNode));
                    }
                });
    }


}