package io.appform.ranger.drove.utils;

import com.github.benmanes.caffeine.cache.CacheLoader;
import lombok.Builder;
import org.checkerframework.checker.nullness.qual.NonNull;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;

/**
 * From Caffeine example at: https://github.com/ben-manes/caffeine/blob/master/examples/coalescing-bulkloader-reactor
 * /src/main/java/com/github/benmanes/caffeine/examples/coalescing/bulkloader/CoalescingBulkLoader.java
 */
public final class CoalescingBulkLoader<K, V> implements CacheLoader<K, V> {
    private final Function<Set<K>, Map<K, V>> mappingFunction;
    private final Sinks.Many<Map.Entry<K, CompletableFuture<V>>> sink;

    /**
     * @param maxSize         the maximum entries to collect before performing a bulk request
     * @param maxTime         the maximum duration to wait before performing a bulk request
     * @param parallelism     the number of parallel bulk loads that can be performed
     * @param mappingFunction the function to compute the values
     */
    @Builder
    public CoalescingBulkLoader(
            int maxSize, Duration maxTime, int parallelism,
            Function<Set<K>, Map<K, V>> mappingFunction) {
        this.sink = Sinks.many().unicast().onBackpressureBuffer();
        this.mappingFunction = requireNonNull(mappingFunction);
        sink.asFlux()
                .bufferTimeout(maxSize, maxTime)
                .map(requests -> requests.stream().collect(
                        toMap(Map.Entry::getKey, Map.Entry::getValue)))
                .parallel(parallelism)
                .runOn(Schedulers.boundedElastic())
                .subscribe(this::handle);
    }

    @Override
    public V load(K key) {
        return loadAll(Set.of(key)).get(key);
    }

    @Override
    public @NonNull Map<K, V> loadAll(@NonNull Iterable<? extends K> keys) {
        return mappingFunction.apply(StreamSupport.stream(keys.spliterator(), false)
                                             .collect(Collectors.toUnmodifiableSet()));
    }

    @Override
    public synchronized @NonNull CompletableFuture<V> asyncReload(@NonNull K key, @NonNull V oldValue, Executor e) {
        var entry = Map.entry(key, new CompletableFuture<V>());
        sink.tryEmitNext(entry).orThrow();
        return entry.getValue();
    }

    @SuppressWarnings("java:S1181")
    private void handle(Map<K, CompletableFuture<V>> requests) {
        try {
            var results = mappingFunction.apply(requests.keySet());
            requests.forEach((key, result) -> result.complete(results.get(key)));
        }
        catch (Throwable t) {
            requests.forEach((key, result) -> result.completeExceptionally(t));
        }
    }
}
