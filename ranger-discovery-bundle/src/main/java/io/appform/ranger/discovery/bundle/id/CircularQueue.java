package io.appform.ranger.discovery.bundle.id;

import com.codahale.metrics.MetricRegistry;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;

@Slf4j
class CircularQueue {
    private static final String QUEUE_FULL_METRIC_STRING = "idGenerator.queueFull.forPrefix.";
    private static final String UNUSED_INDICES_METRIC_STRING = "idGenerator.unusedIds.forPrefix.";

//    private final Meter queueFullMeter;
//    private final Meter unusedDataMeter;

    /** List of data for the specific queue */
    private final AtomicIntegerArray queue;

    /** Size of the queue */
    private final int size;

    /** Pointer to track the first usable index. Helps to determine the next usable data. */
    private final AtomicInteger firstIdx = new AtomicInteger(0);

    /** Pointer to track index of last usable index. Helps to determine which index the next data should go in. */
    private final AtomicInteger lastIdx = new AtomicInteger(0);


    public CircularQueue(int size, final MetricRegistry metricRegistry, final String namespace) {
        this.size = size;
        this.queue = new AtomicIntegerArray(size);
//        this.queueFullMeter = metricRegistry.meter(QUEUE_FULL_METRIC_STRING + namespace);
//        this.unusedDataMeter = metricRegistry.meter(UNUSED_INDICES_METRIC_STRING + namespace);
    }

    public synchronized void setId(int id) {
        // Don't store new data if the queue is already full of unused data.
        if (lastIdx.get() >= firstIdx.get() + size - 1) {
//            queueFullMeter.mark();
            return;
        }
        val arrayIdx = lastIdx.get() % size;
        queue.set(arrayIdx, id);
        lastIdx.getAndIncrement();
    }

    private int getId(int index) {
        val arrayIdx = index % size;
        return queue.get(arrayIdx);
    }

    public synchronized Optional<Integer> getNextId() {
        if (firstIdx.get() < lastIdx.get()) {
            val id = getId(firstIdx.getAndIncrement());
            return Optional.of(id);
        } else {
            return Optional.empty();
        }
    }

    public void reset() {
        val unusedIds = lastIdx.get() - firstIdx.get();
//        unusedDataMeter.mark(unusedIds);
        lastIdx.set(0);
        firstIdx.set(0);
    }
}
