package io.appform.ranger.discovery.bundle.id;

import com.codahale.metrics.MetricRegistry;

import java.util.Optional;

public class IdPool {
    /** List of IDs for the specific IdPool */
    private final CircularQueue queue;

    public IdPool(int size, final MetricRegistry metricRegistry, final String namespace) {
        this.queue = new CircularQueue(size, metricRegistry, namespace);
    }

    public void setId(int id) {
        queue.setId(id);
    }

    public Optional<Integer> getNextId() {
        return queue.getNextId();
    }

    public void reset() {
        queue.reset();
    }
}
