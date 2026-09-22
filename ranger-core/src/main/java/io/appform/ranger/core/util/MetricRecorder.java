package io.appform.ranger.core.util;

import com.codahale.metrics.MetricRegistry;
import java.util.concurrent.atomic.AtomicReference;
import lombok.experimental.UtilityClass;

@UtilityClass
public class MetricRecorder {

    private static final AtomicReference<MetricRegistry> metricRegistry = new AtomicReference<>();

    public static void initialize(MetricRegistry registry) {
        metricRegistry.set(registry);
    }

    protected static MetricRegistry registry() {
        return metricRegistry.get();
    }
}
