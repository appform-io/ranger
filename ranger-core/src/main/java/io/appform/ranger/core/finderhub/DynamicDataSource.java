package io.appform.ranger.core.finderhub;

import io.appform.ranger.core.model.Service;
import lombok.NoArgsConstructor;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@NoArgsConstructor
public class DynamicDataSource implements ServiceDataSource {
    private final Set<Service> serviceCollection = ConcurrentHashMap.newKeySet();

    public DynamicDataSource(Collection<Service> initialServices) {
        this.serviceCollection.addAll(initialServices);
    }

    @Override
    public Collection<Service> services() {
        return Collections.unmodifiableSet(serviceCollection);
    }

    @Override
    public void add(Service service) {
        this.serviceCollection.add(service);
    }

    @Override
    public void start() {
        // Nothing to do here
    }

    @Override
    public void stop() {
        // Nothing to do here
    }
}
