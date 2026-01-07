package io.appform.ranger.core.healthcheck.updater;

import io.appform.ranger.core.healthcheck.HealthcheckResult;
import io.appform.ranger.core.model.ServiceNode;


public abstract class HealthUpdateHandler<T> {

    private HealthUpdateHandler<T> next;

    public HealthUpdateHandler<T> setNext(HealthUpdateHandler<T> next) {
        if (this.next == null) {
            this.next = next;
        } else {
            this.next.setNext(next);
        }
        return this;
    }

    public void handleNext(HealthcheckResult result, ServiceNode<T> serviceNode) {
        // Do handler-specific logic
        handle(result, serviceNode);

        // Pass to next handler if present
        if (next != null) {
            next.handleNext(result, serviceNode);
        }
    }

    protected abstract void handle(HealthcheckResult result, ServiceNode<T> serviceNode);

}
