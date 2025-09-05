package io.appform.ranger.core.healthcheck.updater;

import io.appform.ranger.core.healthcheck.HealthcheckResult;
import io.appform.ranger.core.model.ServiceNode;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RoutingWeightHandler<T> extends HealthUpdateHandler<T> {
    private final Double routingWeight;

    public RoutingWeightHandler(Double routingWeight) {

        if (routingWeight == null || routingWeight < 0.0 || routingWeight > 1.0) {
            log.info("Defaulting to 1 as routing weight not within range or not provided");
            routingWeight = 1.0;
        }
        this.routingWeight = routingWeight;
    }

    @Override
    protected void handle(HealthcheckResult result, ServiceNode<T> node) {
        node.setRoutingWeight(routingWeight);
    }
}
