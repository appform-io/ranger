package io.appform.ranger.core.healthcheck.updater;

import io.appform.ranger.core.healthcheck.HealthcheckResult;
import io.appform.ranger.core.model.ServiceNode;

public class LastUpdatedHandler<T> extends HealthUpdateHandler<T> {
    @Override
    protected void handle(HealthcheckResult result, ServiceNode<T> node) {
        node.setLastUpdatedTimeStamp(result.getUpdatedTime());
    }
}

