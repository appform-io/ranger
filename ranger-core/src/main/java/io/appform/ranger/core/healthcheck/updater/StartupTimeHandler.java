package io.appform.ranger.core.healthcheck.updater;

import io.appform.ranger.core.healthcheck.HealthcheckResult;
import io.appform.ranger.core.model.ServiceNode;
import lombok.extern.slf4j.Slf4j;

import static io.appform.ranger.core.healthcheck.HealthcheckStatus.healthy;

@Slf4j
public class StartupTimeHandler<T> extends HealthUpdateHandler<T> {
    @Override
    protected void handle(HealthcheckResult result, ServiceNode<T> node) {
        if (result.getStatus() == healthy && node.getHealthySinceTimeStamp() == 0) {
            node.setHealthySinceTimeStamp(System.currentTimeMillis());
            log.debug("Setting or Updating startup time for node to {}", node.getHealthySinceTimeStamp());
        }
    }
}
