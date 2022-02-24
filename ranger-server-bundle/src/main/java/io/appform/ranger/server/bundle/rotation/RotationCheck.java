package io.appform.ranger.server.bundle.rotation;

import com.codahale.metrics.health.HealthCheck;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class RotationCheck extends HealthCheck {

    private final RotationStatus rotationStatus;

    @Override
    protected Result check() {
        return null != rotationStatus && rotationStatus.status() ? Result.healthy("Service is rotation") :
                Result.unhealthy("Service is OOR");
    }
}
