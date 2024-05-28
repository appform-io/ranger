package io.appform.ranger.hub.server.bundle.configuration;

/**
 *
 */
public interface RangerConfigurationVisitor<T> {
    T visit(RangerHttpUpstreamConfiguration rangerHttpConfiguration);

    T visit(RangerZkUpstreamConfiguration rangerZkConfiguration);
}
