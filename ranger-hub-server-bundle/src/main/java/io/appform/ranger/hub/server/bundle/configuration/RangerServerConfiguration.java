package io.appform.ranger.hub.server.bundle.configuration;

import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;

/**
 *
 */
@Value
@Jacksonized
@Builder
public class RangerServerConfiguration {
    @NotNull
    @NotEmpty
    String namespace;

    @NotEmpty
    @Valid
    List<RangerUpstreamConfiguration> upstreams;
}
