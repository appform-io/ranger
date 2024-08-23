package io.appform.ranger.hub.server.bundle.configuration;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.appform.ranger.drove.config.DroveUpstreamConfig;
import io.appform.ranger.hub.server.bundle.models.BackendType;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.util.List;

/**
 *
 */
@Data
@EqualsAndHashCode(callSuper = true)
@ToString(callSuper = true)
@JsonIgnoreProperties(ignoreUnknown = true)
public class RangerDroveUpstreamConfiguration extends RangerUpstreamConfiguration {

    @NotEmpty
    @Valid
    private List<DroveUpstreamConfig> droveClusters;


    public RangerDroveUpstreamConfiguration() {
        super(BackendType.DROVE);
    }

    @Override
    public <T> T accept(RangerConfigurationVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
