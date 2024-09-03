package io.appform.ranger.discovery.bundle.id.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class NamespaceConfig {
    @NotNull
    private String namespace;

    /** Size of pre-generated id buffer. Value from DefaultNamespaceConfig will be used if this is null */
    @Min(2)
    private Integer idPoolSizePerBucket;
}
