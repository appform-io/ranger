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
public class DefaultNamespaceConfig {
    /** Size of pre-generated id buffer. Each partition will have separate IdPool, each of size equal to this value. */
    @NotNull
    @Min(2)
    private Integer idPoolSizePerPartition;
}
