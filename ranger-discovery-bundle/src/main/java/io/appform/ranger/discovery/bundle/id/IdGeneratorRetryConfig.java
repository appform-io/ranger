package io.appform.ranger.discovery.bundle.id;

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
public class IdGeneratorRetryConfig {
    @NotNull
    @Min(1)
    private int idGenerationRetryCount;

    @NotNull
    @Min(1)
    private int partitionRetryCount;

}