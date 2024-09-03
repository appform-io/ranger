package io.appform.ranger.discovery.bundle.id.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.dropwizard.validation.ValidationMethod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PartitionRange {
    @NotNull
    @Min(0)
    private int start;

    /* end partition is inclusive in range */
    @NotNull
    @Min(0)
    private int end;

    @ValidationMethod(message = "Partition Range should be non-decreasing")
    @JsonIgnore
    public boolean isRangeValid() {
        return start <= end;
    }
}
