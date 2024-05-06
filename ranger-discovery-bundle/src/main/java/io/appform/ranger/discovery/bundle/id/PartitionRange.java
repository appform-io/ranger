package io.appform.ranger.discovery.bundle.id;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.dropwizard.validation.ValidationMethod;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PartitionRange {
    @NotNull
    @Min(0)
    private int start;

    @NotNull
    private int end;

    @ValidationMethod(message = "Partition Range should be non-decreasing")
    @JsonIgnore
    public boolean isRangeValid() {
        return start <= end;
    }
}
