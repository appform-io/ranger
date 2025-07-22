package io.appform.ranger.core.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.function.Supplier;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WeightSupplier {
    private Supplier<Double> supplier;
    private boolean enabled;
}
