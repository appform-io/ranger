package io.appform.ranger.discovery.bundle.id;

import io.appform.ranger.discovery.bundle.id.decorators.IdDecorator;
import io.appform.ranger.discovery.bundle.id.decorators.IdDecorators;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatter;
import io.appform.ranger.discovery.bundle.id.formatter.IdFormatters;
import lombok.Getter;
import lombok.val;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public enum IdGenerationType {
    
    DEFAULT(0, IdFormatters.original(), List.of()),
    DEFAULT_V2_RANDOM_NONCE(01, IdFormatters.randomNonce(), List.of()),
    BASE_36_RANDOM_NONCE(02, IdFormatters.randomNonce(), List.of(IdDecorators.base36()));
    
    @Getter
    private final int value;
    private final IdFormatter idFormatter;
    private final List<IdDecorator> idDecorators;
    
    IdGenerationType(final int value,
                     final IdFormatter idFormatter,
                     final List<IdDecorator> idDecorators) {
        this.value = value;
        this.idFormatter = idFormatter;
        this.idDecorators = idDecorators;
    }
    
    public static final Map<Integer, IdFormatter> FORMATTER_VALUE_MAP = Arrays.stream(values())
            .collect(Collectors.toMap(IdGenerationType::getValue, type -> type.idFormatter));
    
    public static final Map<Integer, List<IdDecorator>> DECORATOR_VALUE_MAP = Arrays.stream(values())
            .collect(Collectors.toMap(IdGenerationType::getValue, type -> type.idDecorators));
    
    public static final Map<Integer, List<IdDecorator>> DECORATOR_PARSE_VALUE_MAP = Arrays.stream(values())
            .collect(Collectors.toMap(
                    IdGenerationType::getValue,
                    type -> {
                        List<IdDecorator> reversed = new ArrayList<>(type.idDecorators);
                        Collections.reverse(reversed);
                        return Collections.unmodifiableList(reversed);
                    }
            ));
    
    private static final Map<IdFormatter, Map<List<IdDecorator>, Integer>> LOOKUP_MAP =
            Arrays.stream(values())
                    .collect(Collectors.groupingBy(
                            type -> type.idFormatter,
                            Collectors.toMap(
                                    type -> type.idDecorators,
                                    IdGenerationType::getValue
                            )
                    ));
    
    public static Optional<Integer> findValue(final IdFormatter formatter, final List<IdDecorator> decorators) {
        val safeDecorators = decorators != null ? decorators : List.of();
        return Optional.ofNullable(LOOKUP_MAP.get(formatter))
                .map(innerMap -> innerMap.get(safeDecorators));
    }
}
