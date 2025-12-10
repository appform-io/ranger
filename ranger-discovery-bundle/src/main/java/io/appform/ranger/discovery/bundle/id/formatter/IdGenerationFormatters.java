package io.appform.ranger.discovery.bundle.id.formatter;

import lombok.Getter;
import lombok.val;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

public interface IdGenerationFormatters {
    
    static int setFormatter(final int options, final IdFormatterType type) {
        return options | mask(type);
    }
    
    static int unsetFormatter(final int options, final IdFormatterType type) {
        return options & ~mask(type);
    }
    
    static boolean isFormatterSet(final int option, final IdFormatterType type) {
        return (option & mask(type)) > 0;
    }
    
    static int mask(final IdFormatterType type) {
        return 1 << (int) type.getOffset();
    }
    
    static Set<IdFormatterType> getFormatterTypes(final int value) {
        EnumSet<IdFormatterType> formatterTypes = EnumSet.noneOf(IdFormatterType.class);
        for (val formatterType : IdFormatterType.values()) {
            val bitValue = mask(formatterType);
            if ((bitValue & value) == bitValue) {
                formatterTypes.add(formatterType);
            }
        }
        return formatterTypes.stream()
                .sorted(Comparator.comparingInt(IdFormatterType::getOffset))
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
    
    @Getter
    enum IdFormatterType {
        DEFAULT(0),
        DEFAULT_V2(1),
        BASE_36(2),
        RANDOM_NONCE(3);
        
        private final int offset;
        
        IdFormatterType(final int offset) {
            this.offset = offset;
        }
    }
}
