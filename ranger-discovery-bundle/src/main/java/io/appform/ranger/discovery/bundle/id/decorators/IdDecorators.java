package io.appform.ranger.discovery.bundle.id.decorators;

import lombok.experimental.UtilityClass;

@UtilityClass
public class IdDecorators {
    
    private static final IdDecorator base36IdFormatter = new Base36IdDecorator();
    
    public static IdDecorator base36() {
        return base36IdFormatter;
    }
}
