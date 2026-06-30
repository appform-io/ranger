package io.appform.ranger.discovery.bundle.id.decorators;

import java.util.Optional;

public interface IdDecorator {
    
    String decorate(final String idString);
    
    Optional<String> parse(final String idString);
}
