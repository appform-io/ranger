package io.appform.ranger.discovery.bundle.id.formatter;

import lombok.Getter;

@Getter
public enum IdParserType {
    DEFAULT (0),
    SUFFIX (11);

    private final int value;

    IdParserType(final int value) {
        this.value = value;
    }
}
