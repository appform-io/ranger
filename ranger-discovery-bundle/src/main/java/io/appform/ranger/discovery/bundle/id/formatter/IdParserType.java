package io.appform.ranger.discovery.bundle.id.formatter;

import lombok.Getter;

@Getter
public enum IdParserType {
    DEFAULT (0),
    SECOND_PRECISION (10);

    private final int value;

    IdParserType(final int value) {
        this.value = value;
    }
}
