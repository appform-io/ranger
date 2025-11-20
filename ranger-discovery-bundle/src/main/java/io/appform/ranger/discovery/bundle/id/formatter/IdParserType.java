package io.appform.ranger.discovery.bundle.id.formatter;

import lombok.Getter;

@Getter
public enum IdParserType {
    DEFAULT (0),
    DEFAULT_V2(00),
    SUFFIXED (01),
    BASE_36_SUFFIXED(02);

    private final int value;

    IdParserType(final int value) {
        this.value = value;
    }
}
